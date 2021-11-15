use super::{Executor, Message, Result, SimpleExecutor, StreamChunk};
use crate::storage::MemRowTableRef as MemTableRef;
use async_trait::async_trait;
use risingwave_common::array::Row;
use risingwave_common::catalog::Schema;

/// `MViewSinkExecutor` writes data to a row-based memtable, so that data could
/// be queried by the AP engine.
pub struct MViewSinkExecutor {
    input: Box<dyn Executor>,
    table: MemTableRef,
    pk_col: Vec<usize>,
}

impl MViewSinkExecutor {
    pub fn new(input: Box<dyn Executor>, table: MemTableRef, pk_col: Vec<usize>) -> Self {
        Self {
            input,
            table,
            pk_col,
        }
    }
}

#[async_trait]
impl Executor for MViewSinkExecutor {
    async fn next(&mut self) -> Result<Message> {
        super::simple_executor_next(self).await
    }

    fn schema(&self) -> &Schema {
        self.table.schema()
    }
}

impl SimpleExecutor for MViewSinkExecutor {
    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }

    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let StreamChunk {
            ops,
            columns,
            visibility,
            ..
        } = &chunk;

        let mut ingest_op = vec![];
        let mut row_batch = vec![];

        let pk_num = &self.pk_col.len();

        for (idx, op) in ops.iter().enumerate() {
            // check visibility
            let visible = visibility
                .as_ref()
                .map(|x| x.is_set(idx).unwrap())
                .unwrap_or(true);
            if !visible {
                continue;
            }

            // assemble pk row
            let mut pk_row = vec![];
            for column_id in &self.pk_col {
                let datum = columns[*column_id].array_ref().datum_at(idx);
                pk_row.push(datum);
            }
            let pk_row = Row(pk_row);

            // assemble row
            let mut row = vec![];
            for column in columns {
                let datum = column.array_ref().datum_at(idx);
                row.push(datum);
            }
            let row = Row(row);

            use super::Op::*;
            if *pk_num > 0 {
                match op {
                    Insert | UpdateInsert => {
                        ingest_op.push((pk_row, Some(row)));
                    }
                    Delete | UpdateDelete => {
                        ingest_op.push((pk_row, None));
                    }
                }
            } else {
                match op {
                    Insert | UpdateInsert => {
                        row_batch.push((row, true));
                    }
                    Delete | UpdateDelete => {
                        row_batch.push((row, false));
                    }
                }
            }
        }
        if *pk_num > 0 {
            self.table.ingest(ingest_op)?;
        } else {
            self.table.insert_batch(row_batch)?;
        }

        Ok(Message::Chunk(chunk))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{SimpleTableManager, TableManager, TableTypes};
    use crate::stream_op::test_utils::*;
    use crate::stream_op::*;
    use crate::*;

    use risingwave_common::array::{I32Array, Row};
    use risingwave_common::catalog::{Field, SchemaId, TableId};
    use risingwave_common::types::{Int32Type, Scalar};
    use risingwave_pb::data::{data_type::TypeName, DataType};
    use risingwave_pb::plan::{column_desc::ColumnEncodingType, ColumnDesc};

    #[tokio::test]
    async fn test_sink() {
        // Prepare storage and memtable.
        let store_mgr = Arc::new(SimpleTableManager::new());
        let table_id = TableId::new(SchemaId::default(), 1);
        // Two columns of int32 type, the first column is PK.
        let column_desc1 = ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            name: "v1".to_string(),
            is_primary: false,
        };
        let column_desc2 = ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            name: "v2".to_string(),
            is_primary: false,
        };
        let column_descs = vec![column_desc1.to_proto(), column_desc2.to_proto()];
        let pks = vec![0_usize];
        let _res = store_mgr.create_materialized_view(&table_id, column_descs, pks.clone());
        // Prepare source chunks.
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I32Array, Int32Type, [1, 2, 3] },
                column_nonnull! { I32Array, Int32Type, [4, 5, 6] },
            ],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![Op::Insert, Op::Delete],
            columns: vec![
                column_nonnull! { I32Array, Int32Type, [7, 3] },
                column_nonnull! { I32Array, Int32Type, [8, 6] },
            ],
            visibility: None,
        };

        let table_ref = store_mgr.get_table(&table_id).unwrap();
        if let TableTypes::Row(table) = table_ref {
            // Prepare stream executors.
            let schema = Schema {
                fields: vec![
                    Field {
                        data_type: Int32Type::create(false),
                    },
                    Field {
                        data_type: Int32Type::create(false),
                    },
                ],
            };
            let source = MockSource::with_chunks(schema, vec![chunk1, chunk2]);
            let mut sink_executor =
                Box::new(MViewSinkExecutor::new(Box::new(source), table.clone(), pks));

            // First stream chunk. We check the existence of (3) -> (3,6)
            if let Message::Chunk(_chunk) = sink_executor.next().await.unwrap() {
                let value_row = Row(vec![Some(3.to_scalar_value())]);
                let res_row = table.get(value_row);
                if let Ok(res_row_in) = res_row {
                    let datum = res_row_in.unwrap().0.get(1).unwrap().clone();
                    // Dirty trick to assert_eq between (&int32 and integer).
                    let d_value = datum.unwrap().as_int32() + 1;
                    assert_eq!(d_value, 7);
                } else {
                    unreachable!();
                }
            } else {
                unreachable!();
            }

            // Second stream chunk. We check the existence of (7) -> (7,8)
            if let Message::Chunk(_chunk) = sink_executor.next().await.unwrap() {
                // From (7) -> (7,8)
                let value_row = Row(vec![Some(7.to_scalar_value())]);
                let res_row = table.get(value_row);
                if let Ok(res_row_in) = res_row {
                    let datum = res_row_in.unwrap().0.get(1).unwrap().clone();
                    let d_value = datum.unwrap().as_int32() + 1;
                    assert_eq!(d_value, 9);
                } else {
                    unreachable!();
                }
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_sink_no_key() {
        // Prepare storage and memtable.
        let store_mgr = Arc::new(SimpleTableManager::new());
        let table_id = TableId::new(SchemaId::default(), 1);

        // Two columns of int32 type, no pk.
        let column_desc1 = ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            name: "v1".to_string(),
            is_primary: false,
        };
        let column_desc2 = ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            name: "v2".to_string(),
            is_primary: false,
        };
        let column_descs = vec![column_desc1.to_proto(), column_desc2.to_proto()];
        let _res = store_mgr.create_materialized_view(&table_id, column_descs, vec![]);
        // Prepare source chunks.
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I32Array, Int32Type, [1, 2, 3] },
                column_nonnull! { I32Array, Int32Type, [4, 5, 6] },
            ],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![Op::Insert, Op::Delete, Op::Insert],
            columns: vec![
                column_nonnull! { I32Array, Int32Type, [7, 3, 1] },
                column_nonnull! { I32Array, Int32Type, [8, 6, 4] },
            ],
            visibility: None,
        };
        // Prepare stream executors.
        let table_ref = store_mgr.get_table(&table_id).unwrap();
        if let TableTypes::Row(table) = table_ref {
            let schema = Schema {
                fields: vec![
                    Field {
                        data_type: Int32Type::create(false),
                    },
                    Field {
                        data_type: Int32Type::create(false),
                    },
                ],
            };
            let source = MockSource::with_chunks(schema, vec![chunk1, chunk2]);
            let mut sink_executor = Box::new(MViewSinkExecutor::new(
                Box::new(source),
                table.clone(),
                vec![],
            ));

            // First stream chunk. We check the existence of (1,4) -> (1)
            if let Message::Chunk(_chunk) = sink_executor.next().await.unwrap() {
                let value_row = Row(vec![Some(1.to_scalar_value()), Some(4.to_scalar_value())]);
                let res_row = table.get(value_row);
                if let Ok(res_row_in) = res_row {
                    let datum = res_row_in.unwrap().0.get(0).unwrap().clone();
                    // Dirty trick to assert_eq between (&int32 and integer).
                    let d_value = datum.unwrap().as_int32() + 1;
                    assert_eq!(d_value, 2);
                } else {
                    unreachable!();
                }
            } else {
                unreachable!();
            }

            // Second stream chunk. We check the existence of (1,4) -> (2)
            if let Message::Chunk(_chunk) = sink_executor.next().await.unwrap() {
                let value_row = Row(vec![Some(1.to_scalar_value()), Some(4.to_scalar_value())]);
                let res_row = table.get(value_row);
                if let Ok(res_row_in) = res_row {
                    let datum = res_row_in.unwrap().0.get(0).unwrap().clone();
                    // Dirty trick to assert_eq between (&int32 and integer).
                    let d_value = datum.unwrap().as_int32() + 1;
                    assert_eq!(d_value, 3);
                } else {
                    unreachable!();
                }
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }
    }
}
