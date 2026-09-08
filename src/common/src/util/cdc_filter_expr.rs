// Copyright 2022 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Helpers for inspecting a `CdcFilter` search condition, shared by the streaming executor that
//! evaluates it and the meta service that turns it into a dispatcher routing predicate.

use risingwave_pb::expr::ExprNode;
use risingwave_pb::expr::expr_node::{RexNode, Type as ExprType};
use risingwave_pb::stream_plan::PbStreamNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use crate::types::{DataType, ScalarImpl};
use crate::util::stream_graph_visitor::visit_stream_node_body;
use crate::util::value_encoding::deserialize_datum;

/// Column index of `_rw_table_name` in CDC source chunks.
///
/// Current CDC source schema is `(payload, _rw_offset, _rw_table_name)`.
/// Keep this value in sync with the CDC source schema definition.
pub const RW_TABLE_NAME_COLUMN_IDX: u32 = 2;

/// Collect the `_rw_table_name` values the `CdcFilter` of a fragment accepts, for attaching to the
/// upstream dispatcher so it can skip rows belonging to other tables of a shared CDC source.
///
/// Returns an empty vec whenever the predicate cannot be read with certainty, leaving the
/// dispatcher forwarding everything. Under-delivering here would silently drop rows.
pub fn cdc_filter_table_names(node: Option<&PbStreamNode>) -> Vec<String> {
    let Some(node) = node else {
        return vec![];
    };

    let mut search_condition = None;
    let mut count = 0;
    visit_stream_node_body(node, |body| {
        if let NodeBody::CdcFilter(cdc_filter) = body {
            count += 1;
            search_condition = cdc_filter.search_condition.clone();
        }
    });
    // A fragment is expected to hold exactly one `CdcFilter`; bail out if that ever changes.
    if count != 1 {
        return vec![];
    }
    let Some(search_condition) = search_condition else {
        return vec![];
    };
    let Some(names) = extract_cdc_filter_table_names(&search_condition) else {
        return vec![];
    };

    // `CdcFilterExecutorBuilder` widens the predicate at build time for legacy SQL Server tables,
    // rewriting a three-part `db.schema.table` literal into `schema.table`. The literals here are
    // therefore narrower than what the executor accepts, so skip those rather than reimplementing
    // the rewrite.
    if names.iter().any(|name| name.split('.').count() > 2) {
        return vec![];
    }

    tracing::debug!(?names, "cdc source dispatcher will route by table name");
    names
}

/// Extract the `_rw_table_name` values accepted by a `CdcFilter` search condition.
///
/// Recognizes `InputRef(2) = '<literal>'` and `Or` trees of such equalities. The planner emits the
/// latter for quoted Postgres table names, and `CdcFilterExecutorBuilder` synthesizes one for
/// legacy SQL Server tables.
///
/// Returns `None` for any other shape, meaning "cannot filter" rather than "matches nothing".
pub fn extract_cdc_filter_table_names(search_condition: &ExprNode) -> Option<Vec<String>> {
    let mut names = Vec::new();
    collect_table_names(search_condition, &mut names)?;
    if names.is_empty() {
        return None;
    }
    Some(names)
}

fn collect_table_names(expr: &ExprNode, out: &mut Vec<String>) -> Option<()> {
    match expr.function_type() {
        ExprType::Or => {
            let RexNode::FuncCall(func_call) = expr.rex_node.as_ref()? else {
                return None;
            };
            for child in &func_call.children {
                collect_table_names(child, out)?;
            }
            Some(())
        }
        ExprType::Equal => {
            out.push(extract_eq_table_name(expr)?);
            Some(())
        }
        _ => None,
    }
}

/// Extract the literal from `InputRef(2) = '<literal>'`, in either operand order.
fn extract_eq_table_name(expr: &ExprNode) -> Option<String> {
    let RexNode::FuncCall(func_call) = expr.rex_node.as_ref()? else {
        return None;
    };
    let [lhs, rhs] = func_call.children.as_slice() else {
        return None;
    };

    if is_rw_table_name_ref(lhs) {
        extract_varchar_literal(rhs)
    } else if is_rw_table_name_ref(rhs) {
        extract_varchar_literal(lhs)
    } else {
        None
    }
}

fn is_rw_table_name_ref(expr: &ExprNode) -> bool {
    matches!(
        expr.rex_node,
        Some(RexNode::InputRef(RW_TABLE_NAME_COLUMN_IDX))
    )
}

/// The body is value encoding (`Datum::to_protobuf` calls `serialize_datum`), i.e. a null tag byte
/// followed by the payload, so it cannot be read as raw UTF-8.
fn extract_varchar_literal(expr: &ExprNode) -> Option<String> {
    let RexNode::Constant(constant) = expr.rex_node.as_ref()? else {
        return None;
    };
    match deserialize_datum(constant.body.as_slice(), &DataType::Varchar).ok()? {
        Some(ScalarImpl::Utf8(s)) => Some(s.into()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::data::PbDatum;
    use risingwave_pb::expr::FunctionCall;

    use super::*;

    fn varchar_type() -> risingwave_pb::data::DataType {
        risingwave_pb::data::DataType {
            type_name: risingwave_pb::data::data_type::TypeName::Varchar as i32,
            ..Default::default()
        }
    }

    fn bool_type() -> risingwave_pb::data::DataType {
        risingwave_pb::data::DataType {
            type_name: risingwave_pb::data::data_type::TypeName::Boolean as i32,
            ..Default::default()
        }
    }

    fn table_name_ref() -> ExprNode {
        ExprNode {
            function_type: ExprType::Unspecified as i32,
            return_type: Some(varchar_type()),
            rex_node: Some(RexNode::InputRef(RW_TABLE_NAME_COLUMN_IDX)),
        }
    }

    /// Encode the literal the way the planner does.
    fn literal(v: &str) -> ExprNode {
        let datum = Some(ScalarImpl::Utf8(v.into()));
        ExprNode {
            function_type: ExprType::Unspecified as i32,
            return_type: Some(varchar_type()),
            rex_node: Some(RexNode::Constant(PbDatum {
                body: crate::util::value_encoding::serialize_datum(&datum),
            })),
        }
    }

    fn eq(lhs: ExprNode, rhs: ExprNode) -> ExprNode {
        ExprNode {
            function_type: ExprType::Equal as i32,
            return_type: Some(bool_type()),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![lhs, rhs],
            })),
        }
    }

    fn or(children: Vec<ExprNode>) -> ExprNode {
        ExprNode {
            function_type: ExprType::Or as i32,
            return_type: Some(bool_type()),
            rex_node: Some(RexNode::FuncCall(FunctionCall { children })),
        }
    }

    #[test]
    fn test_simple_equality() {
        let expr = eq(table_name_ref(), literal("public.orders"));
        assert_eq!(
            extract_cdc_filter_table_names(&expr),
            Some(vec!["public.orders".to_owned()])
        );
    }

    #[test]
    fn test_reversed_operands() {
        let expr = eq(literal("public.orders"), table_name_ref());
        assert_eq!(
            extract_cdc_filter_table_names(&expr),
            Some(vec!["public.orders".to_owned()])
        );
    }

    #[test]
    fn test_or_of_equalities() {
        let expr = or(vec![
            eq(table_name_ref(), literal("public.\"Orders\"")),
            eq(table_name_ref(), literal("public.Orders")),
        ]);
        assert_eq!(
            extract_cdc_filter_table_names(&expr),
            Some(vec![
                "public.\"Orders\"".to_owned(),
                "public.Orders".to_owned()
            ])
        );
    }

    #[test]
    fn test_nested_or() {
        let expr = or(vec![
            eq(table_name_ref(), literal("a")),
            or(vec![
                eq(table_name_ref(), literal("b")),
                eq(table_name_ref(), literal("c")),
            ]),
        ]);
        assert_eq!(
            extract_cdc_filter_table_names(&expr),
            Some(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()])
        );
    }

    /// An unrecognized shape must yield `None` so the caller keeps forwarding everything.
    #[test]
    fn test_unrecognized_shapes_fail_open() {
        // A different column.
        let other_ref = ExprNode {
            function_type: ExprType::Unspecified as i32,
            return_type: Some(varchar_type()),
            rex_node: Some(RexNode::InputRef(1)),
        };
        assert_eq!(
            extract_cdc_filter_table_names(&eq(other_ref, literal("x"))),
            None
        );

        // A conjunction rather than a disjunction.
        let and_expr = ExprNode {
            function_type: ExprType::And as i32,
            return_type: Some(bool_type()),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![
                    eq(table_name_ref(), literal("a")),
                    eq(table_name_ref(), literal("b")),
                ],
            })),
        };
        assert_eq!(extract_cdc_filter_table_names(&and_expr), None);

        // An `Or` with a non-equality branch.
        let mixed = or(vec![
            eq(table_name_ref(), literal("a")),
            ExprNode {
                function_type: ExprType::IsNotNull as i32,
                return_type: Some(bool_type()),
                rex_node: Some(RexNode::FuncCall(FunctionCall {
                    children: vec![table_name_ref()],
                })),
            },
        ]);
        assert_eq!(extract_cdc_filter_table_names(&mixed), None);
    }

    /// A raw-UTF-8 read would keep the leading null tag byte.
    #[test]
    fn test_literal_is_value_encoded() {
        let lit = literal("public.orders");
        let Some(RexNode::Constant(datum)) = &lit.rex_node else {
            unreachable!()
        };
        assert_ne!(datum.body.as_slice(), b"public.orders");
        assert_eq!(
            extract_cdc_filter_table_names(&eq(table_name_ref(), lit)),
            Some(vec!["public.orders".to_owned()])
        );
    }
}
