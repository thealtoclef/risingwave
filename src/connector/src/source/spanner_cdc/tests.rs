// Copyright 2024 RisingWave Labs
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

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;

    use crate::source::SplitMetaData;
    use crate::source::spanner_cdc::SpannerCdcSplit;
    use crate::source::spanner_cdc::types::ChangeStreamRecord;

    #[test]
    fn test_split_root_partition() {
        let split = SpannerCdcSplit::new_root("test-stream".to_string(), 0, OffsetDateTime::now_utc());

        assert!(split.is_root());
        assert!(split.partition_token.is_none());
        assert!(split.parent_partition_tokens.is_empty());
        assert!(split.offset.is_some());
        assert!(!split.snapshot_done);
        // Root split ID is the index
        assert_eq!(split.id().to_string(), "0");
    }

    #[test]
    fn test_split_child_partition() {
        let split = SpannerCdcSplit::new_child(
            "child-token".to_string(),
            vec!["parent-token".to_string()],
            OffsetDateTime::now_utc(),
            "test-stream".to_string(),
            1,
        );

        assert!(!split.is_root());
        assert_eq!(split.partition_token, Some("child-token".to_string()));
        assert_eq!(split.parent_partition_tokens, vec!["parent-token".to_string()]);
        assert_eq!(split.id().to_string(), "1");
    }

    #[test]
    fn test_split_serialization_roundtrip() {
        let split = SpannerCdcSplit::new_root("test-stream".to_string(), 0, OffsetDateTime::now_utc());

        let json = split.encode_to_json();
        let restored = SpannerCdcSplit::restore_from_json(json).unwrap();

        assert_eq!(split.partition_token, restored.partition_token);
        assert_eq!(split.stream_name, restored.stream_name);
        assert_eq!(split.index, restored.index);
        assert_eq!(split.snapshot_done, restored.snapshot_done);
    }

    #[test]
    fn test_split_advance_offset() {
        let ts = OffsetDateTime::now_utc();
        let mut split = SpannerCdcSplit::new_root("test-stream".to_string(), 0, ts);

        let new_ts = ts + time::Duration::seconds(10);
        split.advance_offset(new_ts);

        assert_eq!(split.offset, Some(new_ts));
    }

    #[test]
    fn test_change_stream_record_empty() {
        let record = ChangeStreamRecord {
            data_change_record: vec![],
            heartbeat_record: vec![],
            child_partitions_record: vec![],
        };

        assert!(record.data_change_record.is_empty());
        assert!(record.heartbeat_record.is_empty());
        assert!(record.child_partitions_record.is_empty());
    }

    #[test]
    fn test_heartbeat_interval_parsing() {
        let props: SpannerCdcProperties = serde_json::from_value(serde_json::json!({
            "connector": "spanner-cdc",
            "spanner.project": "test-project",
            "spanner.instance": "test-instance",
            "database.name": "test-db",
            "spanner.stream_name": "test-stream",
            "spanner.heartbeat_interval": "5s",
        }))
        .unwrap();

        assert_eq!(props.heartbeat_interval_ms().unwrap(), 5000);
    }

    #[test]
    fn test_properties_defaults() {
        let props: SpannerCdcProperties = serde_json::from_value(serde_json::json!({
            "connector": "spanner-cdc",
            "spanner.project": "test-project",
            "spanner.instance": "test-instance",
            "database.name": "test-db",
            "spanner.stream_name": "test-stream",
        }))
        .unwrap();

        assert_eq!(props.get_max_concurrent_partitions(), 100);
        assert_eq!(props.get_buffer_size(), 1024);
        assert_eq!(props.get_retry_attempts(), 3);
        assert!(!props.enable_databoost);
        assert!(!props.auto_schema_change);
    }

    use crate::source::spanner_cdc::SpannerCdcProperties;
}
