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

#[cfg(test)]
mod tests {
    use crate::source::spanner_cdc::types::*;
    use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};
    use crate::source::{SplitId, SplitMetaData};

    #[test]
    fn test_spanner_cdc_split_id() {
        use time::OffsetDateTime;

        let split = SpannerCdcSplit::new(
            Some("test-token".to_string()),
            OffsetDateTime::now_utc(),
            "test-stream".to_string(),
            0,
        );

        let split_id = split.id();
        assert!(split_id.to_string().contains("test-stream"));
        assert!(split_id.to_string().contains("test-token"));
    }

    #[test]
    fn test_spanner_cdc_split_root_partition() {
        use time::OffsetDateTime;

        let split = SpannerCdcSplit::new(
            None, // Root partition
            OffsetDateTime::now_utc(),
            "test-stream".to_string(),
            0,
        );

        let split_id = split.id();
        assert!(split_id.to_string().contains("test-stream"));
        assert!(split_id.to_string().contains("root"));
    }

    #[test]
    fn test_spanner_cdc_split_serialization() {
        use time::OffsetDateTime;

        let split = SpannerCdcSplit::new(
            Some("test-token".to_string()),
            OffsetDateTime::now_utc(),
            "test-stream".to_string(),
            0,
        );

        let json = split.encode_to_json();
        let restored = SpannerCdcSplit::restore_from_json(json).unwrap();

        assert_eq!(split.partition_token, restored.partition_token);
        assert_eq!(split.stream_name, restored.stream_name);
        assert_eq!(split.index, restored.index);
    }

    #[test]
    fn test_spanner_cdc_split_checkpoint() {
        use time::OffsetDateTime;

        let mut split = SpannerCdcSplit::new(
            Some("test-token".to_string()),
            OffsetDateTime::now_utc(),
            "test-stream".to_string(),
            0,
        );

        assert_eq!(split.messages_processed, 0);
        assert!(split.last_commit_timestamp.is_none());

        // Simulate processing messages
        let checkpoint_time = OffsetDateTime::now_utc();
        split.last_commit_timestamp = Some(checkpoint_time);
        split.messages_processed = 100;

        assert_eq!(split.messages_processed, 100);
        assert_eq!(split.effective_start_timestamp(), checkpoint_time);
    }

    #[test]
    fn test_mod_to_json_map() {
        use serde_json::json;

        let modification = Mod {
            keys: Some(json!({"id": "123"})),
            new_values: Some(json!({"id": "123", "name": "test"})),
            old_values: None,
        };

        let map = modification.to_json_map();
        assert!(map.contains_key("keys"));
        assert!(map.contains_key("new_values"));
        assert!(!map.contains_key("old_values"));
    }

    #[test]
    fn test_spanner_cdc_properties_heartbeat_interval() {
        use std::collections::HashMap;

        let properties = SpannerCdcProperties {
            spanner_dsn: "projects/test/instances/test/databases/test".to_string(),
            stream_name: "test-stream".to_string(),
            heartbeat_interval: "5s".to_string(),
            start_time: None,
            end_time: None,
            credentials: None,
            emulator_host: None,
            parallelism: Some(1),
            max_concurrent_partitions: None,
            buffer_size: None,
            enable_partition_discovery: true,
            retry_attempts: None,
            retry_backoff_ms: None,
            unknown_fields: HashMap::new(),
        };

        let interval_ms = properties.heartbeat_interval_ms().unwrap();
        assert_eq!(interval_ms, 5000);
    }

    #[test]
    fn test_spanner_cdc_properties_parse_times() {
        use std::collections::HashMap;

        let properties = SpannerCdcProperties {
            spanner_dsn: "projects/test/instances/test/databases/test".to_string(),
            stream_name: "test-stream".to_string(),
            heartbeat_interval: "3s".to_string(),
            start_time: Some("2025-01-01T00:00:00Z".to_string()),
            end_time: Some("2025-12-31T23:59:59Z".to_string()),
            credentials: None,
            emulator_host: None,
            parallelism: Some(1),
            max_concurrent_partitions: None,
            buffer_size: None,
            enable_partition_discovery: true,
            retry_attempts: None,
            retry_backoff_ms: None,
            unknown_fields: HashMap::new(),
        };

        let start_time = properties.parse_start_time().unwrap();
        assert!(start_time.is_some());

        let end_time = properties.parse_end_time().unwrap();
        assert!(end_time.is_some());
    }

    #[test]
    fn test_spanner_cdc_properties_invalid_time() {
        use std::collections::HashMap;

        let properties = SpannerCdcProperties {
            spanner_dsn: "projects/test/instances/test/databases/test".to_string(),
            stream_name: "test-stream".to_string(),
            heartbeat_interval: "3s".to_string(),
            start_time: Some("invalid-time".to_string()),
            end_time: None,
            credentials: None,
            emulator_host: None,
            parallelism: Some(1),
            max_concurrent_partitions: None,
            buffer_size: None,
            enable_partition_discovery: true,
            retry_attempts: None,
            retry_backoff_ms: None,
            unknown_fields: HashMap::new(),
        };

        let result = properties.parse_start_time();
        assert!(result.is_err());
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
}
