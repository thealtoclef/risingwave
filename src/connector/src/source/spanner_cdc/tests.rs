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
#[expect(clippy::module_inception)]
mod tests {
    use time::OffsetDateTime;

    use crate::source::SplitMetaData;
    use crate::source::spanner_cdc::SpannerCdcSplit;
    use crate::source::spanner_cdc::types::ChangeStreamRecord;

    #[test]
    fn test_split_root_partition() {
        let split =
            SpannerCdcSplit::new_root("test-stream".to_owned(), 0, OffsetDateTime::now_utc());

        assert!(split.is_root());
        assert!(split.partition_token.is_none());
        assert!(split.parent_partition_tokens.is_empty());
        assert!(split.offset.is_some());
        // Root split ID is the index
        assert_eq!(split.id().to_string(), "0");
    }

    #[test]
    fn test_split_child_partition() {
        let split = SpannerCdcSplit::new_child(
            "child-token".to_owned(),
            vec!["parent-token".to_owned()],
            OffsetDateTime::now_utc(),
            "test-stream".to_owned(),
            1,
        );

        assert!(!split.is_root());
        assert_eq!(split.partition_token, Some("child-token".to_owned()));
        assert_eq!(
            split.parent_partition_tokens,
            vec!["parent-token".to_owned()]
        );
        assert_eq!(split.id().to_string(), "1");
    }

    #[test]
    fn test_split_serialization_roundtrip() {
        let split =
            SpannerCdcSplit::new_root("test-stream".to_owned(), 0, OffsetDateTime::now_utc());

        let json = split.encode_to_json();
        let restored = SpannerCdcSplit::restore_from_json(json).unwrap();

        assert_eq!(split.partition_token, restored.partition_token);
        assert_eq!(split.change_stream_name, restored.change_stream_name);
        assert_eq!(split.index, restored.index);
    }

    #[test]
    fn test_split_advance_offset() {
        let ts = OffsetDateTime::now_utc();
        let mut split = SpannerCdcSplit::new_root("test-stream".to_owned(), 0, ts);

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
    fn test_heartbeat_milliseconds_default() {
        let props: SpannerCdcProperties = serde_json::from_value(serde_json::json!({
            "connector": "spanner-cdc",
            "spanner.project": "test-project",
            "spanner.instance": "test-instance",
            "database.name": "test-db",
            "spanner.change_stream.name": "test-stream",
        }))
        .unwrap();

        assert_eq!(props.heartbeat_milliseconds, 2000);
    }

    #[test]
    fn test_heartbeat_milliseconds_override() {
        // The DDL parser always serializes source properties as JSON strings;
        // `DisplayFromStr` handles the string-to-i64 conversion.
        let props: SpannerCdcProperties = serde_json::from_value(serde_json::json!({
            "connector": "spanner-cdc",
            "spanner.project": "test-project",
            "spanner.instance": "test-instance",
            "database.name": "test-db",
            "spanner.change_stream.name": "test-stream",
            "spanner.heartbeat_milliseconds": "5000",
        }))
        .unwrap();

        assert_eq!(props.heartbeat_milliseconds, 5000);
    }

    #[test]
    fn test_properties_defaults() {
        let props: SpannerCdcProperties = serde_json::from_value(serde_json::json!({
            "connector": "spanner-cdc",
            "spanner.project": "test-project",
            "spanner.instance": "test-instance",
            "database.name": "test-db",
            "spanner.change_stream.name": "test-stream",
        }))
        .unwrap();

        assert_eq!(props.get_retry_attempts(), 5);
    }

    use crate::source::spanner_cdc::SpannerCdcProperties;

    #[test]
    fn test_max_missed_heartbeats_default() {
        let props: SpannerCdcProperties = serde_json::from_value(serde_json::json!({
            "connector": "spanner-cdc",
            "spanner.project": "test-project",
            "spanner.instance": "test-instance",
            "database.name": "test-db",
            "spanner.change_stream.name": "test-stream",
        }))
        .unwrap();

        // Default: 10 missed heartbeats * 2000ms heartbeat = 20s stall timeout
        assert_eq!(
            props.get_stall_timeout(),
            std::time::Duration::from_millis(20_000)
        );
    }

    #[test]
    fn test_max_missed_heartbeats_override() {
        let props: SpannerCdcProperties = serde_json::from_value(serde_json::json!({
            "connector": "spanner-cdc",
            "spanner.project": "test-project",
            "spanner.instance": "test-instance",
            "database.name": "test-db",
            "spanner.change_stream.name": "test-stream",
            "spanner.heartbeat_milliseconds": "10000",
            "spanner.max_missed_heartbeats": "5",
        }))
        .unwrap();

        // 5 missed heartbeats * 10000ms = 50s
        assert_eq!(
            props.get_stall_timeout(),
            std::time::Duration::from_millis(50_000)
        );
    }

    fn props_with(overrides: &[(&str, &str)]) -> SpannerCdcProperties {
        let mut json = serde_json::json!({
            "connector": "spanner-cdc",
            "spanner.project": "test-project",
            "spanner.instance": "test-instance",
            "database.name": "test-db",
            "spanner.change_stream.name": "test-stream",
        });
        for (key, value) in overrides {
            json[*key] = serde_json::Value::from(*value);
        }
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn test_validate_options() {
        props_with(&[]).validate().unwrap();
        props_with(&[
            ("spanner.heartbeat_milliseconds", "1000"),
            ("spanner.max_missed_heartbeats", "1"),
        ])
        .validate()
        .unwrap();
        props_with(&[("spanner.heartbeat_milliseconds", "300000")])
            .validate()
            .unwrap();

        for (key, value) in [
            ("spanner.heartbeat_milliseconds", "999"),
            ("spanner.heartbeat_milliseconds", "300001"),
            ("spanner.max_missed_heartbeats", "0"),
            ("spanner.retry_backoff_ms", "0"),
            ("spanner.retry_backoff_max_delay_ms", "0"),
            ("spanner.retry_backoff_factor", "0"),
        ] {
            let err = props_with(&[(key, value)]).validate().unwrap_err();
            assert!(err.to_string().contains(key), "{key} = {value}: {err}");
        }
    }

    #[test]
    fn test_stall_timeout_saturates() {
        let props = props_with(&[
            ("spanner.heartbeat_milliseconds", &i64::MAX.to_string()),
            ("spanner.max_missed_heartbeats", &u32::MAX.to_string()),
        ]);
        assert_eq!(
            props.get_stall_timeout(),
            std::time::Duration::from_millis(u64::MAX)
        );
    }
}
