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

use chrono::{TimeZone, Utc};
use google_cloud_pubsub::subscriber::ReceivedMessage;
use itertools::Itertools;
use risingwave_common::types::{
    Datum, DatumCow, DatumRef, ListValue, ScalarImpl, ScalarRefImpl, StructValue, Timestamptz,
};
use risingwave_pb::data::DataType as PbDataType;
use risingwave_pb::data::data_type::TypeName as PbTypeName;

use crate::parser::additional_columns::get_kafka_header_item_datatype;
use crate::source::{SourceMessage, SourceMeta, SplitId};

#[derive(Debug, Clone)]
pub struct GooglePubsubMeta {
    // timestamp(milliseconds) of message append in mq
    pub timestamp: Option<i64>,
    // server-assigned id, stable across redeliveries
    pub message_id: String,
    // message attributes, sorted by key
    pub attributes: Vec<(String, String)>,
    // only set when the subscription has a dead-letter policy
    pub delivery_attempt: Option<i32>,
    // kept out of `SourceMessage::key` so parsers that always read the key (e.g. Debezium)
    // and the empty-message skip are unaffected; only `INCLUDE key` reads it
    pub ordering_key: String,
}

impl GooglePubsubMeta {
    pub fn extract_timestamp(&self) -> DatumRef<'_> {
        Some(Timestamptz::from_millis(self.timestamp?)?.into())
    }

    pub fn extract_message_id(&self) -> DatumRef<'_> {
        (!self.message_id.is_empty()).then(|| ScalarRefImpl::Utf8(&self.message_id))
    }

    pub fn extract_ordering_key(&self) -> DatumRef<'_> {
        (!self.ordering_key.is_empty()).then(|| ScalarRefImpl::Bytea(self.ordering_key.as_bytes()))
    }

    pub fn extract_delivery_attempt(&self) -> DatumRef<'_> {
        self.delivery_attempt.map(ScalarRefImpl::Int32)
    }

    pub fn extract_header_inner<'a>(
        &'a self,
        inner_field: &str,
        data_type: Option<&PbDataType>,
    ) -> Option<DatumCow<'a>> {
        let (_, value) = self.attributes.iter().find(|(key, _)| key == inner_field)?;

        if let Some(data_type) = data_type
            && data_type.type_name == PbTypeName::Varchar as i32
        {
            Some(Some(ScalarRefImpl::Utf8(value.as_str())).into())
        } else {
            Some(Some(ScalarRefImpl::Bytea(value.as_bytes())).into())
        }
    }

    pub fn extract_headers(&self) -> Option<Datum> {
        if self.attributes.is_empty() {
            return None;
        }
        let header_item: Vec<Datum> = self
            .attributes
            .iter()
            .map(|(key, value)| {
                Some(ScalarImpl::Struct(StructValue::new(vec![
                    Some(ScalarImpl::Utf8(key.as_str().into())),
                    Some(ScalarImpl::Bytea(value.as_bytes().into())),
                ])))
            })
            .collect_vec();
        Some(Some(ScalarImpl::List(ListValue::from_datum_iter(
            &get_kafka_header_item_datatype(),
            header_item,
        ))))
    }
}

/// Tag a `ReceivedMessage` from cloud pubsub so we can inject the virtual split-id into the
/// `SourceMessage`
pub(crate) struct TaggedReceivedMessage(pub(crate) SplitId, pub(crate) ReceivedMessage);

impl From<TaggedReceivedMessage> for SourceMessage {
    fn from(tagged_message: TaggedReceivedMessage) -> Self {
        let TaggedReceivedMessage(split_id, message) = tagged_message;

        let ack_id = message.ack_id().to_owned();
        let delivery_attempt = message.delivery_attempt().map(|n| n as i32);
        let timestamp = message
            .message
            .publish_time
            .and_then(|t| Utc.timestamp_opt(t.seconds, t.nanos as u32).single())
            .map(|t| t.timestamp_millis());
        let message_id = message.message.message_id;
        let attributes = message
            .message
            .attributes
            .into_iter()
            .sorted_unstable()
            .collect_vec();
        let ordering_key = message.message.ordering_key;

        Self {
            key: None,
            payload: {
                let payload = message.message.data;
                match payload.len() {
                    0 => None,
                    _ => Some(payload),
                }
            },
            offset: ack_id,
            split_id,
            meta: SourceMeta::GooglePubsub(GooglePubsubMeta {
                timestamp,
                message_id,
                attributes,
                delivery_attempt,
                ordering_key,
            }),
        }
    }
}
