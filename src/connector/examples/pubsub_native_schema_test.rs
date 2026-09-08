//! Manual verification for the pubsub-native-schema-auto-fetch feature (DATA-5059).
//! Not a CI test: needs live GCP credentials. Run with:
//!   cargo run -p risingwave_connector --example pubsub_native_schema_test -- <schema_name> <credentials_json_path> [hex_payload]

use prost_reflect::DynamicMessage;
use risingwave_connector::schema::protobuf::fetch_from_pubsub_native;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let schema_name = &args[1];
    let credentials = std::fs::read_to_string(&args[2]).unwrap();
    let hex_payload = args.get(3);

    let file_descriptor = fetch_from_pubsub_native(schema_name, None, Some(&credentials))
        .await
        .expect("fetch_from_pubsub_native failed");

    println!("fetched file: {}", file_descriptor.name());
    let messages: Vec<_> = file_descriptor.parent_pool().all_messages().collect();
    for m in &messages {
        println!("message: {}", m.full_name());
        for f in m.fields() {
            println!("  field: {} (number {})", f.name(), f.number());
        }
    }

    if let Some(hex) = hex_payload {
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let message_descriptor = messages.into_iter().next().expect("no message in schema");
        let decoded = DynamicMessage::decode(message_descriptor, bytes.as_slice())
            .expect("failed to decode payload against fetched schema");
        println!("decoded message: {:?}", decoded);
    }
}
