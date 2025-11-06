use std::{
    io::{Read, Result, Write},
    net::TcpStream,
};

use kafkatui_rs::client_builder::ClientBuilder;

mod connection;
mod error;
mod protocol;

#[tokio::main]
async fn main() {
    let mut client = ClientBuilder::new()
        .brokers(vec!["localhost:9092"])
        .build()
        .await
        .unwrap();

    // let api_versions = client.get_api_versions().await.unwrap();

    // for v in api_versions.api_versions.iter() {
    //     println!(
    //         "API version: Api Key: {}, Min version {}, Max version {}",
    //         v.api_key, v.min_version, v.max_version
    //     )
    // }

    let metadata = client.get_metadata(None).await.unwrap();
    for broker in &metadata.brokers {
        println!(
            "  Broker {}: {}:{}",
            broker.node_id, broker.host, broker.port
        );
    }

    for topic in &metadata.topics {
        println!(" Topic {}", topic.topic_name);
        for p in topic.partitions.iter() {
            println!("  Partition: {}", p.partition_id);
        }
    }

    client.disconnect().await.unwrap();
}

#[cfg(test)]
mod round_trip_tests {
    use crate::protocol::{message_buffer::MessageBuffer, message_decoder::MessageDecoder};

    #[test]
    fn test_complete_round_trip() {
        // encode
        let mut buf = MessageBuffer::new();
        buf.put_i32(42);
        buf.put_string("Kafka");
        buf.put_nullable_string(None);
        buf.put_bool(true);
        let encoded = buf.into_bytes();

        // Decode
        let mut decoder = MessageDecoder::new(&encoded);
        assert_eq!(decoder.read_i32().unwrap(), 42);
        assert_eq!(decoder.read_string().unwrap(), "Kafka");
        assert_eq!(decoder.read_nullable_string().unwrap(), None);
        assert_eq!(decoder.read_bool().unwrap(), true);
        assert_eq!(decoder.remaining(), 0);
    }
}
