mod app;
mod connection;
mod error;
mod protocol;

mod components;
mod models;
mod state;
mod ui;

fn main() {
    // let mut client = ClientBuilder::new()
    //     .brokers(vec!["localhost:9092"])
    //     .build()
    //     .await
    //     .unwrap();

    // let api_versions = client.get_api_versions().await.unwrap();

    // for v in api_versions.api_versions.iter() {
    //     println!(
    //         "API version: Api Key: {}, Min version {}, Max version {}",
    //         v.api_key, v.min_version, v.max_version
    //     )
    // }

    // let metadata = client.get_metadata(None).await.unwrap();
    // for broker in &metadata.brokers {
    //     println!(
    //         "  Broker {}: {}:{}",
    //         broker.node_id, broker.host, broker.port
    //     );
    // }

    // for topic in &metadata.topics {
    //     println!(" Topic {}", topic.topic_name);
    //     for p in topic.partitions.iter() {
    //         println!("  Partition: {}", p.partition_id);
    //     }
    // }

    // client.disconnect().await.unwrap();
}
