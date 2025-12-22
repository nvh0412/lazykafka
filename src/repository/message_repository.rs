use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::client::KafkaClient;
use crate::error;
use crate::protocol::fetch::{FetchPartition, FetchTopic};
use crate::protocol::list_offset::{ListOffsetPartition, ListOffsetPartitionResponse, ListOffsetTopic};
use crate::protocol::record::Record;

#[derive(Clone)]
pub struct MessageRepository {
    kafka_client: Arc<RwLock<KafkaClient>>,
}

impl MessageRepository {
    pub fn new(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        Self { kafka_client }
    }

    pub async fn fetch_by_topic(&self, topic: String) -> error::Result<Vec<Record>> {
        let mut client = self.kafka_client.write().await;

        let offset_topics = vec![ListOffsetTopic {
            name: topic,
            partitions: vec![ListOffsetPartition {
                partition_index: 0,
                timestamp: 0,
            }],
        }];

        let mut topic_map: HashMap<String, Vec<ListOffsetPartitionResponse>> = HashMap::new();

        let results = client.list_offsets(offset_topics).await?;

        for t in results.topics {
            topic_map.insert(t.name, t.partitions);
        };

        let mut topics: Vec<FetchTopic> = Vec::new();

        topic_map.iter().for_each(|(topic, offsets)| {
            let mut fetch_topic_request = FetchTopic::new(topic.clone());

            offsets.iter().for_each(|offset| {
                fetch_topic_request.add_partition(FetchPartition {
                    partition: offset.partition_index,
                    fetch_offset: offset.offset,
                    partition_max_bytes: 1_048_576,
                });
            });

            topics.push(fetch_topic_request);
        });

        let mut records = Vec::new();
        let response = client.fetch_messages(topics).await?;

        for topic_r in response.responses {
            for p in topic_r.paritions {
                for batch in p.records {
                    for r in batch.records {
                        records.push(r);
                    }
                }
            }
        }

        Ok(records)
    }
}
