use std::sync::Arc;

use tokio::sync::RwLock;

use crate::client::KafkaClient;
use crate::{error::Result, models::topic::Topic};

#[derive(Clone)]
pub struct TopicRepository {
    kafka_client: Arc<RwLock<KafkaClient>>,
}

impl TopicRepository {
    pub fn new(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        Self { kafka_client }
    }

    pub async fn fetch_all(&self) -> Result<Vec<Topic>> {
        let mut client = self.kafka_client.write().await;
        let metadata = client.get_metadata(None).await?;

        let topics: Vec<Topic> = metadata
            .topics
            .iter()
            .filter(|t| !t.topic_name.starts_with('_'))
            .map(Topic::from)
            .collect();

        Ok(topics)
    }
}
