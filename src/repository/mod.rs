use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{client::KafkaClient, repository::topic_repository::TopicRepository};

pub mod topic_repository;

pub struct RepositoryManager {
    pub kafka_client: Arc<RwLock<KafkaClient>>,
    topic: TopicRepository,
}

impl RepositoryManager {
    pub fn new(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        Self {
            topic: TopicRepository::new(kafka_client.clone()),
            kafka_client: kafka_client,
        }
    }

    pub fn topics(&self) -> &TopicRepository {
        &self.topic
    }
}
