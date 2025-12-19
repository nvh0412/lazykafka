use std::sync::Arc;

use tokio::sync::RwLock;

use crate::repository::message_repository::MessageRepository;
use crate::{client::KafkaClient, repository::topic_repository::TopicRepository};

pub mod message_repository;
pub mod topic_repository;

pub struct RepositoryManager {
    pub kafka_client: Arc<RwLock<KafkaClient>>,
    topic: TopicRepository,
    message: MessageRepository,
}

impl RepositoryManager {
    pub fn new(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        Self {
            topic: TopicRepository::new(kafka_client.clone()),
            message: MessageRepository::new(kafka_client.clone()),
            kafka_client,
        }
    }

    pub fn topics(&self) -> &TopicRepository {
        &self.topic
    }

    pub fn messages(&self) -> &MessageRepository {
        &self.message
    }
}
