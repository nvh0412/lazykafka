use std::sync::Arc;

use tokio::sync::RwLock;

use crate::repository::consumer_group_repository::ConsumerGroupRepository;
use crate::repository::message_repository::MessageRepository;
use crate::{client::KafkaClient, repository::topic_repository::TopicRepository};

pub mod consumer_group_repository;
pub mod message_repository;
pub mod topic_repository;

pub struct RepositoryManager {
    pub kafka_client: Arc<RwLock<KafkaClient>>,
    topic: TopicRepository,
    message: MessageRepository,
    consumer_group: ConsumerGroupRepository,
}

impl RepositoryManager {
    pub fn new(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        Self {
            topic: TopicRepository::new(kafka_client.clone()),
            message: MessageRepository::new(kafka_client.clone()),
            consumer_group: ConsumerGroupRepository::new(kafka_client.clone()),
            kafka_client,
        }
    }

    pub fn topics(&self) -> &TopicRepository {
        &self.topic
    }

    pub fn messages(&self) -> &MessageRepository {
        &self.message
    }

    pub fn consumer_groups(&self) -> &ConsumerGroupRepository {
        &self.consumer_group
    }
}
