use crate::protocol::metadata::TopicMetadata;

#[derive(Debug, Clone)]
pub struct Topic {
    pub name: String,
    pub partitions: usize,
    pub replication_factor: u16,
    // pub config: TopicConfig,
    // pub metrics: TopicMetrics,
}

impl From<&TopicMetadata> for Topic {
    fn from(value: &TopicMetadata) -> Self {
        Self {
            name: value.topic_name.clone(),
            partitions: value.partitions.len(),
            replication_factor: value
                .partitions
                .first()
                .map(|p| p.replicas.len() as u16)
                .unwrap_or(0),
        }
    }
}
