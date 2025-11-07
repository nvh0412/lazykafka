#[derive(Debug, Clone)]
pub struct Topic {
    pub name: String,
    pub partitions: usize,
    pub replication_factor: u16,
    // pub config: TopicConfig,
    // pub metrics: TopicMetrics,
}
