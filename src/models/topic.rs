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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::metadata::PartitionMetadata;

    #[test]
    fn test_topic_construction() {
        let topic = Topic {
            name: "test-topic".to_string(),
            partitions: 3,
            replication_factor: 2,
        };
        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.partitions, 3);
        assert_eq!(topic.replication_factor, 2);
    }

    #[test]
    fn test_topic_clone() {
        let topic = Topic {
            name: "test-topic".to_string(),
            partitions: 5,
            replication_factor: 3,
        };
        let cloned = topic.clone();
        assert_eq!(topic.name, cloned.name);
        assert_eq!(topic.partitions, cloned.partitions);
        assert_eq!(topic.replication_factor, cloned.replication_factor);
    }

    #[test]
    fn test_topic_debug() {
        let topic = Topic {
            name: "test-topic".to_string(),
            partitions: 2,
            replication_factor: 1,
        };
        let debug_str = format!("{:?}", topic);
        assert!(debug_str.contains("Topic"));
        assert!(debug_str.contains("test-topic"));
    }

    #[test]
    fn test_topic_from_metadata_single_partition() {
        let metadata = TopicMetadata {
            error_code: 0,
            topic_name: "single-partition-topic".to_string(),
            partitions: vec![PartitionMetadata {
                error_code: 0,
                partition_id: 0,
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
            }],
        };

        let topic = Topic::from(&metadata);
        assert_eq!(topic.name, "single-partition-topic");
        assert_eq!(topic.partitions, 1);
        assert_eq!(topic.replication_factor, 3);
    }

    #[test]
    fn test_topic_from_metadata_multiple_partitions() {
        let metadata = TopicMetadata {
            error_code: 0,
            topic_name: "multi-partition-topic".to_string(),
            partitions: vec![
                PartitionMetadata {
                    error_code: 0,
                    partition_id: 0,
                    leader: 1,
                    replicas: vec![1, 2],
                    isr: vec![1, 2],
                },
                PartitionMetadata {
                    error_code: 0,
                    partition_id: 1,
                    leader: 2,
                    replicas: vec![2, 3],
                    isr: vec![2, 3],
                },
                PartitionMetadata {
                    error_code: 0,
                    partition_id: 2,
                    leader: 3,
                    replicas: vec![3, 1],
                    isr: vec![3, 1],
                },
            ],
        };

        let topic = Topic::from(&metadata);
        assert_eq!(topic.name, "multi-partition-topic");
        assert_eq!(topic.partitions, 3);
        assert_eq!(topic.replication_factor, 2);
    }

    #[test]
    fn test_topic_from_metadata_empty_partitions() {
        let metadata = TopicMetadata {
            error_code: 0,
            topic_name: "empty-topic".to_string(),
            partitions: vec![],
        };

        let topic = Topic::from(&metadata);
        assert_eq!(topic.name, "empty-topic");
        assert_eq!(topic.partitions, 0);
        assert_eq!(topic.replication_factor, 0);
    }

    #[test]
    fn test_topic_from_metadata_no_replicas() {
        let metadata = TopicMetadata {
            error_code: 0,
            topic_name: "no-replicas-topic".to_string(),
            partitions: vec![PartitionMetadata {
                error_code: 0,
                partition_id: 0,
                leader: 1,
                replicas: vec![],
                isr: vec![],
            }],
        };

        let topic = Topic::from(&metadata);
        assert_eq!(topic.name, "no-replicas-topic");
        assert_eq!(topic.partitions, 1);
        assert_eq!(topic.replication_factor, 0);
    }

    #[test]
    fn test_topic_from_metadata_single_replica() {
        let metadata = TopicMetadata {
            error_code: 0,
            topic_name: "single-replica-topic".to_string(),
            partitions: vec![PartitionMetadata {
                error_code: 0,
                partition_id: 0,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
            }],
        };

        let topic = Topic::from(&metadata);
        assert_eq!(topic.name, "single-replica-topic");
        assert_eq!(topic.partitions, 1);
        assert_eq!(topic.replication_factor, 1);
    }

    #[test]
    fn test_topic_from_metadata_many_replicas() {
        let metadata = TopicMetadata {
            error_code: 0,
            topic_name: "many-replicas-topic".to_string(),
            partitions: vec![PartitionMetadata {
                error_code: 0,
                partition_id: 0,
                leader: 1,
                replicas: vec![1, 2, 3, 4, 5],
                isr: vec![1, 2, 3, 4, 5],
            }],
        };

        let topic = Topic::from(&metadata);
        assert_eq!(topic.name, "many-replicas-topic");
        assert_eq!(topic.partitions, 1);
        assert_eq!(topic.replication_factor, 5);
    }

    #[test]
    fn test_topic_from_metadata_special_characters_in_name() {
        let metadata = TopicMetadata {
            error_code: 0,
            topic_name: "test.topic-with_special.chars".to_string(),
            partitions: vec![PartitionMetadata {
                error_code: 0,
                partition_id: 0,
                leader: 1,
                replicas: vec![1, 2],
                isr: vec![1, 2],
            }],
        };

        let topic = Topic::from(&metadata);
        assert_eq!(topic.name, "test.topic-with_special.chars");
        assert_eq!(topic.partitions, 1);
        assert_eq!(topic.replication_factor, 2);
    }

    #[test]
    fn test_topic_from_metadata_empty_name() {
        let metadata = TopicMetadata {
            error_code: 0,
            topic_name: String::new(),
            partitions: vec![PartitionMetadata {
                error_code: 0,
                partition_id: 0,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
            }],
        };

        let topic = Topic::from(&metadata);
        assert_eq!(topic.name, "");
        assert_eq!(topic.partitions, 1);
        assert_eq!(topic.replication_factor, 1);
    }

    #[test]
    fn test_topic_from_metadata_large_partition_count() {
        let mut partitions = Vec::new();
        for i in 0..100 {
            partitions.push(PartitionMetadata {
                error_code: 0,
                partition_id: i,
                leader: (i % 3) + 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
            });
        }

        let metadata = TopicMetadata {
            error_code: 0,
            topic_name: "large-topic".to_string(),
            partitions,
        };

        let topic = Topic::from(&metadata);
        assert_eq!(topic.name, "large-topic");
        assert_eq!(topic.partitions, 100);
        assert_eq!(topic.replication_factor, 3);
    }

    #[test]
    fn test_topic_from_metadata_different_replica_counts_per_partition() {
        // In reality, Kafka topics should have consistent replication factors,
        // but we test the behavior: replication_factor is based on the first partition
        let metadata = TopicMetadata {
            error_code: 0,
            topic_name: "inconsistent-topic".to_string(),
            partitions: vec![
                PartitionMetadata {
                    error_code: 0,
                    partition_id: 0,
                    leader: 1,
                    replicas: vec![1, 2],
                    isr: vec![1, 2],
                },
                PartitionMetadata {
                    error_code: 0,
                    partition_id: 1,
                    leader: 2,
                    replicas: vec![2, 3, 4],
                    isr: vec![2, 3, 4],
                },
            ],
        };

        let topic = Topic::from(&metadata);
        assert_eq!(topic.name, "inconsistent-topic");
        assert_eq!(topic.partitions, 2);
        // Should use the first partition's replica count
        assert_eq!(topic.replication_factor, 2);
    }

    #[test]
    fn test_topic_zero_partitions() {
        let topic = Topic {
            name: "zero-partition-topic".to_string(),
            partitions: 0,
            replication_factor: 0,
        };
        assert_eq!(topic.partitions, 0);
        assert_eq!(topic.replication_factor, 0);
    }

    #[test]
    fn test_topic_max_replication_factor() {
        let topic = Topic {
            name: "max-rf-topic".to_string(),
            partitions: 1,
            replication_factor: u16::MAX,
        };
        assert_eq!(topic.replication_factor, u16::MAX);
    }

    #[test]
    fn test_topic_long_name() {
        let long_name = "a".repeat(255);
        let topic = Topic {
            name: long_name.clone(),
            partitions: 1,
            replication_factor: 1,
        };
        assert_eq!(topic.name, long_name);
        assert_eq!(topic.name.len(), 255);
    }
}
