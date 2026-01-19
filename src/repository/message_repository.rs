use crate::client::KafkaClient;
use crate::error;
use crate::models::topic::Topic;
use crate::protocol::fetch::{FetchPartition, FetchTopic};
use crate::protocol::list_offset::{
    ListOffsetPartition, ListOffsetPartitionResponse, ListOffsetTopic,
};
use crate::protocol::record::Record;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Special timestamp value to get the earliest available offset
pub const OFFSET_EARLIEST: i64 = -2;
/// Special timestamp value to get the latest (end) offset
pub const OFFSET_LATEST: i64 = -1;

/// Configuration for paginated record fetching
pub const DEFAULT_PAGE_SIZE: usize = 100;
pub const MAX_PAGE_SIZE: usize = 1000;
pub const PARTITION_MAX_BYTES: i32 = 1_048_576; // 1MB per partition

/// Options for fetching records with pagination
#[derive(Clone, Debug)]
pub struct RecordFetchOptions {
    /// Number of records per page
    pub page_size: usize,
    /// Starting offset for each partition (None = use latest offsets)
    pub start_offsets: Option<HashMap<i32, i64>>,
    /// Filter to specific partitions (None = all partitions)
    pub partition_filter: Option<Vec<i32>>,
    /// Whether to fetch from newest first (true) or oldest first (false)
    pub newest_first: bool,
}

impl Default for RecordFetchOptions {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            start_offsets: None,
            partition_filter: None,
            newest_first: true,
        }
    }
}

/// Result of a paginated fetch operation
#[derive(Clone, Debug)]
pub struct RecordFetchResult {
    /// The fetched records for this page
    pub records: Vec<Record>,
    /// Starting offsets used for this fetch (per partition)
    pub start_offsets: HashMap<i32, i64>,
    /// Ending offsets after this fetch (for next page navigation)
    pub end_offsets: HashMap<i32, i64>,
    /// Earliest available offsets per partition
    pub earliest_offsets: HashMap<i32, i64>,
    /// Latest available offsets per partition
    pub latest_offsets: HashMap<i32, i64>,
    /// Total estimated record count (sum of latest - earliest for all partitions)
    pub total_estimated: usize,
    /// Whether there are more records available (has next page)
    pub has_more: bool,
    /// Whether there are previous records available (has previous page)
    pub has_previous: bool,
}

#[derive(Clone)]
pub struct MessageRepository {
    kafka_client: Arc<RwLock<KafkaClient>>,
}

impl MessageRepository {
    pub fn new(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        Self { kafka_client }
    }

    pub async fn fetch_by_topic(&self, topic: Topic) -> error::Result<Vec<Record>> {
        let mut client = self.kafka_client.write().await;
        let partitions: Vec<ListOffsetPartition> = topic
            .partitions
            .iter()
            .map(|p| ListOffsetPartition {
                partition_index: p.partition,
                timestamp: 0,
            })
            .collect();

        let offset_topics = vec![ListOffsetTopic {
            name: topic.name.clone(),
            partitions,
        }];

        let mut topic_map: HashMap<String, Vec<ListOffsetPartitionResponse>> = HashMap::new();

        let results = client.list_offsets(offset_topics).await?;

        for t in results.topics {
            topic_map.insert(t.name, t.partitions);
        }

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
                        let mut record = r.clone();
                        record.partition = Some(p.partition_index);
                        records.push(record);
                    }
                }
            }
        }

        Ok(records)
    }

    /// Fetch records from a topic with pagination support
    pub async fn fetch_by_topic_paginated(
        &self,
        topic: Topic,
        options: RecordFetchOptions,
    ) -> error::Result<RecordFetchResult> {
        let mut client = self.kafka_client.write().await;

        // Determine which partitions to fetch
        let partitions_to_fetch: Vec<_> = if let Some(filter) = &options.partition_filter {
            topic
                .partitions
                .iter()
                .filter(|p| filter.contains(&p.partition))
                .collect()
        } else {
            topic.partitions.iter().collect()
        };

        if partitions_to_fetch.is_empty() {
            return Ok(RecordFetchResult {
                records: vec![],
                start_offsets: HashMap::new(),
                end_offsets: HashMap::new(),
                earliest_offsets: HashMap::new(),
                latest_offsets: HashMap::new(),
                total_estimated: 0,
                has_more: false,
                has_previous: false,
            });
        }

        // Get earliest offsets
        let earliest_offset_topics = vec![ListOffsetTopic {
            name: topic.name.clone(),
            partitions: partitions_to_fetch
                .iter()
                .map(|p| ListOffsetPartition {
                    partition_index: p.partition,
                    timestamp: OFFSET_EARLIEST,
                })
                .collect(),
        }];

        let earliest_results = client.list_offsets(earliest_offset_topics).await?;
        let mut earliest_offsets: HashMap<i32, i64> = HashMap::new();
        for t in &earliest_results.topics {
            for p in &t.partitions {
                earliest_offsets.insert(p.partition_index, p.offset);
            }
        }

        // Get latest offsets
        let latest_offset_topics = vec![ListOffsetTopic {
            name: topic.name.clone(),
            partitions: partitions_to_fetch
                .iter()
                .map(|p| ListOffsetPartition {
                    partition_index: p.partition,
                    timestamp: OFFSET_LATEST,
                })
                .collect(),
        }];

        let latest_results = client.list_offsets(latest_offset_topics).await?;
        let mut latest_offsets: HashMap<i32, i64> = HashMap::new();
        for t in &latest_results.topics {
            for p in &t.partitions {
                latest_offsets.insert(p.partition_index, p.offset);
            }
        }

        // Calculate total estimated records
        let total_estimated: usize = partitions_to_fetch
            .iter()
            .map(|p| {
                let earliest = earliest_offsets.get(&p.partition).copied().unwrap_or(0);
                let latest = latest_offsets.get(&p.partition).copied().unwrap_or(0);
                (latest - earliest).max(0) as usize
            })
            .sum();

        // Determine starting offsets for this fetch
        let page_size = options.page_size.min(MAX_PAGE_SIZE);
        let mut start_offsets: HashMap<i32, i64> = HashMap::new();

        if let Some(provided_offsets) = &options.start_offsets {
            // Use provided offsets
            for p in &partitions_to_fetch {
                if let Some(&offset) = provided_offsets.get(&p.partition) {
                    start_offsets.insert(p.partition, offset);
                } else if options.newest_first {
                    // Start from latest - page_size for newest first
                    let latest = latest_offsets.get(&p.partition).copied().unwrap_or(0);
                    let earliest = earliest_offsets.get(&p.partition).copied().unwrap_or(0);
                    let start = (latest - page_size as i64).max(earliest);
                    start_offsets.insert(p.partition, start);
                } else {
                    // Start from earliest for oldest first
                    let earliest = earliest_offsets.get(&p.partition).copied().unwrap_or(0);
                    start_offsets.insert(p.partition, earliest);
                }
            }
        } else if options.newest_first {
            // Default: start from latest - page_size per partition
            for p in &partitions_to_fetch {
                let latest = latest_offsets.get(&p.partition).copied().unwrap_or(0);
                let earliest = earliest_offsets.get(&p.partition).copied().unwrap_or(0);
                let start = (latest - page_size as i64).max(earliest);
                start_offsets.insert(p.partition, start);
            }
        } else {
            // Start from earliest
            for p in &partitions_to_fetch {
                let earliest = earliest_offsets.get(&p.partition).copied().unwrap_or(0);
                start_offsets.insert(p.partition, earliest);
            }
        }

        // Build fetch request
        let mut fetch_topic = FetchTopic::new(topic.name.clone());
        for p in &partitions_to_fetch {
            let start_offset = start_offsets.get(&p.partition).copied().unwrap_or(0);
            fetch_topic.add_partition(FetchPartition {
                partition: p.partition,
                fetch_offset: start_offset,
                partition_max_bytes: PARTITION_MAX_BYTES,
            });
        }

        // Fetch records
        let mut records = Vec::new();
        let mut end_offsets: HashMap<i32, i64> = HashMap::new();
        let response = client.fetch_messages(vec![fetch_topic]).await?;

        for topic_r in response.responses {
            for p in topic_r.paritions {
                let partition_idx = p.partition_index;
                let mut partition_records: Vec<Record> = Vec::new();

                for batch in p.records {
                    for r in batch.records {
                        let mut record = r.clone();
                        record.partition = Some(partition_idx);
                        partition_records.push(record);
                    }
                }

                // Track the end offset for this partition
                if let Some(last_record) = partition_records.last() {
                    let start = start_offsets.get(&partition_idx).copied().unwrap_or(0);
                    end_offsets.insert(partition_idx, start + last_record.offset_delta + 1);
                } else {
                    end_offsets.insert(
                        partition_idx,
                        start_offsets.get(&partition_idx).copied().unwrap_or(0),
                    );
                }

                records.extend(partition_records);
            }
        }

        // Limit to page_size
        if records.len() > page_size {
            records.truncate(page_size);
        }

        // Determine if there are more/previous records
        let has_more = partitions_to_fetch.iter().any(|p| {
            let end = end_offsets.get(&p.partition).copied().unwrap_or(0);
            let latest = latest_offsets.get(&p.partition).copied().unwrap_or(0);
            end < latest
        });

        let has_previous = partitions_to_fetch.iter().any(|p| {
            let start = start_offsets.get(&p.partition).copied().unwrap_or(0);
            let earliest = earliest_offsets.get(&p.partition).copied().unwrap_or(0);
            start > earliest
        });

        Ok(RecordFetchResult {
            records,
            start_offsets,
            end_offsets,
            earliest_offsets,
            latest_offsets,
            total_estimated,
            has_more,
            has_previous,
        })
    }
}
