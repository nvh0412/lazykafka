use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::client::KafkaClient;
use crate::error::Result;
use crate::models::consumer_group::{
    ConsumerGroupDetail, ConsumerGroupMember, ConsumerGroupState, ConsumerGroupSummary,
    CoordinatorInfo, PartitionAssignment, PartitionOffsetInfo, TopicOffsetInfo,
};
use crate::protocol::list_offset::{ListOffsetPartition, ListOffsetTopic};
use crate::protocol::message_decoder::MessageDecoder;

#[derive(Clone)]
pub struct ConsumerGroupRepository {
    kafka_client: Arc<RwLock<KafkaClient>>,
}

impl ConsumerGroupRepository {
    pub fn new(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        Self { kafka_client }
    }

    /// Fetch all consumer groups (filtered, excludes internal groups starting with '_')
    pub async fn fetch_all(&self) -> Result<Vec<ConsumerGroupSummary>> {
        let mut client = self.kafka_client.write().await;

        // 1. List all groups
        let list_response = client.list_groups().await?;
        if list_response.error_code != 0 {
            return Ok(vec![]);
        }

        // 2. Filter out internal groups (starting with '_')
        let group_ids: Vec<String> = list_response
            .groups
            .iter()
            .filter(|g| !g.group_id.starts_with('_'))
            .map(|g| g.group_id.clone())
            .collect();

        if group_ids.is_empty() {
            return Ok(vec![]);
        }

        // 3. Describe groups for state & member count
        let describe_response = client.describe_groups(group_ids).await?;

        let summaries: Vec<ConsumerGroupSummary> = describe_response
            .groups
            .iter()
            .map(|g| ConsumerGroupSummary {
                group_id: g.group_id.clone(),
            })
            .collect();

        Ok(summaries)
    }

    /// Fetch detailed information for a single consumer group
    pub async fn fetch_detail(&self, group_id: String) -> Result<ConsumerGroupDetail> {
        let mut client = self.kafka_client.write().await;

        // 1. Find coordinator
        let coordinator_response = client.find_coordinator(group_id.clone()).await?;
        let coordinator = if coordinator_response.error_code == 0 {
            Some(CoordinatorInfo {
                node_id: coordinator_response.node_id,
                host: coordinator_response.host,
                port: coordinator_response.port,
            })
        } else {
            None
        };

        // 2. Describe group
        let describe_response = client.describe_groups(vec![group_id.clone()]).await?;
        let described_group = describe_response.groups.into_iter().next().ok_or_else(|| {
            crate::error::KafkaError::ProtocolError("Group not found".to_string())
        })?;

        // 3. Parse members and assignments
        let members: Vec<ConsumerGroupMember> = described_group
            .members
            .iter()
            .map(|m| {
                let assignments = Self::parse_member_assignment(&m.member_assignment);
                ConsumerGroupMember {
                    member_id: m.member_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    assignments,
                }
            })
            .collect();

        // 4. Fetch committed offsets
        let offset_response = client.fetch_offsets(group_id.clone(), None).await?;

        // 5. Fetch end offsets for lag calculation
        let mut topic_offsets: Vec<TopicOffsetInfo> = Vec::new();

        for offset_topic in &offset_response.topics {
            let partitions: Vec<i32> = offset_topic
                .partitions
                .iter()
                .map(|p| p.partition_index)
                .collect();

            // Get latest offsets
            let list_offset_topic = ListOffsetTopic {
                name: offset_topic.name.clone(),
                partitions: partitions
                    .iter()
                    .map(|&p| ListOffsetPartition {
                        partition_index: p,
                        timestamp: -1, // -1 = latest
                    })
                    .collect(),
            };

            let end_offset_response = client.list_offsets(vec![list_offset_topic]).await?;

            let end_offsets: HashMap<i32, i64> = end_offset_response
                .topics
                .iter()
                .flat_map(|t| t.partitions.iter())
                .map(|p| (p.partition_index, p.offset))
                .collect();

            let partition_infos: Vec<PartitionOffsetInfo> = offset_topic
                .partitions
                .iter()
                .map(|p| {
                    let end_offset = end_offsets.get(&p.partition_index).copied().unwrap_or(0);
                    PartitionOffsetInfo::new(p.partition_index, p.committed_offset, end_offset)
                })
                .collect();

            topic_offsets.push(TopicOffsetInfo {
                topic: offset_topic.name.clone(),
                partitions: partition_infos,
            });
        }

        Ok(ConsumerGroupDetail {
            group_id,
            state: ConsumerGroupState::from(described_group.group_state.as_str()),
            protocol_type: described_group.protocol_type,
            protocol: described_group.protocol_data,
            coordinator,
            members,
            topic_offsets,
        })
    }

    /// Parse member_assignment bytes (ConsumerProtocolAssignment format)
    fn parse_member_assignment(data: &[u8]) -> Vec<PartitionAssignment> {
        if data.is_empty() {
            return vec![];
        }

        let mut decoder = MessageDecoder::new(data);

        // Version
        let _version = match decoder.read_i16() {
            Ok(v) => v,
            Err(_) => return vec![],
        };

        // Topic count
        let topic_count = match decoder.read_i32() {
            Ok(c) => c,
            Err(_) => return vec![],
        };

        let mut assignments = Vec::new();
        for _ in 0..topic_count {
            let topic = match decoder.read_string() {
                Ok(t) => t,
                Err(_) => continue,
            };

            let partition_count = match decoder.read_i32() {
                Ok(c) => c,
                Err(_) => continue,
            };

            let mut partitions = Vec::new();
            for _ in 0..partition_count {
                if let Ok(p) = decoder.read_i32() {
                    partitions.push(p);
                }
            }

            assignments.push(PartitionAssignment { topic, partitions });
        }

        assignments
    }
}
