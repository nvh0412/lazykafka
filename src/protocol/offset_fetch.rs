use crate::{
    error::Result,
    protocol::{
        ProtocolRequest, ProtocolResponse, message_buffer::MessageBuffer,
        message_decoder::MessageDecoder, request_header::RequestHeader,
    },
};

/// OffsetFetch Request (API Key 9, Version 3)
/// Fetches committed offsets for a consumer group
pub struct OffsetFetchRequest {
    pub correlation_id: i32,
    pub group_id: String,
    pub topics: Option<Vec<OffsetFetchTopic>>,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

impl OffsetFetchRequest {
    pub fn new(correlation_id: i32, group_id: String, topics: Option<Vec<OffsetFetchTopic>>) -> Self {
        Self {
            correlation_id,
            group_id,
            topics,
        }
    }

    /// Create a request to fetch all topic offsets for a group
    pub fn all_topics(correlation_id: i32, group_id: String) -> Self {
        Self::new(correlation_id, group_id, None)
    }
}

impl ProtocolRequest for OffsetFetchRequest {
    fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(256);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 9,
            api_version: 3,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };

        header.encode(&mut buf);

        // Group ID
        buf.put_string(&self.group_id);

        // Topics array (nullable in version 2+)
        match &self.topics {
            Some(topics) => {
                buf.put_i32(topics.len() as i32);
                for topic in topics {
                    buf.put_string(&topic.name);
                    buf.put_i32(topic.partitions.len() as i32);
                    for partition in &topic.partitions {
                        buf.put_i32(*partition);
                    }
                }
            }
            None => {
                // Null array = fetch all topics
                buf.put_i32(-1);
            }
        }

        buf.write_size_header();
        buf.into_bytes()
    }
}

/// OffsetFetch Response
pub struct OffsetFetchResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetFetchTopicResponse>,
    pub error_code: i16,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchTopicResponse {
    pub name: String,
    pub partitions: Vec<OffsetFetchPartitionResponse>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OffsetFetchPartitionResponse {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub metadata: Option<String>,
    pub error_code: i16,
}

impl ProtocolResponse<OffsetFetchResponse> for OffsetFetchResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;
        let throttle_time_ms = decoder.read_i32()?;

        let topic_count = decoder.read_i32()? as usize;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = decoder.read_string()?;

            let partition_count = decoder.read_i32()? as usize;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                partitions.push(OffsetFetchPartitionResponse {
                    partition_index: decoder.read_i32()?,
                    committed_offset: decoder.read_i64()?,
                    metadata: decoder.read_nullable_string()?,
                    error_code: decoder.read_i16()?,
                });
            }

            topics.push(OffsetFetchTopicResponse { name, partitions });
        }

        let error_code = decoder.read_i16()?;

        Ok(Self {
            correlation_id,
            throttle_time_ms,
            topics,
            error_code,
        })
    }
}
