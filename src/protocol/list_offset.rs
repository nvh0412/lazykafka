use crate::{
    error::Result,
    protocol::{
        ProtocolRequest, ProtocolResponse, message_buffer::MessageBuffer,
        message_decoder::MessageDecoder, request_header::RequestHeader,
    },
};

pub struct ListOffsetRequest {
    pub correlation_id: i32,
    pub replica_id: i32,
    pub topics: Vec<ListOffsetTopic>,
}

pub struct ListOffsetTopic {
    pub name: String,
    pub partitions: Vec<ListOffsetPartition>,
}

pub struct ListOffsetPartition {
    pub partition_index: i32,
    pub timestamp: i64,
}

impl ListOffsetRequest {
    pub fn new(correlation_id: i32, replica_id: i32, topics: Vec<ListOffsetTopic>) -> Self {
        Self {
            correlation_id,
            replica_id,
            topics,
        }
    }
}

impl ProtocolRequest for ListOffsetRequest {
    fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(4000);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 2,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };

        header.encode(&mut buf);

        buf.put_i32(self.replica_id);

        buf.put_i32(self.topics.len() as i32);

        for topic in self.topics.iter() {
            buf.put_string(topic.name.as_str());
            buf.put_i32(topic.partitions.len() as i32);

            for p in topic.partitions.iter() {
                buf.put_i32(p.partition_index);
                buf.put_i64(p.timestamp);
            }
        }

        buf.write_size_header();
        buf.into_bytes()
    }
}

pub struct ListOffsetResponse {
    pub correlation_id: i32,
    pub topics: Vec<ListOffsetTopicResponse>,
}

pub struct ListOffsetTopicResponse {
    pub name: String,
    pub partitions: Vec<ListOffsetPartitionResponse>,
}

pub struct ListOffsetPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
}

impl ProtocolResponse<ListOffsetResponse> for ListOffsetResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;
        let topic_count = decoder.read_i32()?;
        let mut topics = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            let name = decoder.read_string()?;

            let partition_count = decoder.read_i32()?;
            let mut partitions = Vec::with_capacity(partition_count as usize);

            for _ in 0..partition_count {
                partitions.push(ListOffsetPartitionResponse {
                    partition_index: decoder.read_i32()?,
                    error_code: decoder.read_i16()?,
                    timestamp: decoder.read_i64()?,
                    offset: decoder.read_i64()?,
                })
            }

            topics.push(ListOffsetTopicResponse { name, partitions });
        }

        Ok(ListOffsetResponse {
            correlation_id,
            topics,
        })
    }
}
