use crate::{
    error::Result,
    protocol::{
        message_buffer::MessageBuffer, message_decoder::MessageDecoder,
        request_header::RequestHeader,
    },
};

pub struct OffsetFetchRequest {
    pub correlation_id: i32,
    pub group_id: String,
    pub topics: Vec<OffsetFetchTopic>,
}

pub struct OffsetFetchTopic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

impl OffsetFetchRequest {
    pub fn new(correlation_id: i32, group_id: String, topics: Vec<OffsetFetchTopic>) -> Self {
        Self {
            correlation_id,
            group_id,
            topics,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(4000);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 9,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };

        header.encode(&mut buf);

        // Group ID
        buf.put_string(&self.group_id);

        // Topics array
        buf.put_i32(self.topics.len() as i32);

        for topic in self.topics.iter() {
            buf.put_string(&topic.topic);

            // Partitions array
            buf.put_i32(topic.partitions.len() as i32);
            for partition in topic.partitions.iter() {
                buf.put_i32(*partition);
            }
        }

        buf.write_size_header();
        buf.into_bytes()
    }
}

pub struct OffsetFetchResponse {
    pub correlation_id: i32,
    pub topics: Vec<OffsetFetchTopicResponse>,
}

pub struct OffsetFetchTopicResponse {
    pub topic: String,
    pub partitions: Vec<OffsetFetchPartitionResponse>,
}

pub struct OffsetFetchPartitionResponse {
    pub partition: i32,
    pub offset: i64,
    pub metadata: String,
    pub error_code: i16,
}

impl OffsetFetchResponse {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;

        let topic_count = decoder.read_i32()?;
        let mut topics = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            let topic_name = decoder.read_string()?;

            let partition_count = decoder.read_i32()?;
            let mut partitions = Vec::with_capacity(partition_count as usize);

            for _ in 0..partition_count {
                let partition = decoder.read_i32()?;
                let offset = decoder.read_i64()?;
                let metadata = decoder.read_string()?;
                let error_code = decoder.read_i16()?;

                partitions.push(OffsetFetchPartitionResponse {
                    partition,
                    offset,
                    metadata,
                    error_code,
                });
            }

            topics.push(OffsetFetchTopicResponse {
                topic: topic_name,
                partitions,
            });
        }

        Ok(Self {
            correlation_id,
            topics,
        })
    }
}
