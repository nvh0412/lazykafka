use crate::{
    error::Result,
    protocol::{
        message_buffer::MessageBuffer, message_decoder::MessageDecoder,
        request_header::RequestHeader,
    },
};

pub struct MetadataRequest {
    pub correlation_id: i32,
    pub topics: Option<Vec<String>>,
}

impl MetadataRequest {
    pub fn new(correlation_id: i32, topics: Option<Vec<String>>) -> Self {
        Self {
            correlation_id,
            topics,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(4000);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 3,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };

        header.encode(&mut buf);

        match &self.topics {
            Some(topics) => {
                buf.put_i32(topics.len() as i32);
                for topic in topics {
                    buf.put_string(topic);
                }
            }
            None => {
                // In version 0, an empty array indicates "request metadata for all topics."
                // In version 1 and higher, an empty array indicates "request metadata for no topics,"
                // and a null array is used to indicate "request metadata for all topics."
                // https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/MetadataRequest.json
                buf.put_i32(0);
            }
        }

        buf.write_size_header();
        buf.into_bytes()
    }
}

pub struct MetadataResponse {
    pub correlation_id: i32,
    pub brokers: Vec<Broker>,
    pub topics: Vec<TopicMetadata>,
}

pub struct Broker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

pub struct TopicMetadata {
    pub error_code: i16,
    pub topic_name: String,
    pub partitions: Vec<PartitionMetadata>,
}

pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

impl MetadataResponse {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;

        let broker_count = decoder.read_i32()? as usize;
        let mut brokers = Vec::with_capacity(broker_count);

        for _ in 0..broker_count {
            brokers.push(Broker {
                node_id: decoder.read_i32()?,
                host: decoder.read_string()?,
                port: decoder.read_i32()?,
            });
        }

        // Read topics
        let topic_count = decoder.read_i32()?;
        println!("[DEBUG] having {} topics", topic_count);
        let mut topics = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            let error_code = decoder.read_i16()?;
            let topic_name = decoder.read_string()?;

            let partition_count = decoder.read_i32()? as usize;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let error_code = decoder.read_i16()?;
                let partition_id = decoder.read_i32()?;
                let leader = decoder.read_i32()?;

                let replica_count = decoder.read_i32()? as usize;
                let mut replicas = Vec::with_capacity(replica_count);

                for _ in 0..replica_count {
                    replicas.push(decoder.read_i32()?);
                }

                let isr_count = decoder.read_i32()? as usize;
                let mut isr = Vec::with_capacity(isr_count);

                for _ in 0..isr_count {
                    isr.push(decoder.read_i32()?);
                }

                partitions.push(PartitionMetadata {
                    error_code,
                    partition_id,
                    leader,
                    replicas,
                    isr,
                });
            }

            topics.push(TopicMetadata {
                error_code,
                topic_name,
                partitions,
            });
        }

        Ok(Self {
            correlation_id,
            brokers,
            topics,
        })
    }
}
