use crate::{
    error::Result,
    protocol::{
        message_buffer::MessageBuffer, message_decoder::MessageDecoder,
        request_header::RequestHeader,
    },
};

pub struct DeleteTopicsRequest {
    pub correlation_id: i32,
    pub topics: Vec<String>,
    pub timeout_ms: i32,
}

impl DeleteTopicsRequest {
    pub fn new(correlation_id: i32, topics: Vec<String>, timeout_ms: i32) -> Self {
        Self {
            correlation_id,
            topics,
            timeout_ms,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(4000);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 20,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };

        header.encode(&mut buf);

        // Encode topics array
        buf.put_i32(self.topics.len() as i32);
        for topic in &self.topics {
            buf.put_string(topic);
        }

        // Encode timeout
        buf.put_i32(self.timeout_ms);

        buf.write_size_header();
        buf.into_bytes()
    }
}

pub struct DeleteTopicsResponse {
    pub correlation_id: i32,
    pub topic_error_codes: Vec<TopicErrorCode>,
}

pub struct TopicErrorCode {
    pub topic: String,
    pub error_code: i16,
}

impl DeleteTopicsResponse {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;

        // Read topic error codes array
        let topic_count = decoder.read_i32()? as usize;
        let mut topic_error_codes = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = decoder.read_string()?;
            let error_code = decoder.read_i16()?;

            topic_error_codes.push(TopicErrorCode { topic, error_code });
        }

        Ok(Self {
            correlation_id,
            topic_error_codes,
        })
    }
}
