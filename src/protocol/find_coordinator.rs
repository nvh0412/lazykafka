use crate::{
    error::Result,
    protocol::{
        ProtocolRequest, ProtocolResponse, message_buffer::MessageBuffer,
        message_decoder::MessageDecoder, request_header::RequestHeader,
    },
};

/// FindCoordinator Request (API Key 10, Version 2)
/// Finds the coordinator broker for a consumer group
pub struct FindCoordinatorRequest {
    pub correlation_id: i32,
    pub key: String,  // The group_id to find coordinator for
    pub key_type: i8, // 0 = group, 1 = transaction
}

impl FindCoordinatorRequest {
    pub fn new(correlation_id: i32, key: String, key_type: i8) -> Self {
        Self {
            correlation_id,
            key,
            key_type,
        }
    }

    /// Create a request to find the coordinator for a consumer group
    pub fn for_group(correlation_id: i32, group_id: String) -> Self {
        Self::new(correlation_id, group_id, 0)
    }
}

impl ProtocolRequest for FindCoordinatorRequest {
    fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(256);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 10,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };

        header.encode(&mut buf);

        // Version 2: key, key_type
        buf.put_string(&self.key);
        buf.put_i8(self.key_type);

        buf.write_size_header();
        buf.into_bytes()
    }
}

/// FindCoordinator Response
#[allow(dead_code)]
pub struct FindCoordinatorResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl ProtocolResponse<FindCoordinatorResponse> for FindCoordinatorResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_nullable_string()?;
        let node_id = decoder.read_i32()?;
        let host = decoder.read_string()?;
        let port = decoder.read_i32()?;

        Ok(Self {
            correlation_id,
            throttle_time_ms,
            error_code,
            error_message,
            node_id,
            host,
            port,
        })
    }
}
