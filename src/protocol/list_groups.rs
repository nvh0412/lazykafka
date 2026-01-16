use crate::{
    error::Result,
    protocol::{
        ProtocolRequest, ProtocolResponse, message_buffer::MessageBuffer,
        message_decoder::MessageDecoder, request_header::RequestHeader,
    },
};

/// ListGroups Request (API Key 16, Version 2)
/// Lists all consumer groups on the cluster
pub struct ListGroupsRequest {
    pub correlation_id: i32,
}

impl ListGroupsRequest {
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }
}

impl ProtocolRequest for ListGroupsRequest {
    fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(64);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 16,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };

        header.encode(&mut buf);

        // Version 2 has empty body (no states_filter or types_filter)

        buf.write_size_header();
        buf.into_bytes()
    }
}

/// ListGroups Response
pub struct ListGroupsResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub groups: Vec<GroupListing>,
}

/// A consumer group listing
#[derive(Debug, Clone)]
pub struct GroupListing {
    pub group_id: String,
    pub protocol_type: String,
}

impl ProtocolResponse<ListGroupsResponse> for ListGroupsResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;

        let group_count = decoder.read_i32()? as usize;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            groups.push(GroupListing {
                group_id: decoder.read_string()?,
                protocol_type: decoder.read_string()?,
            });
        }

        Ok(Self {
            correlation_id,
            throttle_time_ms,
            error_code,
            groups,
        })
    }
}
