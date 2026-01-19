use crate::{
    error::Result,
    protocol::{
        ProtocolRequest, ProtocolResponse, message_buffer::MessageBuffer,
        message_decoder::MessageDecoder, request_header::RequestHeader,
    },
};

/// DescribeGroups Request (API Key 15, Version 3)
/// Gets detailed information about consumer groups
pub struct DescribeGroupsRequest {
    pub correlation_id: i32,
    pub groups: Vec<String>,
}

impl DescribeGroupsRequest {
    pub fn new(correlation_id: i32, groups: Vec<String>) -> Self {
        Self {
            correlation_id,
            groups,
        }
    }
}

impl ProtocolRequest for DescribeGroupsRequest {
    fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(256);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 15,
            api_version: 3,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };

        header.encode(&mut buf);

        // Groups array
        buf.put_i32(self.groups.len() as i32);
        for group_id in &self.groups {
            buf.put_string(group_id);
        }

        // Version 3: include_authorized_operations (false)
        buf.put_bool(false);

        buf.write_size_header();
        buf.into_bytes()
    }
}

/// DescribeGroups Response
pub struct DescribeGroupsResponse {
    pub correlation_id: i32,
    pub groups: Vec<DescribedGroup>,
}

/// Detailed information about a consumer group
#[derive(Debug, Clone)]
pub struct DescribedGroup {
    pub group_id: String,
    pub group_state: String,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<GroupMember>,
}

/// A member of a consumer group
#[derive(Debug, Clone)]
pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub member_assignment: Vec<u8>,
}

impl ProtocolResponse<DescribeGroupsResponse> for DescribeGroupsResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;
        let _throttle_time_ms = decoder.read_i32()?;

        let group_count = decoder.read_i32()? as usize;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let _error_code = decoder.read_i16()?;
            let group_id = decoder.read_string()?;
            let group_state = decoder.read_string()?;
            let protocol_type = decoder.read_string()?;
            let protocol_data = decoder.read_string()?;

            let member_count = decoder.read_i32()? as usize;
            let mut members = Vec::with_capacity(member_count);

            for _ in 0..member_count {
                let member_id = decoder.read_string()?;
                let client_id = decoder.read_string()?;
                let client_host = decoder.read_string()?;
                let _member_metadata = decoder.read_bytes()?.unwrap_or_default();
                let member_assignment = decoder.read_bytes()?.unwrap_or_default();

                members.push(GroupMember {
                    member_id,
                    client_id,
                    client_host,
                    member_assignment,
                });
            }

            let _authorized_operations = decoder.read_i32()?;

            groups.push(DescribedGroup {
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
            });
        }

        Ok(Self {
            correlation_id,
            groups,
        })
    }
}
