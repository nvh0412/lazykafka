use crate::error::Result;

pub mod api_versions;
pub mod compression;
pub mod create_partitions;
pub mod delete_topics;
pub mod describe_configs;
pub mod describe_groups;
pub mod fetch;
pub mod find_coordinator;
pub mod list_groups;
pub mod list_offset;
pub mod message_buffer;
pub mod message_decoder;
pub mod metadata;
pub mod offset_fetch;
pub mod record;
pub mod record_batch;
pub mod request_header;

pub trait ProtocolRequest {
    fn encode(&self) -> Vec<u8>;
}

pub trait ProtocolResponse<T> {
    fn decode(data: &[u8]) -> Result<T>;
}
