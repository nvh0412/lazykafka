use crate::error::Result;

pub mod api_versions;
pub mod fetch;
pub mod list_offset;
pub mod message_buffer;
pub mod message_decoder;
pub mod metadata;
pub mod offset_fetch;
pub mod request_header;

pub trait ProtocolRequest {
    fn encode(&self) -> Vec<u8>;
}

pub trait ProtocolResponse<T> {
    fn decode(data: &[u8]) -> Result<T>;
}
