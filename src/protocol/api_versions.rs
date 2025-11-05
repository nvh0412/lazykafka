use crate::{
    error::Result,
    protocol::{
        message_buffer::MessageBuffer, message_decoder::MessageDecoder,
        request_header::RequestHeader,
    },
};

pub struct ApiVersionRequest {
    pub correlation_id: i32,
}

impl ApiVersionRequest {
    // TODO: allow client to pass client_id
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(64);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 18,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };
        header.encode(&mut buf);

        buf.write_size_header();
        buf.into_bytes()
    }
}

#[derive(Debug)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug)]
pub struct ApiVersionsResponse {
    pub correlation_id: i32,
    pub error_code: i16,
    pub api_versions: Vec<ApiVersion>,
}

impl ApiVersionsResponse {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;

        let array_len = decoder.read_i32()? as usize;
        let mut api_versions = Vec::with_capacity(array_len);

        for _ in 0..array_len {
            api_versions.push(ApiVersion {
                api_key: decoder.read_i16()?,
                min_version: decoder.read_i16()?,
                max_version: decoder.read_i16()?,
            });
        }

        Ok(Self {
            correlation_id,
            error_code,
            api_versions,
        })
    }
}
