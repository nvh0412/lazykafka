use crate::{
    error::Result,
    protocol::{
        message_buffer::MessageBuffer, message_decoder::MessageDecoder,
        request_header::RequestHeader,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceType {
    Topic = 2,
    Broker = 4,
}

impl ResourceType {
    pub fn from_i8(value: i8) -> Option<Self> {
        match value {
            2 => Some(ResourceType::Topic),
            4 => Some(ResourceType::Broker),
            _ => None,
        }
    }

    pub fn as_i8(&self) -> i8 {
        *self as i8
    }
}

pub struct DescribeConfigsResource {
    pub resource_type: ResourceType,
    pub resource_name: String,
    pub config_names: Option<Vec<String>>,
}

#[allow(dead_code)]
impl DescribeConfigsResource {
    pub fn new(resource_type: ResourceType, resource_name: String) -> Self {
        Self {
            resource_type,
            resource_name,
            config_names: None,
        }
    }

    pub fn with_config_names(
        resource_type: ResourceType,
        resource_name: String,
        config_names: Vec<String>,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            config_names: Some(config_names),
        }
    }
}

#[allow(dead_code)]
pub struct DescribeConfigsRequest {
    pub correlation_id: i32,
    pub resources: Vec<DescribeConfigsResource>,
    pub include_synonyms: bool,
    pub include_documentation: bool,
}

#[allow(dead_code)]
impl DescribeConfigsRequest {
    pub fn new(correlation_id: i32, resources: Vec<DescribeConfigsResource>) -> Self {
        Self {
            correlation_id,
            resources,
            include_synonyms: false,
            include_documentation: false,
        }
    }

    pub fn with_options(
        correlation_id: i32,
        resources: Vec<DescribeConfigsResource>,
        include_synonyms: bool,
        include_documentation: bool,
    ) -> Self {
        Self {
            correlation_id,
            resources,
            include_synonyms,
            include_documentation,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(4000);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 32,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };

        header.encode(&mut buf);

        // Version 1 uses standard ARRAY encoding
        buf.put_i32(self.resources.len() as i32);

        for resource in &self.resources {
            buf.put_i8(resource.resource_type.as_i8());
            buf.put_string(&resource.resource_name);

            match &resource.config_names {
                Some(names) => {
                    buf.put_i32(names.len() as i32);
                    for name in names {
                        buf.put_string(name);
                    }
                }
                None => {
                    buf.put_i32(-1);
                }
            }
        }

        buf.put_bool(self.include_synonyms);

        buf.write_size_header();
        buf.into_bytes()
    }
}

#[allow(dead_code)]
pub struct DescribeConfigsResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub resources: Vec<DescribeConfigsResourceResult>,
}

#[allow(dead_code)]
pub struct DescribeConfigsResourceResult {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub resource_type: ResourceType,
    pub resource_name: String,
    pub configs: Vec<ConfigEntry>,
}

#[allow(dead_code)]
pub struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    pub config_source: i8,
    pub is_sensitive: bool,
    pub synonyms: Vec<ConfigSynonym>,
    pub config_type: i8,
    pub documentation: Option<String>,
}

#[allow(dead_code)]
pub struct ConfigSynonym {
    pub name: String,
    pub value: Option<String>,
    pub source: i8,
}

impl DescribeConfigsResponse {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;

        // Version 1 has throttle_time_ms
        let throttle_time_ms = decoder.read_i32()?;

        // Version 1 uses standard ARRAY encoding
        let resource_count = decoder.read_i32()? as usize;
        let mut resources = Vec::with_capacity(resource_count);

        for _ in 0..resource_count {
            let error_code = decoder.read_i16()?;
            let error_message = decoder.read_nullable_string()?;
            let resource_type_value = decoder.read_i8()?;
            let resource_type =
                ResourceType::from_i8(resource_type_value).unwrap_or(ResourceType::Topic);
            let resource_name = decoder.read_string()?;

            let config_count = decoder.read_i32()? as usize;
            let mut configs = Vec::with_capacity(config_count);

            for _ in 0..config_count {
                let name = decoder.read_string()?;
                let value = decoder.read_nullable_string()?;
                let read_only = decoder.read_bool()?;
                let config_source = decoder.read_i8()?;
                let is_sensitive = decoder.read_bool()?;

                let synonym_count = decoder.read_i32()? as usize;
                let mut synonyms = Vec::with_capacity(synonym_count);

                for _ in 0..synonym_count {
                    let synonym_name = decoder.read_string()?;
                    let synonym_value = decoder.read_nullable_string()?;
                    let synonym_source = decoder.read_i8()?;

                    synonyms.push(ConfigSynonym {
                        name: synonym_name,
                        value: synonym_value,
                        source: synonym_source,
                    });
                }

                let config_type = 0;
                let documentation = None;

                configs.push(ConfigEntry {
                    name,
                    value,
                    read_only,
                    config_source,
                    is_sensitive,
                    synonyms,
                    config_type,
                    documentation,
                });
            }

            resources.push(DescribeConfigsResourceResult {
                error_code,
                error_message,
                resource_type,
                resource_name,
                configs,
            });
        }

        Ok(Self {
            correlation_id,
            throttle_time_ms,
            resources,
        })
    }
}
