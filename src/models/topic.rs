use crate::protocol::metadata::TopicMetadata;
use crate::protocol::describe_configs::DescribeConfigsResourceResult;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub configs: HashMap<String, ConfigValue>,
}

#[derive(Debug, Clone)]
pub struct ConfigValue {
    pub value: Option<String>,
    pub is_sensitive: bool,
    pub is_read_only: bool,
    pub source: ConfigSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigSource {
    DynamicTopicConfig,
    DynamicBrokerConfig,
    DynamicDefaultBrokerConfig,
    StaticBrokerConfig,
    DefaultConfig,
    Unknown,
}

impl From<i8> for ConfigSource {
    fn from(value: i8) -> Self {
        match value {
            1 => ConfigSource::DynamicTopicConfig,
            2 => ConfigSource::DynamicBrokerConfig,
            3 => ConfigSource::DynamicDefaultBrokerConfig,
            4 => ConfigSource::StaticBrokerConfig,
            5 => ConfigSource::DefaultConfig,
            _ => ConfigSource::Unknown,
        }
    }
}

impl From<&DescribeConfigsResourceResult> for TopicConfig {
    fn from(result: &DescribeConfigsResourceResult) -> Self {
        let mut configs = HashMap::new();

        for entry in &result.configs {
            configs.insert(
                entry.name.clone(),
                ConfigValue {
                    value: entry.value.clone(),
                    is_sensitive: entry.is_sensitive,
                    is_read_only: entry.read_only,
                    source: ConfigSource::from(entry.config_source),
                },
            );
        }

        Self { configs }
    }
}

impl TopicConfig {
    pub fn new() -> Self {
        Self {
            configs: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&ConfigValue> {
        self.configs.get(key)
    }
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub name: String,
    pub partitions: usize,
    pub replication_factor: u16,
    pub config: Option<TopicConfig>,
    // pub metrics: TopicMetrics,
}

impl From<&TopicMetadata> for Topic {
    fn from(value: &TopicMetadata) -> Self {
        Self {
            name: value.topic_name.clone(),
            partitions: value.partitions.len(),
            replication_factor: value
                .partitions
                .first()
                .map(|p| p.replicas.len() as u16)
                .unwrap_or(0),
            config: None,
        }
    }
}
