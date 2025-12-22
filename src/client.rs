use std::time::Duration;

use crate::models::broker_stats::{BrokerStats, ConnectionStatus};

use crate::protocol::describe_configs::{
    DescribeConfigsRequest, DescribeConfigsResource, DescribeConfigsResponse, ResourceType,
};
use crate::protocol::fetch::{FetchRequest, FetchResponse, FetchTopic};
use crate::protocol::list_offset::{ListOffsetRequest, ListOffsetResponse, ListOffsetTopic};
use crate::protocol::offset_fetch::{OffsetFetchRequest, OffsetFetchResponse, OffsetFetchTopic};
use crate::protocol::{ProtocolRequest, ProtocolResponse};
use crate::{
    connection::KafkaConnection,
    error::{KafkaError, Result},
    protocol::{
        api_versions::{ApiVersionRequest, ApiVersionsResponse},
        metadata::{MetadataRequest, MetadataResponse},
    },
};

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub client_id: String,
    pub request_timeout: Duration,
    pub connection_timeout: Duration,
    pub retry_count: u32,
    pub retry_backoff: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            client_id: "kafkatui-rs-client".to_string(),
            request_timeout: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(10),
            retry_count: 3,
            retry_backoff: Duration::from_millis(100),
        }
    }
}

pub struct KafkaClient {
    pub connection: KafkaConnection,
    pub config: ClientConfig,
    pub api_versions: Option<ApiVersionsResponse>,
}

impl KafkaClient {
    pub fn get_stats(&self) -> BrokerStats {
        BrokerStats {
            broker: "localhost:9092".to_string(),
            status: if self.connection.is_connected() {
                ConnectionStatus::Connected
            } else {
                ConnectionStatus::Disconnected
            },
            bytes_sent: self.connection.bytes_sent(),
            bytes_received: self.connection.bytes_received(),
        }
    }

    /// Get API versions supported by broker
    pub async fn get_api_versions(&mut self) -> Result<ApiVersionsResponse> {
        let correlation_id = self.connection.next_correlation_id();
        let request = ApiVersionRequest::new(correlation_id);

        self.connection.send_request(&request.encode()).await?;

        let response_bytes =
            tokio::time::timeout(self.config.request_timeout, self.connection.read_response())
                .await
                .map_err(|_| KafkaError::ProtocolError("Request timeout".to_string()))??;

        let response = ApiVersionsResponse::decode(&response_bytes)?;

        if response.correlation_id != correlation_id {
            return Err(KafkaError::InvalidResponse {
                expected: format!("correlation_id {}", correlation_id),
                actual: format!("correlation_id: {}", response.correlation_id),
            });
        }

        self.api_versions = Some(response.clone());
        Ok(response)
    }

    /// Get cluster metadata
    pub async fn get_metadata(&mut self, topics: Option<Vec<String>>) -> Result<MetadataResponse> {
        let correlation_id = self.connection.next_correlation_id();
        let request = MetadataRequest::new(correlation_id, topics.clone());

        self.connection.send_request(&request.encode()).await?;

        let response_bytes =
            tokio::time::timeout(self.config.request_timeout, self.connection.read_response())
                .await
                .map_err(|_| KafkaError::ConnectionFailed("Request timeout".to_string()))??;

        let response = MetadataResponse::decode(&response_bytes)?;
        if response.correlation_id != correlation_id {
            return Err(KafkaError::InvalidResponse {
                expected: format!("correlation_id {}", correlation_id),
                actual: format!("correlation_id: {}", response.correlation_id),
            });
        }

        Ok(response)
    }

    /// Fetch messages
    pub async fn fetch_messages(&mut self, topics: Vec<FetchTopic>) -> Result<FetchResponse> {
        let correlation_id = self.connection.next_correlation_id();
        let request = FetchRequest::new(correlation_id, -1, 500, 0, 1_048_576, 0, topics);

        self.connection.send_request(&request.encode()).await?;

        let response_bytes =
            tokio::time::timeout(self.config.request_timeout, self.connection.read_response())
                .await
                .map_err(|_| KafkaError::ConnectionFailed("Request timeout".to_string()))??;

        let response = FetchResponse::decode(&response_bytes)?;

        if response.correlation_id != correlation_id {
            return Err(KafkaError::InvalidResponse {
                expected: format!("correlation_id: {}", correlation_id),
                actual: format!("correlation_id: {}", response.correlation_id),
            });
        }

        Ok(response)
    }

    /// Offset Fetch messages
    pub async fn fetch_offsets(
        &mut self,
        topics: Vec<OffsetFetchTopic>,
    ) -> Result<OffsetFetchResponse> {
        let correlation_id = self.connection.next_correlation_id();
        let request = OffsetFetchRequest::new(correlation_id, String::from("group-id"), topics);

        self.connection.send_request(&request.encode()).await?;

        let response_bytes =
            tokio::time::timeout(self.config.request_timeout, self.connection.read_response())
                .await
                .map_err(|_| KafkaError::ConnectionFailed("Request timeout".to_string()))??;

        let response = OffsetFetchResponse::decode(&response_bytes)?;

        if response.correlation_id != correlation_id {
            return Err(KafkaError::InvalidResponse {
                expected: format!("correlation_id: {}", correlation_id),
                actual: format!("correlation_id: {}", response.correlation_id),
            });
        }

        Ok(response)
    }

    /// List Offsets
    pub async fn list_offsets(
        &mut self,
        topics: Vec<ListOffsetTopic>,
    ) -> Result<ListOffsetResponse> {
        let correlation_id = self.connection.next_correlation_id();
        let request = ListOffsetRequest::new(correlation_id, -1, topics);

        self.connection.send_request(&request.encode()).await?;

        let response_bytes =
            tokio::time::timeout(self.config.request_timeout, self.connection.read_response())
                .await
                .map_err(|_| KafkaError::ConnectionFailed("Request timeout".to_string()))??;

        let response = ListOffsetResponse::decode(&response_bytes)?;

        if response.correlation_id != correlation_id {
            return Err(KafkaError::InvalidResponse {
                expected: format!("correlation_id: {}", correlation_id),
                actual: format!("correlation_id: {}", response.correlation_id),
            });
        }

        Ok(response)
    }

    /// Fetch topic configurations
    pub async fn fetch_topic_configs(&mut self, topic_name: String) -> Result<DescribeConfigsResponse> {
        let correlation_id = self.connection.next_correlation_id();

        let resource = DescribeConfigsResource::new(ResourceType::Topic, topic_name);
        let request = DescribeConfigsRequest::new(correlation_id, vec![resource]);

        self.connection.send_request(&request.encode()).await?;

        let response_bytes =
            tokio::time::timeout(self.config.request_timeout, self.connection.read_response())
                .await
                .map_err(|_| KafkaError::ConnectionFailed("Request timeout".to_string()))??;

        let response = DescribeConfigsResponse::decode(&response_bytes)?;

        if response.correlation_id != correlation_id {
            return Err(KafkaError::InvalidResponse {
                expected: format!("correlation_id: {}", correlation_id),
                actual: format!("correlation_id: {}", response.correlation_id),
            });
        }

        Ok(response)
    }

    /// Disconnect gracefully
    pub async fn disconnect(&mut self) -> Result<()> {
        self.connection.disconnect().await
    }
}
