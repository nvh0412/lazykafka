use std::time::Duration;

use tokio::sync::mpsc;

use crate::models::broker_stats::{BrokerStats, ConnectionStatus};
use crate::models::protocol_log::ProtocolLogMessage;

use crate::protocol::create_partitions::{CreatePartitionsRequest, CreatePartitionsResponse};
use crate::protocol::delete_topics::{DeleteTopicsRequest, DeleteTopicsResponse};
use crate::protocol::describe_configs::{
    DescribeConfigsRequest, DescribeConfigsResource, DescribeConfigsResponse, ResourceType,
};
use crate::protocol::describe_groups::{DescribeGroupsRequest, DescribeGroupsResponse};
use crate::protocol::fetch::{FetchRequest, FetchResponse, FetchTopic};
use crate::protocol::find_coordinator::{FindCoordinatorRequest, FindCoordinatorResponse};
use crate::protocol::list_groups::{ListGroupsRequest, ListGroupsResponse};
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
#[allow(dead_code)]
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
#[allow(dead_code)]
pub struct KafkaClient {
    pub connection: KafkaConnection,
    pub config: ClientConfig,
    pub api_versions: Option<ApiVersionsResponse>,
}

impl KafkaClient {
    /// Set the protocol logger for the connection
    pub fn set_logger(&mut self, tx: mpsc::UnboundedSender<ProtocolLogMessage>) {
        self.connection.set_logger(tx);
    }

    pub fn get_stats(&self) -> BrokerStats {
        BrokerStats {
            broker: self.connection.broker_address().to_string(),
            status: if self.connection.is_connected() {
                ConnectionStatus::Connected
            } else {
                ConnectionStatus::Disconnected
            },
            bytes_sent: self.connection.bytes_sent(),
            bytes_received: self.connection.bytes_received(),
        }
    }

    /// Attempt to reconnect to the broker
    pub async fn reconnect(&mut self) -> Result<()> {
        // Close existing connection if any
        if self.connection.is_connected() {
            let _ = self.connection.disconnect().await;
        }

        // Try to connect with retries
        for attempt in 1..=self.config.retry_count {
            match self.connection.connect().await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    if attempt < self.config.retry_count {
                        tokio::time::sleep(self.config.retry_backoff).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Err(KafkaError::ConnectionFailed(
            "Max reconnection attempts exceeded".to_string(),
        ))
    }

    /// Check if an error is a connection error that warrants reconnection
    fn is_connection_error(error: &KafkaError) -> bool {
        matches!(error, KafkaError::ConnectionFailed(_) | KafkaError::Io(_))
    }

    /// Get API versions supported by broker
    #[allow(dead_code)]
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
        // Try the request, and retry once if connection fails
        match self.get_metadata_internal(topics.clone()).await {
            Ok(response) => Ok(response),
            Err(e) if Self::is_connection_error(&e) => {
                self.reconnect().await?;
                self.get_metadata_internal(topics).await
            }
            Err(e) => Err(e),
        }
    }

    async fn get_metadata_internal(
        &mut self,
        topics: Option<Vec<String>>,
    ) -> Result<MetadataResponse> {
        let correlation_id = self.connection.next_correlation_id();
        let request = MetadataRequest::new(correlation_id, topics);

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
        // Try the request, and retry once if connection fails
        match self.fetch_messages_internal(topics.clone()).await {
            Ok(response) => Ok(response),
            Err(e) if Self::is_connection_error(&e) => {
                self.reconnect().await?;
                self.fetch_messages_internal(topics).await
            }
            Err(e) => Err(e),
        }
    }

    async fn fetch_messages_internal(&mut self, topics: Vec<FetchTopic>) -> Result<FetchResponse> {
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

    /// List Offsets
    pub async fn list_offsets(
        &mut self,
        topics: Vec<ListOffsetTopic>,
    ) -> Result<ListOffsetResponse> {
        // Try the request, and retry once if connection fails
        match self.list_offsets_internal(topics.clone()).await {
            Ok(response) => Ok(response),
            Err(e) if Self::is_connection_error(&e) => {
                self.reconnect().await?;
                self.list_offsets_internal(topics).await
            }
            Err(e) => Err(e),
        }
    }

    async fn list_offsets_internal(
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
    pub async fn fetch_topic_configs(
        &mut self,
        topic_name: String,
    ) -> Result<DescribeConfigsResponse> {
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

    pub async fn delete_topics(
        &mut self,
        topic_names: Vec<String>,
    ) -> Result<DeleteTopicsResponse> {
        let correlation_id = self.connection.next_correlation_id();

        let resource = DeleteTopicsRequest::new(correlation_id, topic_names, 100);

        self.connection.send_request(&resource.encode()).await?;

        let response_bytes =
            tokio::time::timeout(self.config.request_timeout, self.connection.read_response())
                .await
                .map_err(|_| KafkaError::ConnectionFailed("Request timeout".to_string()))??;

        let response = DeleteTopicsResponse::decode(&response_bytes)?;

        if response.correlation_id != correlation_id {
            return Err(KafkaError::InvalidResponse {
                expected: format!("correlation_id: {}", correlation_id),
                actual: format!("correlation_id: {}", response.correlation_id),
            });
        }

        Ok(response)
    }

    /// Create additional partitions for a topic
    pub async fn create_partitions(
        &mut self,
        topic_name: String,
        new_partition_count: i32,
        timeout_ms: i32,
    ) -> Result<CreatePartitionsResponse> {
        match self
            .create_partitions_internal(topic_name.clone(), new_partition_count, timeout_ms)
            .await
        {
            Ok(response) => Ok(response),
            Err(e) if Self::is_connection_error(&e) => {
                self.reconnect().await?;
                self.create_partitions_internal(topic_name, new_partition_count, timeout_ms)
                    .await
            }
            Err(e) => Err(e),
        }
    }

    async fn create_partitions_internal(
        &mut self,
        topic_name: String,
        new_partition_count: i32,
        timeout_ms: i32,
    ) -> Result<CreatePartitionsResponse> {
        let correlation_id = self.connection.next_correlation_id();
        let request =
            CreatePartitionsRequest::new(correlation_id, topic_name, new_partition_count, timeout_ms);

        self.connection.send_request(&request.encode()).await?;

        let response_bytes =
            tokio::time::timeout(self.config.request_timeout, self.connection.read_response())
                .await
                .map_err(|_| KafkaError::ConnectionFailed("Request timeout".to_string()))??;

        let response = CreatePartitionsResponse::decode(&response_bytes)?;

        if response.correlation_id != correlation_id {
            return Err(KafkaError::InvalidResponse {
                expected: format!("correlation_id {}", correlation_id),
                actual: format!("correlation_id: {}", response.correlation_id),
            });
        }

        Ok(response)
    }

    // =====================
    // Consumer Group APIs
    // =====================

    /// Find the coordinator broker for a consumer group
    pub async fn find_coordinator(&mut self, group_id: String) -> Result<FindCoordinatorResponse> {
        match self.find_coordinator_internal(group_id.clone()).await {
            Ok(response) => Ok(response),
            Err(e) if Self::is_connection_error(&e) => {
                self.reconnect().await?;
                self.find_coordinator_internal(group_id).await
            }
            Err(e) => Err(e),
        }
    }

    async fn find_coordinator_internal(
        &mut self,
        group_id: String,
    ) -> Result<FindCoordinatorResponse> {
        let correlation_id = self.connection.next_correlation_id();
        let request = FindCoordinatorRequest::for_group(correlation_id, group_id);

        self.connection.send_request(&request.encode()).await?;

        let response_bytes =
            tokio::time::timeout(self.config.request_timeout, self.connection.read_response())
                .await
                .map_err(|_| KafkaError::ConnectionFailed("Request timeout".to_string()))??;

        let response = FindCoordinatorResponse::decode(&response_bytes)?;

        if response.correlation_id != correlation_id {
            return Err(KafkaError::InvalidResponse {
                expected: format!("correlation_id: {}", correlation_id),
                actual: format!("correlation_id: {}", response.correlation_id),
            });
        }

        Ok(response)
    }

    /// List all consumer groups on the cluster
    pub async fn list_groups(&mut self) -> Result<ListGroupsResponse> {
        match self.list_groups_internal().await {
            Ok(response) => Ok(response),
            Err(e) if Self::is_connection_error(&e) => {
                self.reconnect().await?;
                self.list_groups_internal().await
            }
            Err(e) => Err(e),
        }
    }

    async fn list_groups_internal(&mut self) -> Result<ListGroupsResponse> {
        let correlation_id = self.connection.next_correlation_id();
        let request = ListGroupsRequest::new(correlation_id);

        self.connection.send_request(&request.encode()).await?;

        let response_bytes =
            tokio::time::timeout(self.config.request_timeout, self.connection.read_response())
                .await
                .map_err(|_| KafkaError::ConnectionFailed("Request timeout".to_string()))??;

        let response = ListGroupsResponse::decode(&response_bytes)?;

        if response.correlation_id != correlation_id {
            return Err(KafkaError::InvalidResponse {
                expected: format!("correlation_id: {}", correlation_id),
                actual: format!("correlation_id: {}", response.correlation_id),
            });
        }

        Ok(response)
    }

    /// Describe consumer groups (get detailed info including members)
    pub async fn describe_groups(
        &mut self,
        group_ids: Vec<String>,
    ) -> Result<DescribeGroupsResponse> {
        match self.describe_groups_internal(group_ids.clone()).await {
            Ok(response) => Ok(response),
            Err(e) if Self::is_connection_error(&e) => {
                self.reconnect().await?;
                self.describe_groups_internal(group_ids).await
            }
            Err(e) => Err(e),
        }
    }

    async fn describe_groups_internal(
        &mut self,
        group_ids: Vec<String>,
    ) -> Result<DescribeGroupsResponse> {
        let correlation_id = self.connection.next_correlation_id();
        let request = DescribeGroupsRequest::new(correlation_id, group_ids);

        self.connection.send_request(&request.encode()).await?;

        let response_bytes =
            tokio::time::timeout(self.config.request_timeout, self.connection.read_response())
                .await
                .map_err(|_| KafkaError::ConnectionFailed("Request timeout".to_string()))??;

        let response = DescribeGroupsResponse::decode(&response_bytes)?;

        if response.correlation_id != correlation_id {
            return Err(KafkaError::InvalidResponse {
                expected: format!("correlation_id: {}", correlation_id),
                actual: format!("correlation_id: {}", response.correlation_id),
            });
        }

        Ok(response)
    }

    /// Fetch committed offsets for a consumer group
    pub async fn fetch_offsets(
        &mut self,
        group_id: String,
        topics: Option<Vec<OffsetFetchTopic>>,
    ) -> Result<OffsetFetchResponse> {
        match self.fetch_offsets_internal(group_id.clone(), topics.clone()).await {
            Ok(response) => Ok(response),
            Err(e) if Self::is_connection_error(&e) => {
                self.reconnect().await?;
                self.fetch_offsets_internal(group_id, topics).await
            }
            Err(e) => Err(e),
        }
    }

    async fn fetch_offsets_internal(
        &mut self,
        group_id: String,
        topics: Option<Vec<OffsetFetchTopic>>,
    ) -> Result<OffsetFetchResponse> {
        let correlation_id = self.connection.next_correlation_id();
        let request = OffsetFetchRequest::new(correlation_id, group_id, topics);

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

    /// Disconnect gracefully
    #[allow(dead_code)]
    pub async fn disconnect(&mut self) -> Result<()> {
        self.connection.disconnect().await
    }
}
