use std::time::Duration;

use crate::models::broker_stats::{BrokerStats, ConnectionStatus};

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
    // pub async fn connect(broker: &str) -> Result<Self>;
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

        println!("[DEBUG] Requesting metadata for topics: {:?}", topics);

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

    /// Disconnect gracefully
    pub async fn disconnect(&mut self) -> Result<()> {
        self.connection.disconnect().await
    }
}
