use std::time::Duration;

use crate::{
    client::{ClientConfig, KafkaClient},
    connection::KafkaConnection,
    error::{KafkaError, Result},
};

pub struct ClientBuilder {
    brokers: Vec<String>,
    config: ClientConfig,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            brokers: vec![],
            config: ClientConfig::default(),
        }
    }

    pub fn broker(mut self, broker: &str) -> Self {
        self.brokers.push(broker.to_string());
        self
    }

    pub fn brokers(mut self, brokers: Vec<&str>) -> Self {
        self.brokers = brokers.iter().map(|b| b.to_string()).collect();
        self
    }

    pub fn client_id(mut self, id: &str) -> Self {
        self.config.client_id = id.to_string();
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
        self
    }

    pub fn retry_count(mut self, count: u32) -> Self {
        self.config.retry_count = count;
        self
    }

    #[allow(dead_code)]
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.config.retry_backoff = backoff;
        self
    }

    // NOTE: I think build shouldn't connect the connection, but open for an improvement here
    pub async fn build(self) -> Result<KafkaClient> {
        if self.brokers.is_empty() {
            return Err(KafkaError::ProtocolError(
                "No brokers specified".to_string(),
            ));
        }

        // Connect brokers in order
        let mut last_error = None;

        for broker in self.brokers.iter() {
            println!("[INFO] Attempting to connect to broker: {}", broker);

            let mut connection = KafkaConnection::new(broker.clone());

            match tokio::time::timeout(self.config.connection_timeout, connection.connect()).await {
                Ok(Ok(_)) => {
                    let client = KafkaClient {
                        connection,
                        config: self.config.clone(),
                        api_versions: None,
                    };

                    // match client.get_api_versions().await {
                    //     Ok(res) => {
                    //         println!(
                    //             "[INFO] Broker support {} API versions",
                    //             res.api_versions.len()
                    //         );
                    //         return Ok(client);
                    //     }
                    //     Err(e) => {
                    //         println!("[WARN] Failed to get API version from {}: {:?}", broker, e);
                    //         last_error = Some(e);
                    //     }
                    // }

                    return Ok(client);
                }
                Ok(Err(e)) => {
                    println!("[WARN] Failed to connect to {}: {:?}", broker, e);
                }
                Err(_) => {
                    println!("[WARN] Connection timeout to {}", broker);
                    last_error = Some(KafkaError::ProtocolError(format!(
                        "Connection timeout to {}",
                        broker
                    )));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            KafkaError::ConnectionFailed("Failed to connect to any broker".to_string())
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_default() {
        let builder = ClientBuilder::new();
        assert!(builder.brokers.is_empty());
        assert_eq!(builder.config.client_id, "kafkatui-rs-client");
        assert_eq!(builder.config.retry_count, 3);
    }

    #[test]
    fn test_builder_chain() {
        let builder = ClientBuilder::new()
            .broker("localhost:9092")
            .broker("localhost:9093")
            .client_id("my-client")
            .retry_count(5)
            .request_timeout(Duration::from_secs(60));

        assert_eq!(builder.brokers.len(), 2);
        assert_eq!(builder.config.client_id, "my-client");
        assert_eq!(builder.config.retry_count, 5);
        assert_eq!(builder.config.request_timeout, Duration::from_secs(60));
    }

    #[tokio::test]
    async fn test_build_no_brokers() {
        let result = ClientBuilder::new().build().await;
        assert!(result.is_err());

        match result {
            Err(KafkaError::ProtocolError(msg)) => {
                assert_eq!(msg, "No brokers specified");
            }
            _ => panic!("Expected ProtocolError for no brokers"),
        }
    }

    #[tokio::test]
    async fn test_build_invalid_broker() {
        let result = ClientBuilder::new()
            .broker("invalid.broker:9092")
            .connection_timeout(Duration::from_millis(100))
            .build()
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_build_with_fallback() {
        // First broker invalid, second might work
        let builder = ClientBuilder::new()
            .broker("invalid.broker:9092")
            .broker("localhost:9092")
            .connection_timeout(Duration::from_millis(500));

        // Will fail on invalid broker, try localhost
        let result = builder.build().await;
        // Result depends on whether Kafka is running
        assert!(result.is_ok());
    }
}
