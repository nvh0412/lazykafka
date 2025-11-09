use std::time::Duration;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

use crate::error::{KafkaError, Result};

// ============================================================================
// Connection State Management
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connected,
    Failed,
}

pub struct KafkaConnection {
    stream: Option<TcpStream>,
    broker_address: String,
    state: ConnectionState,
    correlation_id: i32,
    /// Track bytes sent/received for debugging
    bytes_sent: usize,
    bytes_received: usize,
}

impl KafkaConnection {
    pub fn new(broker: String) -> Self {
        Self {
            stream: None,
            broker_address: broker,
            state: ConnectionState::Disconnected,
            correlation_id: 0,
            bytes_sent: 0,
            bytes_received: 0,
        }
    }

    pub fn bytes_sent(&self) -> usize {
        self.bytes_sent
    }

    pub fn bytes_received(&self) -> usize {
        self.bytes_received
    }

    pub async fn connect(&mut self) -> Result<()> {
        match TcpStream::connect(&self.broker_address).await {
            Ok(stream) => {
                println!("[INFO] Successfully connected to {}", self.broker_address);
                self.stream = Some(stream);
                self.state = ConnectionState::Connected;

                Ok(())
            }
            Err(e) => {
                println!(
                    "[ERROR] Failed to connect to {}: {}",
                    self.broker_address, e
                );
                self.state = ConnectionState::Failed;
                Err(KafkaError::ConnectionFailed(self.broker_address.clone()))
            }
        }
    }

    pub fn next_correlation_id(&mut self) -> i32 {
        self.correlation_id += 1;
        self.correlation_id
    }

    pub fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected && self.stream.is_some()
    }

    pub async fn send_request(&mut self, request: &[u8]) -> Result<()> {
        if !self.is_connected() {
            return Err(KafkaError::ConnectionFailed("Not connected".to_string()));
        }

        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| KafkaError::ConnectionFailed("Stream is None".to_string()))?;

        match stream.write_all(request).await {
            Ok(_) => {
                stream.flush().await?;
                self.bytes_sent += request.len();
                Ok(())
            }
            Err(e) => {
                self.state = ConnectionState::Failed;
                Err(KafkaError::Io(e))
            }
        }
    }

    /// Read a complete response from Kafka
    /// Returns the response bytes (without the size header)
    pub async fn read_response(&mut self) -> Result<Vec<u8>> {
        self.read_response_with_timeout(Duration::from_secs(30))
            .await
    }

    pub async fn read_response_with_timeout(
        &mut self,
        timeout_duration: Duration,
    ) -> Result<Vec<u8>> {
        if !self.is_connected() {
            return Err(KafkaError::ConnectionFailed("Not connected".to_string()));
        }

        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| KafkaError::ConnectionFailed("Stream is None".to_string()))?;

        // a buffer with 4 bytes header
        let mut size_buf = [0u8; 4];

        // Read header
        match timeout(timeout_duration, stream.read_exact(&mut size_buf)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                self.state = ConnectionState::Failed;
                return Err(KafkaError::Io(e));
            }
            Err(_) => {
                return Err(KafkaError::ProtocolError("Response timeout".to_string()));
            }
        }

        let size = i32::from_be_bytes(size_buf) as usize;

        if size == 0 {
            return Err(KafkaError::ProtocolError(
                "Received zero-length response".to_string(),
            ));
        }

        if size > 100_000_000 {
            return Err(KafkaError::ProtocolError(format!(
                "Response size too large: {} bytes",
                size
            )));
        }

        // Read the complete response body
        let mut response = vec![0u8; size];

        match timeout(timeout_duration, stream.read_exact(&mut response)).await {
            Ok(Ok(_)) => {
                self.bytes_received += 4 + size;
                Ok(response)
            }
            Ok(Err(e)) => {
                self.state = ConnectionState::Failed;
                Err(KafkaError::Io(e))
            }
            Err(_) => Err(KafkaError::ProtocolError(
                "Response body timeout".to_string(),
            )),
        }
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            stream.shutdown().await?;
            self.state = ConnectionState::Disconnected;
            println!("[INFO] Disconnected from {}", self.broker_address);
        }

        Ok(())
    }
}

#[cfg(test)]
mod connection_tests {
    use super::*;

    #[test]
    fn test_connection_creation() {
        let conn = KafkaConnection::new("localhost:9092".to_string());
        assert_eq!(conn.state, ConnectionState::Disconnected);
        assert!(!conn.is_connected());
        assert_eq!(conn.correlation_id, 0);
        assert!(conn.stream.is_none());
    }

    #[test]
    fn test_correlation_id_increment() {
        let mut conn = KafkaConnection::new("localhost:9092".to_string());
        assert_eq!(conn.next_correlation_id(), 1);
        assert_eq!(conn.next_correlation_id(), 2);
        assert_eq!(conn.next_correlation_id(), 3);
    }

    #[tokio::test]
    async fn test_send_without_connection() {
        let mut conn = KafkaConnection::new("localhost:9092".to_string());
        let result = conn.send_request(&[1, 2, 3]).await;
        assert!(result.is_err());
        match result {
            Err(KafkaError::ConnectionFailed(_)) => {}
            _ => panic!("Expected ConnectionFailed error"),
        }
    }
}

#[cfg(test)]
pub mod test_helpers {
    use std::time::Duration;

    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Start a mock Kafka server that echoes correlation IDs
    pub async fn start_mock_kafka_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 1024];

                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }

                        // Read the size header
                        if n >= 4 {
                            // Create a mock response
                            // For ApiVersions request (API key 18), send back a simple response
                            if n >= 8 && buf[4] == 0 && buf[5] == 18 {
                                // API key 18
                                let correlation_id =
                                    i32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);

                                // Build mock response
                                let mut response = Vec::new();
                                response.extend_from_slice(&[0, 0, 0, 12]); // Size (12 bytes)
                                response.extend_from_slice(&correlation_id.to_be_bytes()); // Correlation ID
                                response.extend_from_slice(&[0, 0]); // Error code 0
                                response.extend_from_slice(&[0, 0, 0, 0]); // Empty API versions array

                                socket.write_all(&response).await.ok();
                            }
                        }
                    }
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        addr
    }
}

#[cfg(test)]
mod integration_tests {
    use tokio::net::TcpListener;

    use super::test_helpers::*;
    use super::*;

    #[tokio::test]
    async fn test_connection_with_mock_server() {
        let addr = start_mock_kafka_server().await;
        let mut conn = KafkaConnection::new(addr);

        assert!(conn.connect().await.is_ok());
        assert!(conn.is_connected());
        assert_eq!(conn.state, ConnectionState::Connected);

        let mut request = vec![];
        request.extend_from_slice(&[0, 0, 0, 20]);
        request.extend_from_slice(&[0, 18]);
        request.extend_from_slice(&[0, 0]);
        request.extend_from_slice(&[0, 0, 0, 1]);
        request.extend_from_slice(&[0, 0]);

        assert!(conn.send_request(&request).await.is_ok());

        assert!(conn.disconnect().await.is_ok());
        assert!(!conn.is_connected());
    }

    #[tokio::test]
    async fn test_response_timeout() {
        // connect to a server that doesn't respond
        // We will mock a server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let mut conn = KafkaConnection::new(addr);
        conn.connect().await.unwrap();

        conn.send_request(&[0, 0, 0, 1, 42]).await.unwrap();

        let result = conn
            .read_response_with_timeout(Duration::from_millis(100))
            .await;
        assert!(result.is_err());
        match result {
            Err(KafkaError::ProtocolError(msg)) => {
                assert!(msg.contains("timeout"));
            }
            _ => panic!("Expected timeout error"),
        }
    }
}
