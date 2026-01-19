use std::time::Duration;

use socket2::{Socket, TcpKeepalive};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    time::timeout,
};

use crate::error::{KafkaError, Result};
use crate::models::protocol_log::{ApiType, ProtocolLogMessage};

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
    /// Optional channel for protocol logging
    log_tx: Option<mpsc::UnboundedSender<ProtocolLogMessage>>,
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
            log_tx: None,
        }
    }

    /// Set the protocol logger channel
    pub fn set_logger(&mut self, tx: mpsc::UnboundedSender<ProtocolLogMessage>) {
        self.log_tx = Some(tx);
    }

    /// Get the broker address
    pub fn broker_address(&self) -> &str {
        &self.broker_address
    }

    /// Parse request header to extract API key, version, and correlation ID
    /// Kafka request header format: size(4) + api_key(2) + api_version(2) + correlation_id(4) + ...
    fn parse_request_header(&self, request: &[u8]) -> Option<(i16, i16, i32)> {
        if request.len() < 12 {
            return None;
        }
        // Skip size (4 bytes), read api_key (2 bytes)
        let api_key = i16::from_be_bytes([request[4], request[5]]);
        let api_version = i16::from_be_bytes([request[6], request[7]]);
        let correlation_id = i32::from_be_bytes([request[8], request[9], request[10], request[11]]);
        Some((api_key, api_version, correlation_id))
    }

    /// Generate a summary for a request based on API type
    fn generate_request_summary(&self, api_type: ApiType) -> String {
        match api_type {
            ApiType::Metadata => "Refreshing metadata".to_string(),
            ApiType::Fetch => "Fetching messages".to_string(),
            ApiType::ListOffset => "Listing offsets".to_string(),
            ApiType::ApiVersions => "Getting API versions".to_string(),
            ApiType::FindCoordinator => "Finding coordinator".to_string(),
            ApiType::ListGroups => "Listing groups".to_string(),
            ApiType::DescribeGroups => "Describing groups".to_string(),
            ApiType::OffsetFetch => "Fetching offsets".to_string(),
            ApiType::DescribeConfigs => "Describing configs".to_string(),
            ApiType::DeleteTopics => "Deleting topics".to_string(),
            ApiType::CreatePartitions => "Creating partitions".to_string(),
            ApiType::Unknown(key) => format!("Unknown API {}", key),
        }
    }

    /// Log a request if logging is enabled
    fn log_request(&self, request: &[u8]) {
        if let Some(tx) = &self.log_tx
            && let Some((api_key, api_version, correlation_id)) = self.parse_request_header(request)
        {
            let api_type = ApiType::from_api_key(api_key);
            let summary = self.generate_request_summary(api_type);
            let _ = tx.send(ProtocolLogMessage::Request {
                api_type,
                api_version,
                correlation_id,
                broker: self.broker_address.clone(),
                bytes: request.len(),
                summary,
            });
        }
    }

    /// Log a response if logging is enabled
    fn log_response(&self, correlation_id: i32, response: &[u8], error_code: Option<i16>) {
        if let Some(tx) = &self.log_tx {
            let summary = format!("{} bytes", response.len());
            let _ = tx.send(ProtocolLogMessage::Response {
                correlation_id,
                bytes: response.len(),
                error_code,
                summary,
            });
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
                // Configure TCP keep-alive to prevent idle connection timeouts
                if let Err(e) = Self::configure_keepalive(&stream) {
                    println!("[WARN] Failed to configure TCP keep-alive: {}", e);
                    // Continue anyway - keep-alive is a nice-to-have, not required
                }

                self.stream = Some(stream);
                self.state = ConnectionState::Connected;

                Ok(())
            }
            Err(_e) => {
                self.state = ConnectionState::Failed;
                Err(KafkaError::ConnectionFailed(self.broker_address.clone()))
            }
        }
    }

    /// Configure TCP keep-alive to prevent idle connection drops
    #[cfg(unix)]
    fn configure_keepalive(stream: &TcpStream) -> std::io::Result<()> {
        use std::os::unix::io::{AsRawFd, FromRawFd};

        // Get the raw file descriptor from the tokio TcpStream
        let raw_fd = stream.as_raw_fd();

        // Create a socket2::Socket from the raw fd (without taking ownership)
        let socket = unsafe { Socket::from_raw_fd(raw_fd) };

        // Configure keep-alive parameters:
        // - Start probing after 60 seconds of idle time
        // - Send probes every 30 seconds
        // - Close connection after 3 failed probes (90 seconds total)
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(30));

        socket.set_tcp_keepalive(&keepalive)?;

        // Prevent Socket from closing the fd when dropped (we don't own it)
        std::mem::forget(socket);

        Ok(())
    }

    #[cfg(windows)]
    fn configure_keepalive(stream: &TcpStream) -> std::io::Result<()> {
        use std::os::windows::io::{AsRawSocket, FromRawSocket};

        let raw_socket = stream.as_raw_socket();
        let socket = unsafe { Socket::from_raw_socket(raw_socket) };

        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(30));

        socket.set_tcp_keepalive(&keepalive)?;
        std::mem::forget(socket);

        Ok(())
    }

    pub fn next_correlation_id(&mut self) -> i32 {
        self.correlation_id += 1;
        self.correlation_id
    }

    pub fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected && self.stream.is_some()
    }

    /// Validate that the connection is actually alive by checking socket health
    pub async fn validate_connection(&mut self) -> bool {
        if !self.is_connected() {
            return false;
        }

        // Try to peek at the socket to see if it's still alive
        // If the connection was closed by the broker, peek will detect it
        if let Some(stream) = &self.stream {
            let mut buf = [0u8; 1];
            match stream.try_read(&mut buf) {
                Ok(0) => {
                    // Connection closed by peer (read returned 0 bytes)
                    self.state = ConnectionState::Failed;
                    false
                }
                Ok(_) => {
                    // This shouldn't happen in normal operation (we read actual data)
                    // This means there's buffered data we didn't expect
                    true
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No data available to read - connection is healthy
                    true
                }
                Err(_) => {
                    // Some other error - treat as connection failure
                    self.state = ConnectionState::Failed;
                    false
                }
            }
        } else {
            false
        }
    }

    pub async fn send_request(&mut self, request: &[u8]) -> Result<()> {
        // Validate connection health before sending
        if !self.validate_connection().await {
            return Err(KafkaError::ConnectionFailed(
                "Connection validation failed".to_string(),
            ));
        }

        // Log the request before sending
        self.log_request(request);

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
                // Classify the error - is it a connection issue or something else?
                let is_connection_error = matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::NotConnected
                );

                if is_connection_error {
                    self.state = ConnectionState::Failed;
                }

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
                // Classify the error
                let is_connection_error = matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::NotConnected
                        | std::io::ErrorKind::UnexpectedEof
                );

                if is_connection_error {
                    println!(
                        "[ERROR] Connection lost while reading from {}: {}",
                        self.broker_address, e
                    );
                    self.state = ConnectionState::Failed;
                }

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

                // Log the response (correlation_id is the first 4 bytes)
                if response.len() >= 4 {
                    let correlation_id =
                        i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
                    // For error code, we'd need to know the API type - for now pass None
                    self.log_response(correlation_id, &response, None);
                }

                Ok(response)
            }
            Ok(Err(e)) => {
                // Classify the error
                let is_connection_error = matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::NotConnected
                        | std::io::ErrorKind::UnexpectedEof
                );

                if is_connection_error {
                    println!(
                        "[ERROR] Connection lost while reading body from {}: {}",
                        self.broker_address, e
                    );
                    self.state = ConnectionState::Failed;
                }

                Err(KafkaError::Io(e))
            }
            Err(_) => Err(KafkaError::ProtocolError(
                "Response body timeout".to_string(),
            )),
        }
    }

    #[allow(dead_code)]
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
