#[derive(Debug, Clone, Default)]
pub struct BrokerStats {
    pub broker: String,
    pub status: ConnectionStatus,
    pub bytes_sent: usize,
    pub bytes_received: usize,
}

#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    Connected,
    Connecting,
    Disconnected,
    Failed(String),
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        ConnectionStatus::Disconnected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_stats_default() {
        let stats = BrokerStats::default();
        assert_eq!(stats.broker, String::new());
        assert!(matches!(stats.status, ConnectionStatus::Disconnected));
        assert_eq!(stats.bytes_sent, 0);
        assert_eq!(stats.bytes_received, 0);
    }

    #[test]
    fn test_broker_stats_construction() {
        let stats = BrokerStats {
            broker: "localhost:9092".to_string(),
            status: ConnectionStatus::Connected,
            bytes_sent: 1024,
            bytes_received: 2048,
        };
        assert_eq!(stats.broker, "localhost:9092");
        assert!(matches!(stats.status, ConnectionStatus::Connected));
        assert_eq!(stats.bytes_sent, 1024);
        assert_eq!(stats.bytes_received, 2048);
    }

    #[test]
    fn test_broker_stats_clone() {
        let stats = BrokerStats {
            broker: "localhost:9092".to_string(),
            status: ConnectionStatus::Connecting,
            bytes_sent: 512,
            bytes_received: 1024,
        };
        let cloned = stats.clone();
        assert_eq!(stats.broker, cloned.broker);
        assert_eq!(stats.bytes_sent, cloned.bytes_sent);
        assert_eq!(stats.bytes_received, cloned.bytes_received);
    }

    #[test]
    fn test_broker_stats_debug() {
        let stats = BrokerStats {
            broker: "localhost:9092".to_string(),
            status: ConnectionStatus::Connected,
            bytes_sent: 100,
            bytes_received: 200,
        };
        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("BrokerStats"));
        assert!(debug_str.contains("localhost:9092"));
    }

    #[test]
    fn test_connection_status_default() {
        let status = ConnectionStatus::default();
        assert!(matches!(status, ConnectionStatus::Disconnected));
    }

    #[test]
    fn test_connection_status_connected() {
        let status = ConnectionStatus::Connected;
        assert!(matches!(status, ConnectionStatus::Connected));
    }

    #[test]
    fn test_connection_status_connecting() {
        let status = ConnectionStatus::Connecting;
        assert!(matches!(status, ConnectionStatus::Connecting));
    }

    #[test]
    fn test_connection_status_disconnected() {
        let status = ConnectionStatus::Disconnected;
        assert!(matches!(status, ConnectionStatus::Disconnected));
    }

    #[test]
    fn test_connection_status_failed() {
        let status = ConnectionStatus::Failed("Connection timeout".to_string());
        match status {
            ConnectionStatus::Failed(msg) => assert_eq!(msg, "Connection timeout"),
            _ => panic!("Expected Failed status"),
        }
    }

    #[test]
    fn test_connection_status_failed_empty_message() {
        let status = ConnectionStatus::Failed(String::new());
        match status {
            ConnectionStatus::Failed(msg) => assert_eq!(msg, ""),
            _ => panic!("Expected Failed status"),
        }
    }

    #[test]
    fn test_connection_status_clone() {
        let status = ConnectionStatus::Failed("Error message".to_string());
        let cloned = status.clone();
        match (status, cloned) {
            (ConnectionStatus::Failed(msg1), ConnectionStatus::Failed(msg2)) => {
                assert_eq!(msg1, msg2);
            }
            _ => panic!("Expected both to be Failed status"),
        }
    }

    #[test]
    fn test_connection_status_debug() {
        let status = ConnectionStatus::Connected;
        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("Connected"));

        let status = ConnectionStatus::Failed("Test error".to_string());
        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("Failed"));
        assert!(debug_str.contains("Test error"));
    }

    #[test]
    fn test_broker_stats_with_different_statuses() {
        let statuses = vec![
            ConnectionStatus::Connected,
            ConnectionStatus::Connecting,
            ConnectionStatus::Disconnected,
            ConnectionStatus::Failed("Network error".to_string()),
        ];

        for status in statuses {
            let stats = BrokerStats {
                broker: "test-broker".to_string(),
                status,
                bytes_sent: 0,
                bytes_received: 0,
            };
            // Ensure the stats can be created with any status
            assert_eq!(stats.broker, "test-broker");
        }
    }

    #[test]
    fn test_broker_stats_large_byte_counts() {
        let stats = BrokerStats {
            broker: "localhost:9092".to_string(),
            status: ConnectionStatus::Connected,
            bytes_sent: usize::MAX,
            bytes_received: usize::MAX,
        };
        assert_eq!(stats.bytes_sent, usize::MAX);
        assert_eq!(stats.bytes_received, usize::MAX);
    }

    #[test]
    fn test_broker_stats_zero_byte_counts() {
        let stats = BrokerStats {
            broker: "localhost:9092".to_string(),
            status: ConnectionStatus::Disconnected,
            bytes_sent: 0,
            bytes_received: 0,
        };
        assert_eq!(stats.bytes_sent, 0);
        assert_eq!(stats.bytes_received, 0);
    }
}
