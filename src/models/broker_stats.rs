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
