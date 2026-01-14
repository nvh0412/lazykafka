#[derive(Debug, Clone, Default)]
pub struct BrokerStats {
    pub broker: String,
    pub status: ConnectionStatus,
    pub bytes_sent: usize,
    pub bytes_received: usize,
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub enum ConnectionStatus {
    Connected,
    Connecting,
    #[default]
    Disconnected,
    Failed(String),
}
