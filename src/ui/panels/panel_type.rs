/// Represents the type of panel displayed in the main content area
#[derive(Clone, Debug, PartialEq)]
pub enum MainPanelType {
    /// Default - Kafka message records
    Records,
    /// Consumer group details (inline panel)
    GroupDetail(String),
    // Future panel types:
    // BrokerDetail(i32),
}

impl Default for MainPanelType {
    fn default() -> Self {
        Self::Records
    }
}
