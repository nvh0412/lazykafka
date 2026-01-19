/// Represents the type of panel displayed in the main content area
#[derive(Clone, Debug, PartialEq, Default)]
pub enum MainPanelType {
    /// Default - Kafka message records
    #[default]
    Records,
    /// Consumer group details (inline panel)
    GroupDetail(String),
}
