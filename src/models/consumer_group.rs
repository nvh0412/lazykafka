use ratatui::prelude::Color;

/// Consumer group state enumeration
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerGroupState {
    Stable,
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Dead,
    Unknown,
}

impl From<&str> for ConsumerGroupState {
    fn from(s: &str) -> Self {
        match s {
            "Stable" => Self::Stable,
            "Empty" => Self::Empty,
            "PreparingRebalance" => Self::PreparingRebalance,
            "CompletingRebalance" => Self::CompletingRebalance,
            "Dead" => Self::Dead,
            _ => Self::Unknown,
        }
    }
}

impl ConsumerGroupState {
    pub fn display_color(&self) -> Color {
        match self {
            Self::Stable => Color::Green,
            Self::Empty => Color::Yellow,
            Self::PreparingRebalance => Color::Cyan,
            Self::CompletingRebalance => Color::Cyan,
            Self::Dead => Color::Red,
            Self::Unknown => Color::Gray,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stable => "Stable",
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Dead => "Dead",
            Self::Unknown => "Unknown",
        }
    }
}

/// Summary for list view
#[derive(Debug, Clone)]
pub struct ConsumerGroupSummary {
    pub group_id: String,
    pub state: ConsumerGroupState,
    pub protocol_type: String,
    pub member_count: usize,
    pub coordinator_id: Option<i32>,
}

/// Full detail for detail view
#[derive(Debug, Clone)]
pub struct ConsumerGroupDetail {
    pub group_id: String,
    pub state: ConsumerGroupState,
    pub protocol_type: String,
    pub protocol: String,
    pub coordinator: Option<CoordinatorInfo>,
    pub members: Vec<ConsumerGroupMember>,
    pub topic_offsets: Vec<TopicOffsetInfo>,
}

#[derive(Debug, Clone)]
pub struct CoordinatorInfo {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

/// Member with assignments
#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignments: Vec<PartitionAssignment>,
}

#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

/// Offset info for lag calculation
#[derive(Debug, Clone)]
pub struct TopicOffsetInfo {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetInfo>,
}

#[derive(Debug, Clone)]
pub struct PartitionOffsetInfo {
    pub partition: i32,
    pub committed_offset: i64,
    pub end_offset: i64,
    pub lag: i64,
}

impl PartitionOffsetInfo {
    pub fn new(partition: i32, committed_offset: i64, end_offset: i64) -> Self {
        let lag = if committed_offset >= 0 && end_offset >= 0 {
            end_offset - committed_offset
        } else {
            0
        };
        Self {
            partition,
            committed_offset,
            end_offset,
            lag,
        }
    }
}

impl ConsumerGroupDetail {
    /// Calculate total lag across all partitions
    pub fn total_lag(&self) -> i64 {
        self.topic_offsets
            .iter()
            .flat_map(|t| t.partitions.iter())
            .map(|p| p.lag)
            .sum()
    }

    /// Get number of topics this group is subscribed to
    pub fn topic_count(&self) -> usize {
        self.topic_offsets.len()
    }

    /// Get total number of assigned partitions
    pub fn partition_count(&self) -> usize {
        self.members
            .iter()
            .flat_map(|m| m.assignments.iter())
            .map(|a| a.partitions.len())
            .sum()
    }
}
