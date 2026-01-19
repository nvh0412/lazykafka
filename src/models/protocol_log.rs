use std::time::{Duration, SystemTime};

/// Direction of a protocol message
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogDirection {
    Request,
    Response,
}

/// Message sent from connection layer to application for protocol logging
#[derive(Debug, Clone)]
pub enum ProtocolLogMessage {
    Request {
        api_type: ApiType,
        api_version: i16,
        correlation_id: i32,
        broker: String,
        bytes: usize,
        summary: String,
    },
    Response {
        correlation_id: i32,
        bytes: usize,
        error_code: Option<i16>,
        summary: String,
    },
}

/// Type of Kafka API request/response
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiType {
    Metadata,
    Fetch,
    ListOffset,
    ApiVersions,
    FindCoordinator,
    ListGroups,
    DescribeGroups,
    OffsetFetch,
    DescribeConfigs,
    DeleteTopics,
    CreatePartitions,
    Unknown(i16),
}

impl ApiType {
    /// Create ApiType from Kafka API key
    pub fn from_api_key(api_key: i16) -> Self {
        match api_key {
            0 => ApiType::Fetch, // Produce is 0, but we only do Fetch
            1 => ApiType::Fetch,
            2 => ApiType::ListOffset,
            3 => ApiType::Metadata,
            10 => ApiType::FindCoordinator,
            15 => ApiType::DescribeGroups,
            16 => ApiType::ListGroups,
            18 => ApiType::ApiVersions,
            9 => ApiType::OffsetFetch,
            32 => ApiType::DescribeConfigs,
            20 => ApiType::DeleteTopics,
            37 => ApiType::CreatePartitions,
            _ => ApiType::Unknown(api_key),
        }
    }

    /// Get display name for the API type
    pub fn display_name(&self) -> &'static str {
        match self {
            ApiType::Metadata => "Metadata",
            ApiType::Fetch => "Fetch",
            ApiType::ListOffset => "ListOffset",
            ApiType::ApiVersions => "ApiVersions",
            ApiType::FindCoordinator => "FindCoord",
            ApiType::ListGroups => "ListGroups",
            ApiType::DescribeGroups => "DescGroups",
            ApiType::OffsetFetch => "OffsetFetch",
            ApiType::DescribeConfigs => "DescConfigs",
            ApiType::DeleteTopics => "DeleteTopics",
            ApiType::CreatePartitions => "CreateParts",
            ApiType::Unknown(_) => "Unknown",
        }
    }
}

/// Status of a log entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogStatus {
    Pending,
    Success,
    Error,
}

/// A single protocol log entry
#[derive(Debug, Clone)]
pub struct ProtocolLogEntry {
    pub timestamp: SystemTime,
    pub direction: LogDirection,
    pub api_type: ApiType,
    pub api_version: i16,
    pub bytes: usize,
    pub duration: Option<Duration>,
    pub status: LogStatus,
    pub summary: String,
}

impl ProtocolLogEntry {
    /// Create a new request log entry
    pub fn new_request(api_type: ApiType, api_version: i16, bytes: usize, summary: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            direction: LogDirection::Request,
            api_type,
            api_version,
            bytes,
            duration: None,
            status: LogStatus::Pending,
            summary,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_response(
        api_type: ApiType,
        api_version: i16,
        bytes: usize,
        duration: Duration,
        error_code: Option<i16>,
        summary: String,
    ) -> Self {
        let (status, _error_message) = if let Some(code) = error_code {
            if code != 0 {
                (LogStatus::Error, Some(format!("Error code: {}", code)))
            } else {
                (LogStatus::Success, None)
            }
        } else {
            (LogStatus::Success, None)
        };

        Self {
            timestamp: SystemTime::now(),
            direction: LogDirection::Response,
            api_type,
            api_version,
            bytes,
            duration: Some(duration),
            status,
            summary,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolLogFilter {
    #[default]
    All,
    Fetch,
    Metadata,
    Groups,
    Errors,
}

impl ProtocolLogFilter {
    pub fn next(&self) -> Self {
        match self {
            ProtocolLogFilter::All => ProtocolLogFilter::Fetch,
            ProtocolLogFilter::Fetch => ProtocolLogFilter::Metadata,
            ProtocolLogFilter::Metadata => ProtocolLogFilter::Groups,
            ProtocolLogFilter::Groups => ProtocolLogFilter::Errors,
            ProtocolLogFilter::Errors => ProtocolLogFilter::All,
        }
    }

    pub fn matches(&self, entry: &ProtocolLogEntry) -> bool {
        match self {
            ProtocolLogFilter::All => true,
            ProtocolLogFilter::Fetch => {
                matches!(entry.api_type, ApiType::Fetch | ApiType::ListOffset)
            }
            ProtocolLogFilter::Metadata => matches!(entry.api_type, ApiType::Metadata),
            ProtocolLogFilter::Groups => matches!(
                entry.api_type,
                ApiType::ListGroups
                    | ApiType::DescribeGroups
                    | ApiType::FindCoordinator
                    | ApiType::OffsetFetch
            ),
            ProtocolLogFilter::Errors => matches!(entry.status, LogStatus::Error),
        }
    }
}
