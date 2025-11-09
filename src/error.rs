use std::{error::Error, fmt, io};

#[derive(Debug)]
pub enum KafkaError {
    Io(io::Error),
    ConnectionFailed(String),
    ProtocolError(String),
    InvalidResponse { expected: String, actual: String },
    DecodeError(String),
    InsufficientData { needed: usize, available: usize },
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KafkaError::Io(err) => write!(f, "IO error: {}", err),
            KafkaError::ConnectionFailed(broker) => {
                write!(f, "Connection failed to {}", broker)
            }
            KafkaError::ProtocolError(msg) => write!(f, "Protocol error: {}", msg),
            KafkaError::InvalidResponse { expected, actual } => {
                write!(f, "Invalid response: expected {}, got {}", expected, actual)
            }
            KafkaError::DecodeError(msg) => write!(f, "Decode error: {}", msg),
            KafkaError::InsufficientData { needed, available } => {
                write!(
                    f,
                    "Not enough data: need {} bytes, have {}",
                    needed, available
                )
            }
        }
    }
}

impl Error for KafkaError {}

impl From<io::Error> for KafkaError {
    fn from(value: io::Error) -> Self {
        KafkaError::Io(value)
    }
}
pub type Result<T> = std::result::Result<T, KafkaError>;
