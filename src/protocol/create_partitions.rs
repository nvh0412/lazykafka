use crate::{
    error::Result,
    protocol::{
        message_buffer::MessageBuffer, message_decoder::MessageDecoder,
        request_header::RequestHeader,
    },
};

/// Request to create additional partitions for topics (API Key: 37, Version: 0)
pub struct CreatePartitionsRequest {
    pub correlation_id: i32,
    pub topics: Vec<CreatePartitionsTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

pub struct CreatePartitionsTopic {
    pub topic: String,
    pub count: i32,                         // New TOTAL partition count
    pub assignments: Option<Vec<Vec<i32>>>, // Optional broker assignments for new partitions
}

impl CreatePartitionsRequest {
    pub fn new(
        correlation_id: i32,
        topic: String,
        new_partition_count: i32,
        timeout_ms: i32,
    ) -> Self {
        Self {
            correlation_id,
            topics: vec![CreatePartitionsTopic {
                topic,
                count: new_partition_count,
                assignments: None, // Let Kafka choose the assignments
            }],
            timeout_ms,
            validate_only: false,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(256);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 37, // CreatePartitions API
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };

        header.encode(&mut buf);

        // Encode topics array
        buf.put_i32(self.topics.len() as i32);
        for topic in &self.topics {
            buf.put_string(&topic.topic);

            // NewPartitions structure
            buf.put_i32(topic.count);

            // Assignments array (nullable)
            match &topic.assignments {
                Some(assignments) => {
                    buf.put_i32(assignments.len() as i32);
                    for assignment in assignments {
                        buf.put_i32(assignment.len() as i32);
                        for broker_id in assignment {
                            buf.put_i32(*broker_id);
                        }
                    }
                }
                None => {
                    buf.put_i32(-1); // null array
                }
            }
        }

        // Timeout
        buf.put_i32(self.timeout_ms);

        // Validate only
        buf.put_bool(self.validate_only);

        buf.write_size_header();
        buf.into_bytes()
    }
}

/// Response from CreatePartitions request
pub struct CreatePartitionsResponse {
    pub correlation_id: i32,
    pub results: Vec<CreatePartitionsResult>,
}

pub struct CreatePartitionsResult {
    pub error_code: i16,
    pub error_message: Option<String>,
}

impl CreatePartitionsResult {
    /// Check if this result indicates success
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Get a human-readable error description
    pub fn error_description(&self) -> Option<String> {
        if self.error_code == 0 {
            return None;
        }

        let desc = match self.error_code {
            37 => "INVALID_PARTITIONS: New partition count must be greater than current count",
            39 => {
                "INVALID_REPLICA_ASSIGNMENT: Assignment list size doesn't match replication factor"
            }
            42 => "INVALID_REQUEST: Invalid request format or duplicate topics",
            _ => "Unknown error",
        };

        Some(format!(
            "{} (code: {}): {}",
            desc,
            self.error_code,
            self.error_message.as_deref().unwrap_or("No details")
        ))
    }
}

impl CreatePartitionsResponse {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;
        let _throttle_time_ms = decoder.read_i32()?;

        // Read results array
        let result_count = decoder.read_i32()? as usize;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let _topic = decoder.read_string()?;
            let error_code = decoder.read_i16()?;
            let error_message = decoder.read_nullable_string()?;

            results.push(CreatePartitionsResult {
                error_code,
                error_message,
            });
        }

        Ok(Self {
            correlation_id,
            results,
        })
    }

    /// Get the first error from results, if any
    pub fn first_error(&self) -> Option<&CreatePartitionsResult> {
        self.results.iter().find(|r| !r.is_success())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_request() {
        let request = CreatePartitionsRequest::new(1, "test-topic".to_string(), 6, 30000);
        let encoded = request.encode();

        // Should not be empty
        assert!(!encoded.is_empty());

        // First 4 bytes are size header
        let size = i32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(size as usize, encoded.len() - 4);
    }

    #[test]
    fn test_decode_response_success() {
        // Simulate a successful response
        let mut buf = MessageBuffer::new();
        buf.put_i32(1); // correlation_id
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(1); // results count
        buf.put_string("test-topic");
        buf.put_i16(0); // error_code = success
        buf.put_i16(-1); // error_message = null (nullable string with -1)

        let data = buf.into_bytes();
        let response = CreatePartitionsResponse::decode(&data).unwrap();

        assert_eq!(response.correlation_id, 1);
        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].is_success());
        assert!(response.first_error().is_none());
    }

    #[test]
    fn test_decode_response_error() {
        let mut buf = MessageBuffer::new();
        buf.put_i32(1); // correlation_id
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(1); // results count
        buf.put_string("test-topic");
        buf.put_i16(37); // error_code = INVALID_PARTITIONS
        buf.put_string("Partition count must increase"); // error_message

        let data = buf.into_bytes();
        let response = CreatePartitionsResponse::decode(&data).unwrap();

        assert!(!response.results[0].is_success());
        assert!(response.first_error().is_some());
        assert!(
            response.results[0]
                .error_description()
                .unwrap()
                .contains("INVALID_PARTITIONS")
        );
    }
}
