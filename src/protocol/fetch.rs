use crate::{
    error::Result,
    protocol::{
        message_buffer::MessageBuffer, message_decoder::MessageDecoder, record_batch::RecordBatch,
        request_header::RequestHeader,
    },
};

pub struct FetchRequest {
    pub correlation_id: i32,
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub topics: Vec<FetchTopic>,
}

pub struct FetchTopic {
    pub topic: String,
    pub partitions: Vec<FetchPartition>,
}

impl FetchTopic {
    pub fn new(topic: String) -> Self {
        Self {
            topic,
            partitions: vec![],
        }
    }

    pub fn add_partition(&mut self, partition: FetchPartition) {
        self.partitions.push(partition);
    }
}

pub struct FetchPartition {
    pub partition: i32,
    pub fetch_offset: i64,
    pub partition_max_bytes: i32,
}

impl FetchRequest {
    pub fn new(
        correlation_id: i32,
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        topics: Vec<FetchTopic>,
    ) -> Self {
        Self {
            correlation_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            topics,
            replica_id,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = MessageBuffer::with_capacity(4000);

        buf.reserve_size_header();

        let header = RequestHeader {
            api_key: 1,
            api_version: 4,
            correlation_id: self.correlation_id,
            client_id: Some("kafka-rust-client".to_string()),
        };

        header.encode(&mut buf);

        buf.put_i32(self.replica_id);
        buf.put_i32(self.max_wait_ms);
        buf.put_i32(self.min_bytes);
        buf.put_i32(self.max_bytes);
        buf.put_i8(self.isolation_level);

        buf.put_i32(self.topics.len() as i32);

        for topic_request in self.topics.iter() {
            buf.put_string(topic_request.topic.as_str());

            buf.put_i32(topic_request.partitions.len() as i32);

            for partition_request in topic_request.partitions.iter() {
                buf.put_i32(partition_request.partition);
                buf.put_i64(partition_request.fetch_offset);
                buf.put_i32(partition_request.partition_max_bytes);
            }
        }

        buf.write_size_header();
        buf.into_bytes()
    }
}

pub struct FetchResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub responses: Vec<FetchTopicResponse>,
}

pub struct FetchTopicResponse {
    pub topic: String,
    pub paritions: Vec<FetchPartitionResponse>,
}

pub struct FetchPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub aborted_transactions: Vec<AbortedTransaction>,
    pub records: Vec<RecordBatch>,
}

pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

impl FetchResponse {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        let correlation_id = decoder.read_i32()?;

        let throttle_time_ms = decoder.read_i32()?;

        let response_count = decoder.read_i32()?;

        let mut responses: Vec<FetchTopicResponse> = Vec::with_capacity(response_count as usize);

        for _ in 0..response_count {
            let topic_name = decoder.read_string()?;

            let parition_count = decoder.read_i32()?;

            let mut paritions: Vec<FetchPartitionResponse> =
                Vec::with_capacity(parition_count as usize);

            for _ in 0..parition_count {
                let partition_index = decoder.read_i32()?;
                let error_code = decoder.read_i16()?;
                let high_watermark = decoder.read_i64()?;
                let last_stable_offset = decoder.read_i64()?;

                let transaction_count = decoder.read_i32()?;
                let mut aborted_transactions;

                if transaction_count > 0 {
                    aborted_transactions = Vec::with_capacity(transaction_count as usize);

                    for _ in 0..transaction_count {
                        let producer_id = decoder.read_i64()?;
                        let first_offset = decoder.read_i64()?;
                        aborted_transactions.push(AbortedTransaction {
                            producer_id,
                            first_offset,
                        });
                    }
                } else {
                    aborted_transactions = vec![];
                }

                // Decode all RecordBatches from the records bytes
                let records_bytes = decoder.read_bytes()?.unwrap_or(vec![]);
                let mut record_batches = Vec::new();

                if !records_bytes.is_empty() {
                    let mut batch_decoder = MessageDecoder::new(&records_bytes);

                    // Keep decoding batches until we've consumed all the bytes
                    while batch_decoder.remaining() > 0 {
                        // Each batch starts with base_offset (8 bytes) + batch_length (4 bytes)
                        // We need at least 12 bytes to read these fields
                        if batch_decoder.remaining() < 12 {
                            break; // Not enough data for another batch
                        }

                        let start_pos = batch_decoder.position();

                        // Try to decode a batch
                        let remaining_data = &records_bytes[start_pos..];
                        match RecordBatch::decode(remaining_data) {
                            Ok(batch) => {
                                // Calculate how many bytes this batch consumed
                                // batch_length + 8 (base_offset) + 4 (batch_length itself)
                                let consumed = 12 + batch.batch_length as usize;

                                // Advance the decoder position
                                let _ = batch_decoder.read_bytes_from_length(consumed as i64);

                                record_batches.push(batch);
                            }
                            Err(_) => {
                                // If we can't decode a batch, stop trying
                                break;
                            }
                        }
                    }
                }

                paritions.push(FetchPartitionResponse {
                    partition_index,
                    error_code,
                    high_watermark,
                    last_stable_offset,
                    aborted_transactions,
                    records: record_batches,
                })
            }

            responses.push(FetchTopicResponse {
                topic: topic_name,
                paritions: paritions,
            });
        }

        Ok(Self {
            correlation_id,
            throttle_time_ms,
            responses: responses,
        })
    }
}
