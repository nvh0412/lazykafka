use std::borrow::Cow;

use crate::{
    error::{KafkaError, Result},
    protocol::{compression, message_decoder::MessageDecoder, record::Record},
};

/// Bit 5 of `attributes`: the batch is a transaction control batch (commit/abort marker).
const CONTROL_BATCH_FLAG: i16 = 0x20;

#[allow(dead_code)]
pub struct RecordBatch {
    pub attributes: i16,
    pub base_offset: i64,
    pub base_sequence: i32,
    pub batch_length: i32,
    pub crc: u32,
    pub first_timestamp: i64,
    pub last_offset_delta: i32,
    pub magic: i8,
    pub max_timestamp: i64,
    pub partition_leader_epoch: i32,
    pub producer_epoch: i16,
    pub producer_id: i64,
    pub records: Vec<Record>,
    /// Set when the batch was recognised but its records could not be shown
    /// (e.g. legacy magic version, or decompression failed).
    pub skip_reason: Option<String>,
}

impl RecordBatch {
    pub fn decode(data: &[u8]) -> Result<RecordBatch> {
        let mut decoder = MessageDecoder::new(data);
        let base_offset = decoder.read_i64()?;
        let batch_length = decoder.read_i32()?;

        // batch_length covers from partition_leader_epoch onwards.
        // Limit parsing to those bytes so we never read into the next batch.
        // A truncated trailing batch (broker cut the response at max_bytes) is normal,
        // so this must be an error, never a panic.
        let batch_len = usize::try_from(batch_length).map_err(|_| {
            KafkaError::DecodeError(format!("negative batch length {}", batch_length))
        })?;
        let batch_data = decoder.read_slice(batch_len)?;
        let mut batch_decoder = MessageDecoder::new(batch_data);

        let partition_leader_epoch = batch_decoder.read_i32()?;
        let magic = batch_decoder.read_i8()?;

        if magic != 2 {
            // Legacy message sets (magic 0/1) have a different layout; don't misparse them.
            return Ok(Self {
                base_offset,
                batch_length,
                partition_leader_epoch,
                magic,
                crc: 0,
                attributes: 0,
                last_offset_delta: 0,
                first_timestamp: 0,
                max_timestamp: 0,
                producer_id: -1,
                producer_epoch: -1,
                base_sequence: -1,
                records: Vec::new(),
                skip_reason: Some(format!("unsupported record format (magic {})", magic)),
            });
        }

        let crc = batch_decoder.read_i32()? as u32;
        let attributes = batch_decoder.read_i16()?;
        let last_offset_delta = batch_decoder.read_i32()?;
        let first_timestamp = batch_decoder.read_i64()?;
        let max_timestamp = batch_decoder.read_i64()?;
        let producer_id = batch_decoder.read_i64()?;
        let producer_epoch = batch_decoder.read_i16()?;
        let base_sequence = batch_decoder.read_i32()?;
        let record_count = batch_decoder.read_i32()?;

        let record_count = usize::try_from(record_count).map_err(|_| {
            KafkaError::DecodeError(format!("negative record count {}", record_count))
        })?;

        let compression_type = attributes & 0x07;
        let is_control = attributes & CONTROL_BATCH_FLAG != 0;
        let records_bytes = batch_decoder.remaining_slice();

        let mut skip_reason = None;
        let mut records = Vec::new();

        if is_control {
            // Transaction markers carry no user data.
        } else if records_bytes.is_empty() && record_count == 0 {
            // Nothing to decode (and nothing to decompress).
        } else {
            // With compression, everything after record_count is a single compressed blob
            // holding all records; decompress it, then parse as usual.
            let payload: Result<Cow<[u8]>> = if compression_type == 0 {
                Ok(Cow::Borrowed(records_bytes))
            } else {
                compression::decompress(compression_type, records_bytes).map(Cow::Owned)
            };

            match payload {
                Ok(payload) => records = Self::decode_records(&payload, record_count)?,
                Err(e) => {
                    skip_reason = Some(format!(
                        "{} batch could not be decompressed: {}",
                        compression::codec_name(compression_type),
                        e
                    ));
                }
            }
        }

        Ok(Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            first_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
            skip_reason,
        })
    }

    fn decode_records(payload: &[u8], record_count: usize) -> Result<Vec<Record>> {
        let mut decoder = MessageDecoder::new(payload);
        // Every record takes at least one byte, so remaining() bounds a sane capacity.
        let mut records = Vec::with_capacity(record_count.min(decoder.remaining()));

        for _ in 0..record_count {
            // Each record is prefixed with a zigzag varint length.
            let record_length = decoder.read_varint_zigzag()?;
            let len = usize::try_from(record_length).map_err(|_| {
                KafkaError::DecodeError(format!("negative record length {}", record_length))
            })?;

            // Bounds-checked slice of exactly this record's bytes (advances the decoder).
            let record_bytes = decoder.read_slice(len)?;
            let mut record_decoder = MessageDecoder::new(record_bytes);
            let record = Record::decode_from_batch(&mut record_decoder, record_length)?;
            records.push(record);
        }

        Ok(records)
    }
}

#[cfg(test)]
mod record_batch_tests {
    use super::*;

    fn create_minimal_record_bytes() -> Vec<u8> {
        // Minimal record: content is 6 bytes (attrs + timestamp_delta + offset_delta + key_len + value_len + headers_count)
        // Length uses zigzag encoding: zigzag(6) = 12 = 0x0C
        vec![
            0x0C, // length (varint zigzag: 6)
            0x00, // attributes (i8: 0)
            0x00, // timestamp_delta (varint zigzag: 0)
            0x00, // offset_delta (varint: 0)
            0x00, // key_length (varint: 0, means empty)
            0x00, // value_length (varint: 0, means empty)
            0x00, // headers_count (varint: 0)
        ]
    }

    #[test]
    fn test_record_batch_decode_minimal_no_records() {
        // batch_length = 49 bytes (0x31): partition_leader_epoch(4) + magic(1) + crc(4) +
        // attributes(2) + last_offset_delta(4) + first_timestamp(8) + max_timestamp(8) +
        // producer_id(8) + producer_epoch(2) + base_sequence(4) + record_count(4) = 49
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // batch_length (i32) = 49
            0x00, 0x00, 0x00, 0x31, // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00, // magic (i8)
            0x02, // crc (i32/u32)
            0x12, 0x34, 0x56, 0x78, // attributes (i16)
            0x00, 0x00, // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00, // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // producer_epoch (i16)
            0xFF, 0xFF, // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF, // record_count (i32) = 0 records
            0x00, 0x00, 0x00, 0x00,
        ];

        let result = RecordBatch::decode(&data);
        assert!(result.is_ok());
        let batch = result.unwrap();

        assert_eq!(batch.base_offset, 0);
        assert_eq!(batch.batch_length, 49);
        assert_eq!(batch.partition_leader_epoch, 0);
        assert_eq!(batch.magic, 2);
        assert_eq!(batch.crc, 0x12345678);
        assert_eq!(batch.attributes, 0);
        assert_eq!(batch.last_offset_delta, 0);
        assert_eq!(batch.first_timestamp, 0);
        assert_eq!(batch.max_timestamp, 0);
        assert_eq!(batch.producer_id, -1);
        assert_eq!(batch.producer_epoch, -1);
        assert_eq!(batch.base_sequence, -1);
        assert_eq!(batch.records.len(), 0);
    }

    #[test]
    fn test_record_batch_decode_with_single_record() {
        // batch_length = 49 (header) + 7 (minimal record bytes) = 56 (0x38)
        let mut data = vec![
            // base_offset (i64) = 100
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // batch_length (i32) = 56
            0x00, 0x00, 0x00, 0x38, // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x01, // magic (i8)
            0x02, // crc (i32/u32)
            0xAB, 0xCD, 0xEF, 0x01, // attributes (i16) = 8 (some transactional flags)
            0x00, 0x08, // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00, // first_timestamp (i64)
            0x00, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x00, 0x00, // max_timestamp (i64)
            0x00, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x00, 0x01, // producer_id (i64) = 10
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, // producer_epoch (i16) = 5
            0x00, 0x05, // base_sequence (i32) = 20
            0x00, 0x00, 0x00, 0x14, // record_count (i32) = 1 record
            0x00, 0x00, 0x00, 0x01,
        ];

        // Append a minimal record
        data.extend(create_minimal_record_bytes());

        let result = RecordBatch::decode(&data);
        assert!(result.is_ok());
        let batch = result.unwrap();

        assert_eq!(batch.base_offset, 100);
        assert_eq!(batch.partition_leader_epoch, 1);
        assert_eq!(batch.magic, 2);
        assert_eq!(batch.attributes, 8);
        assert_eq!(batch.producer_id, 10);
        assert_eq!(batch.producer_epoch, 5);
        assert_eq!(batch.base_sequence, 20);
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].length, 6);
    }

    #[test]
    fn test_record_batch_decode_with_multiple_records() {
        // batch_length = 49 (header) + 3 * 7 (records) = 70 (0x46)
        let mut data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // batch_length (i32) = 70
            0x00, 0x00, 0x00, 0x46, // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00, // magic (i8)
            0x02, // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00, // attributes (i16)
            0x00, 0x00, // last_offset_delta (i32) = 2 (0-indexed for 3 records)
            0x00, 0x00, 0x00, 0x02, // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // producer_epoch (i16)
            0xFF, 0xFF, // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF, // record_count (i32) = 3 records
            0x00, 0x00, 0x00, 0x03,
        ];

        // Append 3 minimal records
        for _ in 0..3 {
            data.extend(create_minimal_record_bytes());
        }

        let result = RecordBatch::decode(&data);
        assert!(result.is_ok());
        let batch = result.unwrap();

        assert_eq!(batch.records.len(), 3);
        assert_eq!(batch.last_offset_delta, 2);
        for record in &batch.records {
            assert_eq!(record.length, 6);
        }
    }

    #[test]
    fn test_record_batch_decode_negative_record_count() {
        // A negative record_count must be rejected with an error, not panic
        // batch_length = 49 bytes (header only)
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // batch_length (i32) = 49
            0x00, 0x00, 0x00, 0x31, // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00, // magic (i8)
            0x02, // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00, // attributes (i16)
            0x00, 0x00, // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00, // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // producer_epoch (i16)
            0xFF, 0xFF, // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF, // record_count (i32) = -1 (negative value)
            0xFF, 0xFF, 0xFF, 0xFF,
        ];

        assert!(RecordBatch::decode(&data).is_err());
    }

    #[test]
    fn test_record_batch_decode_excessive_record_count() {
        // Test for Issue #1 - excessive record_count
        // batch_length = 49 bytes (header only)
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // batch_length (i32) = 49
            0x00, 0x00, 0x00, 0x31, // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00, // magic (i8)
            0x02, // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00, // attributes (i16)
            0x00, 0x00, // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00, // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // producer_epoch (i16)
            0xFF, 0xFF, // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF, // record_count (i32) = 1,000,000 (very large value)
            0x00, 0x0F, 0x42, 0x40,
        ];

        let result = RecordBatch::decode(&data);
        // Should fail with InsufficientData when trying to read 1M records
        assert!(result.is_err());
    }

    #[test]
    fn test_record_batch_decode_magic_byte_values() {
        // Test different magic byte values
        // batch_length = 49 bytes (header only)
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // batch_length (i32) = 49
            0x00, 0x00, 0x00, 0x31, // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00, // magic (i8) = 1 (old format)
            0x01, // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00, // attributes (i16)
            0x00, 0x00, // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00, // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // producer_epoch (i16)
            0xFF, 0xFF, // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF, // record_count (i32) = 0 records
            0x00, 0x00, 0x00, 0x00,
        ];

        let result = RecordBatch::decode(&data);
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.magic, 1);
        // Legacy magic is recognised but skipped, with a reason
        assert!(batch.records.is_empty());
        assert!(batch.skip_reason.is_some());
    }

    #[test]
    fn test_record_batch_decode_attributes_compression() {
        // Test attributes with compression flags
        // batch_length = 49 bytes (header only)
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // batch_length (i32) = 49
            0x00, 0x00, 0x00, 0x31, // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00, // magic (i8)
            0x02, // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00, // attributes (i16) = 1 (gzip compression)
            0x00, 0x01, // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00, // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // producer_epoch (i16)
            0xFF, 0xFF, // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF, // record_count (i32) = 0 records
            0x00, 0x00, 0x00, 0x00,
        ];

        let result = RecordBatch::decode(&data);
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.attributes, 1);
        // Note: Compression is not handled, this just tests that the attribute is read correctly
        let compression_type = batch.attributes & 0x07;
        assert_eq!(compression_type, 1); // gzip
    }

    #[test]
    fn test_record_batch_decode_timestamps() {
        // Test with realistic timestamp values
        // batch_length = 49 bytes (header only)
        // 1609459200000 in bytes: 0x00, 0x00, 0x01, 0x76, 0xBB, 0x3E, 0x70, 0x00
        let timestamp: i64 = 1609459200000; // 2021-01-01 00:00:00 UTC
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // batch_length (i32) = 49
            0x00, 0x00, 0x00, 0x31, // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00, // magic (i8)
            0x02, // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00, // attributes (i16)
            0x00, 0x00, // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00, // first_timestamp (i64) = 1609459200000
            0x00, 0x00, 0x01, 0x76, 0xBB, 0x3E, 0x70, 0x00,
            // max_timestamp (i64) = 1609459200000
            0x00, 0x00, 0x01, 0x76, 0xBB, 0x3E, 0x70, 0x00, // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // producer_epoch (i16)
            0xFF, 0xFF, // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF, // record_count (i32) = 0 records
            0x00, 0x00, 0x00, 0x00,
        ];

        let result = RecordBatch::decode(&data);
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.first_timestamp, timestamp);
        assert_eq!(batch.max_timestamp, timestamp);
    }

    #[test]
    fn test_record_batch_decode_insufficient_data() {
        // Incomplete record batch data
        // batch_length is set to match the available data (5 bytes), but the decoder
        // needs more bytes to parse the complete batch header, so it will fail
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // batch_length (i32) = 5 (matches available data after this field)
            0x00, 0x00, 0x00, 0x05, // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00,
            // magic (i8)
            0x02,
            // Missing rest of fields - decoder will fail trying to read crc, attributes, etc.
        ];

        let result = RecordBatch::decode(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_record_batch_decode_with_record_with_data() {
        // Record content is 14 bytes: attrs(1) + ts_delta(1) + offset(1) + key_len(1) + key(3) + val_len(1) + value(5) + headers(1)
        // All lengths use zigzag encoding:
        //   record_length = 14 -> zigzag(14) = 28 = 0x1C
        //   key_length = 3 -> zigzag(3) = 6 = 0x06
        //   value_length = 5 -> zigzag(5) = 10 = 0x0A
        //   headers_count = 0 -> zigzag(0) = 0 = 0x00
        // batch_length = 49 (header) + 1 (length varint) + 14 (record content) = 64 (0x40)
        let mut data = vec![
            // Record batch header
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // base_offset
            0x00, 0x00, 0x00, 0x40, // batch_length = 64
            0x00, 0x00, 0x00, 0x00, // partition_leader_epoch
            0x02, // magic
            0x00, 0x00, 0x00, 0x00, // crc
            0x00, 0x00, // attributes
            0x00, 0x00, 0x00, 0x00, // last_offset_delta
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // first_timestamp
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // max_timestamp
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // producer_id
            0xFF, 0xFF, // producer_epoch
            0xFF, 0xFF, 0xFF, 0xFF, // base_sequence
            0x00, 0x00, 0x00, 0x01, // record_count: 1
        ];

        // Add a record with actual key and value
        data.extend(vec![
            0x1C, // length (varint zigzag: 14) - zigzag(14) = 28 = 0x1C
            0x00, // attributes
            0x00, // timestamp_delta (varint zigzag: 0)
            0x00, // offset_delta (varint: 0)
            // key
            0x06, // key_length (varint zigzag: 3) - zigzag(3) = 6 = 0x06
            b'k', b'e', b'y', // key: "key"
            // value
            0x0A, // value_length (varint zigzag: 5) - zigzag(5) = 10 = 0x0A
            b'v', b'a', b'l', b'u', b'e', // value: "value"
            0x00, // headers_count (varint zigzag: 0)
        ]);

        let result = RecordBatch::decode(&data);
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].key, Some(b"key".to_vec()));
        assert_eq!(batch.records[0].value, b"value".to_vec());
    }

    // ---- compressed batch tests ----

    /// Encode a record (length-prefixed, v2 layout) with the given key/value.
    fn encode_record(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut body = vec![0x00, 0x00, 0x00]; // attributes, ts_delta, offset_delta
        body.push((key.len() as u8) << 1);
        body.extend_from_slice(key);
        body.push((value.len() as u8) << 1);
        body.extend_from_slice(value);
        body.push(0x00); // headers_count
        let mut out = vec![(body.len() as u8) << 1];
        out.extend(body);
        out
    }

    fn records_blob() -> Vec<u8> {
        let mut blob = encode_record(b"k1", b"first value");
        blob.extend(encode_record(b"k2", b"second value"));
        blob
    }

    /// Build a full v2 batch whose records section is `payload`.
    fn build_batch(attributes: i16, record_count: i32, payload: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&0i32.to_be_bytes()); // partition_leader_epoch
        body.push(2); // magic
        body.extend_from_slice(&0i32.to_be_bytes()); // crc
        body.extend_from_slice(&attributes.to_be_bytes());
        body.extend_from_slice(&(record_count - 1).to_be_bytes()); // last_offset_delta
        body.extend_from_slice(&0i64.to_be_bytes()); // first_timestamp
        body.extend_from_slice(&0i64.to_be_bytes()); // max_timestamp
        body.extend_from_slice(&(-1i64).to_be_bytes()); // producer_id
        body.extend_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
        body.extend_from_slice(&(-1i32).to_be_bytes()); // base_sequence
        body.extend_from_slice(&record_count.to_be_bytes());
        body.extend_from_slice(payload);

        let mut out = Vec::new();
        out.extend_from_slice(&0i64.to_be_bytes()); // base_offset
        out.extend_from_slice(&(body.len() as i32).to_be_bytes());
        out.extend(body);
        out
    }

    fn assert_two_records(batch: &RecordBatch) {
        assert!(batch.skip_reason.is_none(), "{:?}", batch.skip_reason);
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].key, Some(b"k1".to_vec()));
        assert_eq!(batch.records[0].value, b"first value".to_vec());
        assert_eq!(batch.records[1].key, Some(b"k2".to_vec()));
        assert_eq!(batch.records[1].value, b"second value".to_vec());
    }

    fn all_codec_batches() -> Vec<(&'static str, Vec<u8>)> {
        use std::io::Write;
        let blob = records_blob();

        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&blob).unwrap();
        let gz = gz.finish().unwrap();

        let snappy_raw = snap::raw::Encoder::new().compress_vec(&blob).unwrap();

        let mut xerial = b"\x82SNAPPY\0".to_vec();
        xerial.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
        xerial.extend_from_slice(&(snappy_raw.len() as i32).to_be_bytes());
        xerial.extend_from_slice(&snappy_raw);

        let mut lz4 = lz4_flex::frame::FrameEncoder::new(Vec::new());
        lz4.write_all(&blob).unwrap();
        let lz4 = lz4.finish().unwrap();

        // zstd frame with a single raw block (hand-built, no encoder needed)
        let mut zstd = Vec::new();
        zstd.extend_from_slice(&[0x28, 0xB5, 0x2F, 0xFD, 0x20, blob.len() as u8]);
        let header = ((blob.len() as u32) << 3) | 1;
        zstd.extend_from_slice(&header.to_le_bytes()[..3]);
        zstd.extend_from_slice(&blob);

        vec![
            ("none", build_batch(0, 2, &blob)),
            ("gzip", build_batch(1, 2, &gz)),
            ("snappy-raw", build_batch(2, 2, &snappy_raw)),
            ("snappy-xerial", build_batch(2, 2, &xerial)),
            ("lz4", build_batch(3, 2, &lz4)),
            ("zstd", build_batch(4, 2, &zstd)),
        ]
    }

    #[test]
    fn test_record_batch_decode_all_codecs() {
        for (name, data) in all_codec_batches() {
            let batch = RecordBatch::decode(&data)
                .unwrap_or_else(|e| panic!("{} failed to decode: {}", name, e));
            assert_two_records(&batch);
        }
    }

    #[test]
    fn test_record_batch_corrupt_compressed_payload_is_skipped_not_panic() {
        // Regression: a snappy batch used to panic at record_batch.rs:72.
        let data = build_batch(2, 2, &[0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03]);
        let batch = RecordBatch::decode(&data).unwrap();
        assert!(batch.records.is_empty());
        assert!(batch.skip_reason.unwrap().contains("snappy"));
    }

    #[test]
    fn test_record_batch_control_batch_has_no_records() {
        let data = build_batch(0x20, 1, &[0x00, 0x01, 0x02]);
        let batch = RecordBatch::decode(&data).unwrap();
        assert!(batch.records.is_empty());
        assert!(batch.skip_reason.is_none());
    }

    #[test]
    fn test_record_batch_negative_batch_length_errors() {
        let mut data = build_batch(0, 2, &records_blob());
        data[8..12].copy_from_slice(&(-5i32).to_be_bytes());
        assert!(RecordBatch::decode(&data).is_err());
    }

    #[test]
    fn test_record_batch_batch_length_past_end_errors() {
        let mut data = build_batch(0, 2, &records_blob());
        data[8..12].copy_from_slice(&i32::MAX.to_be_bytes());
        assert!(RecordBatch::decode(&data).is_err());
    }

    #[test]
    fn test_record_batch_negative_record_length_errors() {
        // zigzag(-1) = 1
        let data = build_batch(0, 1, &[0x01, 0x00, 0x00]);
        assert!(RecordBatch::decode(&data).is_err());
    }

    #[test]
    fn test_record_batch_truncated_and_mutated_never_panics() {
        for (_, data) in all_codec_batches() {
            // every truncation
            for n in 0..data.len() {
                let _ = RecordBatch::decode(&data[..n]);
            }
            // deterministic byte-flip fuzz
            let mut seed: u64 = 0x2545F4914F6CDD1D;
            for _ in 0..2000 {
                let mut mutated = data.clone();
                for _ in 0..3 {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    let i = (seed as usize) % mutated.len();
                    mutated[i] = (seed >> 32) as u8;
                }
                let _ = RecordBatch::decode(&mutated);
            }
        }
    }
}
