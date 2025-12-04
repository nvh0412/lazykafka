use crate::{
    error::Result,
    protocol::{ProtocolResponse, message_decoder::MessageDecoder, record::Record},
};

pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl ProtocolResponse<RecordBatch> for RecordBatch {
    fn decode(data: &[u8]) -> Result<RecordBatch> {
        let mut decoder = MessageDecoder::new(data);
        let base_offset = decoder.read_i64()?;
        let batch_length = decoder.read_i32()?;
        let partition_leader_epoch = decoder.read_i32()?;
        let magic = decoder.read_i8()?;
        let crc = decoder.read_i32()? as u32;
        let attributes = decoder.read_i16()?;
        let last_offset_delta = decoder.read_i32()?;
        let first_timestamp = decoder.read_i64()?;
        let max_timestamp = decoder.read_i64()?;
        let producer_id = decoder.read_i64()?;
        let producer_epoch = decoder.read_i16()?;
        let base_sequence = decoder.read_i32()?;

        let record_count = decoder.read_i32()?;
        // let compression_type = attributes & 0x07;

        let mut records = Vec::with_capacity(record_count as usize);
        for _ in 0..record_count {
            records.push(Record::decode_from_decoder(&mut decoder)?);
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
        })
    }
}

#[cfg(test)]
mod record_batch_tests {
    use super::*;

    fn create_minimal_record_bytes() -> Vec<u8> {
        // Minimal record: length=16, attrs=0, timestamp_delta=0, offset_delta=0,
        // key_length=0, value_length=0, headers_count=0
        vec![
            0x10, // length (varint: 16)
            0x00, // attributes (i8: 0)
            0x00, // timestamp_delta (varint zigzag: 0)
            0x00, // offset_delta (varint: 0)
            0x00, // key_length (varint: 0)
            0x00, // value_length (varint: 0)
            0x00, // headers_count (varint: 0)
        ]
    }

    #[test]
    fn test_record_batch_decode_minimal_no_records() {
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // batch_length (i32)
            0x00, 0x00, 0x00, 0x3D,
            // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00,
            // magic (i8)
            0x02,
            // crc (i32/u32)
            0x12, 0x34, 0x56, 0x78,
            // attributes (i16)
            0x00, 0x00,
            // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00,
            // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            // producer_epoch (i16)
            0xFF, 0xFF,
            // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF,
            // record_count (i32)
            0x00, 0x00, 0x00, 0x00, // 0 records
        ];

        let result = RecordBatch::decode(&data);
        assert!(result.is_ok());
        let batch = result.unwrap();

        assert_eq!(batch.base_offset, 0);
        assert_eq!(batch.batch_length, 61);
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
        let mut data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // 100
            // batch_length (i32)
            0x00, 0x00, 0x00, 0x50,
            // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x01,
            // magic (i8)
            0x02,
            // crc (i32/u32)
            0xAB, 0xCD, 0xEF, 0x01,
            // attributes (i16)
            0x00, 0x08, // Some compression/transactional flags
            // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00,
            // first_timestamp (i64)
            0x00, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x00, 0x00,
            // max_timestamp (i64)
            0x00, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x00, 0x01,
            // producer_id (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A,
            // producer_epoch (i16)
            0x00, 0x05,
            // base_sequence (i32)
            0x00, 0x00, 0x00, 0x14,
            // record_count (i32)
            0x00, 0x00, 0x00, 0x01, // 1 record
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
        assert_eq!(batch.records[0].length, 16);
    }

    #[test]
    fn test_record_batch_decode_with_multiple_records() {
        let mut data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // batch_length (i32)
            0x00, 0x00, 0x00, 0x50,
            // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00,
            // magic (i8)
            0x02,
            // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00,
            // attributes (i16)
            0x00, 0x00,
            // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x02, // 2 records (0-indexed)
            // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            // producer_epoch (i16)
            0xFF, 0xFF,
            // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF,
            // record_count (i32)
            0x00, 0x00, 0x00, 0x03, // 3 records
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
            assert_eq!(record.length, 16);
        }
    }

    #[test]
    #[should_panic(expected = "capacity overflow")]
    fn test_record_batch_decode_negative_record_count() {
        // Test for Issue #1 - negative record_count causes panic
        // This test documents the bug: negative record_count causes capacity overflow
        // when cast to usize, leading to a panic
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // batch_length (i32)
            0x00, 0x00, 0x00, 0x3D,
            // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00,
            // magic (i8)
            0x02,
            // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00,
            // attributes (i16)
            0x00, 0x00,
            // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00,
            // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            // producer_epoch (i16)
            0xFF, 0xFF,
            // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF,
            // record_count (i32) - negative value
            0xFF, 0xFF, 0xFF, 0xFF, // -1
        ];

        let _result = RecordBatch::decode(&data);
        // This will panic with "capacity overflow" - documenting the bug
    }

    #[test]
    fn test_record_batch_decode_excessive_record_count() {
        // Test for Issue #1 - excessive record_count
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // batch_length (i32)
            0x00, 0x00, 0x00, 0x3D,
            // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00,
            // magic (i8)
            0x02,
            // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00,
            // attributes (i16)
            0x00, 0x00,
            // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00,
            // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            // producer_epoch (i16)
            0xFF, 0xFF,
            // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF,
            // record_count (i32) - very large value
            0x00, 0x0F, 0x42, 0x40, // 1,000,000
        ];

        let result = RecordBatch::decode(&data);
        // Should fail with InsufficientData when trying to read 1M records
        assert!(result.is_err());
    }

    #[test]
    fn test_record_batch_decode_magic_byte_values() {
        // Test different magic byte values
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // batch_length (i32)
            0x00, 0x00, 0x00, 0x3D,
            // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00,
            // magic (i8)
            0x01, // Magic byte 1 (old format)
            // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00,
            // attributes (i16)
            0x00, 0x00,
            // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00,
            // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            // producer_epoch (i16)
            0xFF, 0xFF,
            // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF,
            // record_count (i32)
            0x00, 0x00, 0x00, 0x00,
        ];

        let result = RecordBatch::decode(&data);
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.magic, 1);
        // Note: Magic byte is not validated, so any value is accepted
    }

    #[test]
    fn test_record_batch_decode_attributes_compression() {
        // Test attributes with compression flags
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // batch_length (i32)
            0x00, 0x00, 0x00, 0x3D,
            // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00,
            // magic (i8)
            0x02,
            // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00,
            // attributes (i16) - compression type 1 (gzip)
            0x00, 0x01,
            // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00,
            // first_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // max_timestamp (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            // producer_epoch (i16)
            0xFF, 0xFF,
            // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF,
            // record_count (i32)
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
        // Test with timestamp values
        let timestamp: i64 = 1000000000000; // 1000000000000 = 0xE8D4A51000
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // batch_length (i32)
            0x00, 0x00, 0x00, 0x3D,
            // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00,
            // magic (i8)
            0x02,
            // crc (i32/u32)
            0x00, 0x00, 0x00, 0x00,
            // attributes (i16)
            0x00, 0x00,
            // last_offset_delta (i32)
            0x00, 0x00, 0x00, 0x00,
            // first_timestamp (i64) - 0x000000E8D4A51000
            0x00, 0x00, 0x00, 0xE8, 0xD4, 0xA5, 0x10, 0x00,
            // max_timestamp (i64) - same
            0x00, 0x00, 0x00, 0xE8, 0xD4, 0xA5, 0x10, 0x00,
            // producer_id (i64)
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            // producer_epoch (i16)
            0xFF, 0xFF,
            // base_sequence (i32)
            0xFF, 0xFF, 0xFF, 0xFF,
            // record_count (i32)
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
        let data = vec![
            // base_offset (i64)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // batch_length (i32)
            0x00, 0x00, 0x00, 0x3D,
            // partition_leader_epoch (i32)
            0x00, 0x00, 0x00, 0x00,
            // magic (i8)
            0x02,
            // Missing rest of fields
        ];

        let result = RecordBatch::decode(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_record_batch_decode_with_record_with_data() {
        let mut data = vec![
            // Record batch header
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // base_offset
            0x00, 0x00, 0x00, 0x50, // batch_length
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
            0x20, // length (varint: 32)
            0x00, // attributes
            0x00, // timestamp_delta (varint zigzag: 0)
            0x00, // offset_delta (varint: 0)
            // key
            0x03,             // key_length (varint: 3)
            b'k', b'e', b'y', // key: "key"
            // value
            0x05,                         // value_length (varint: 5)
            b'v', b'a', b'l', b'u', b'e', // value: "value"
            0x00, // headers_count (varint: 0)
        ]);

        let result = RecordBatch::decode(&data);
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].key, Some(b"key".to_vec()));
        assert_eq!(batch.records[0].value, b"value".to_vec());
    }
}

