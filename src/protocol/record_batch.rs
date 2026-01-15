use crate::{
    error::Result,
    protocol::{message_decoder::MessageDecoder, record::Record},
};

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
}

impl RecordBatch {
    pub fn decode(data: &[u8]) -> Result<RecordBatch> {
        let mut decoder = MessageDecoder::new(data);
        let base_offset = decoder.read_i64()?;
        let batch_length = decoder.read_i32()?;

        // batch_length covers from partition_leader_epoch onwards
        // We need to extract only those bytes to avoid reading into next batch
        let batch_start_pos = decoder.position(); // position after base_offset and batch_length
        let batch_end_pos = batch_start_pos + batch_length as usize;

        // Create a new decoder limited to this batch's bytes only
        let batch_data = &data[batch_start_pos..batch_end_pos];
        let mut batch_decoder = MessageDecoder::new(batch_data);

        let partition_leader_epoch = batch_decoder.read_i32()?;
        let magic = batch_decoder.read_i8()?;
        let crc = batch_decoder.read_i32()? as u32;
        let attributes = batch_decoder.read_i16()?;
        let last_offset_delta = batch_decoder.read_i32()?;
        let first_timestamp = batch_decoder.read_i64()?;
        let max_timestamp = batch_decoder.read_i64()?;
        let producer_id = batch_decoder.read_i64()?;
        let producer_epoch = batch_decoder.read_i16()?;
        let base_sequence = batch_decoder.read_i32()?;

        let record_count = batch_decoder.read_i32()?;

        let compression_type = attributes & 0x07;

        if compression_type != 0 {
            eprintln!(
                "[WARNING] Compression detected but not yet supported! Type: {}",
                compression_type
            );
        }

        let mut records = Vec::with_capacity(record_count as usize);
        for _i in 0..record_count {
            // In Kafka batch format v2, each record is prefixed with a varint length
            // This length tells us how many bytes the record occupies
            // NOTE: Kafka varints use zigzag encoding even for lengths
            let record_length = batch_decoder.read_varint_zigzag()?;

            // Extract exactly record_length bytes for this record
            let record_start = batch_decoder.position();
            let record_end = record_start + record_length as usize;

            // Create a slice for just this record
            let record_bytes = &batch_data[record_start..record_end];

            // Decode the record from its isolated bytes
            // Note: We pass the length we already read, and decode from batch format
            let mut record_decoder = MessageDecoder::new(record_bytes);
            let record = Record::decode_from_batch(&mut record_decoder, record_length)?;

            // Advance the batch_decoder past this record's bytes
            // (Record::decode created its own decoder, so batch_decoder position hasn't moved)
            let _ = batch_decoder.read_bytes_from_length(record_length)?;

            records.push(record);
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
    #[should_panic(expected = "capacity overflow")]
    fn test_record_batch_decode_negative_record_count() {
        // Test for Issue #1 - negative record_count causes panic
        // This test documents the bug: negative record_count causes capacity overflow
        // when cast to usize, leading to a panic
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

        let _result = RecordBatch::decode(&data);
        // This will panic with "capacity overflow" - documenting the bug
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
        // Note: Magic byte is not validated, so any value is accepted
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
}
