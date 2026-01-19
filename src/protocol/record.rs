use crate::{
    error::{KafkaError, Result},
    protocol::{ProtocolResponse, message_decoder::MessageDecoder},
};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Record {
    pub length: i64, // varint
    pub attributes: i8,
    pub timestamp_delta: i64, // varint
    pub offset_delta: i64,    // varint
    pub key_length: i64,      // varint
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<Header>,
    pub partition: Option<i32>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Header {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

impl Record {
    pub fn decode_from_batch(decoder: &mut MessageDecoder, length: i64) -> Result<Self> {
        let attributes = decoder.read_i8()?;

        // timestamp_delta uses signed varint (zigzag encoding)
        let timestamp_delta = decoder.read_varint_zigzag()?;

        // offset_delta uses unsigned varint
        let offset_delta = decoder.read_varint()?;

        // In batch format v2, key_length uses signed varint (zigzag)
        // Negative value means null
        let key_length = decoder.read_varint_zigzag()?;
        let key = if key_length < 0 {
            None
        } else {
            decoder.read_bytes_from_length(key_length)?
        };

        // value_length also uses signed varint (zigzag)
        let value_length = decoder.read_varint_zigzag()?;
        let value: Vec<u8> = if value_length < 0 {
            vec![]
        } else {
            decoder
                .read_bytes_from_length(value_length)?
                .unwrap_or(vec![])
        };

        // headers_count uses signed varint (zigzag)
        let headers_count = decoder.read_varint_zigzag()?;

        if headers_count < 0 {
            return Err(KafkaError::DecodeError(format!(
                "Invalid headers count: {}",
                headers_count
            )));
        }

        // Prevent excessive memory allocation
        const MAX_HEADERS: i64 = 100_000;
        if headers_count > MAX_HEADERS {
            return Err(KafkaError::DecodeError(format!(
                "Headers count too large: {} (max: {})",
                headers_count, MAX_HEADERS
            )));
        }

        let mut headers = Vec::with_capacity(headers_count as usize);
        for _ in 0..headers_count {
            headers.push(Header::decode_from_batch(decoder)?);
        }

        Ok(Record {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key_length,
            key,
            value,
            headers,
            partition: None,
        })
    }

    pub fn decode_from_decoder(decoder: &mut MessageDecoder) -> Result<Self> {
        let length = decoder.read_varint()?;
        Self::decode_from_batch(decoder, length)
    }
}

impl ProtocolResponse<Record> for Record {
    fn decode(data: &[u8]) -> Result<Self> {
        let mut decoder = MessageDecoder::new(data);

        Record::decode_from_decoder(&mut decoder)
    }
}

impl Header {
    fn decode_from_batch(decoder: &mut MessageDecoder) -> Result<Header> {
        // Header key_length uses signed varint (zigzag encoding)
        let header_key_length = decoder.read_varint_zigzag()?;
        let key = if header_key_length < 0 {
            String::new()
        } else {
            String::from_utf8(
                decoder
                    .read_bytes_from_length(header_key_length)?
                    .unwrap_or(vec![]),
            )
            .map_err(|_| KafkaError::DecodeError(String::from("Invalid key header")))?
        };

        // Header value_length uses signed varint (zigzag encoding)
        let header_value_length = decoder.read_varint_zigzag()?;
        let value = if header_value_length < 0 {
            None
        } else {
            decoder.read_bytes_from_length(header_value_length)?
        };

        Ok(Header { key, value })
    }

    fn decode_from_decoder(decoder: &mut MessageDecoder) -> Result<Header> {
        let header_key_length = decoder.read_varint()?;
        let key = String::from_utf8(
            decoder
                .read_bytes_from_length(header_key_length)?
                .unwrap_or(vec![]),
        )
        .map_err(|_| KafkaError::DecodeError(String::from("Invalid key header")))?;

        let header_value_length = decoder.read_varint()?;
        let value = decoder.read_bytes_from_length(header_value_length)?;

        Ok(Header { key, value })
    }
}

impl ProtocolResponse<Header> for Header {
    fn decode(data: &[u8]) -> Result<Header> {
        let mut decoder = MessageDecoder::new(data);
        Header::decode_from_decoder(&mut decoder)
    }
}

#[cfg(test)]
mod record_tests {
    use super::*;

    #[test]
    fn test_header_decode_basic() {
        // Header with key "test" (4 bytes) and value [0x01, 0x02] (2 bytes)
        let data = vec![
            0x04, // key length (varint: 4)
            b't', b'e', b's', b't', // key: "test"
            0x02, // value length (varint: 2)
            0x01, 0x02, // value: [1, 2]
        ];

        let header = Header::decode(&data).unwrap();
        assert_eq!(header.key, "test");
        assert_eq!(header.value, Some(vec![0x01, 0x02]));
    }

    #[test]
    fn test_header_decode_null_value() {
        // Header with key "key" and empty value
        let data = vec![
            0x03, // key length (varint: 3)
            b'k', b'e', b'y', // key: "key"
            0x00, // value length (varint: 0 = empty)
        ];

        let header = Header::decode(&data).unwrap();
        assert_eq!(header.key, "key");
        assert_eq!(header.value, Some(vec![]));
    }

    #[test]
    fn test_header_decode_empty_key() {
        let data = vec![
            0x00, // key length (varint: 0)
            0x02, // value length (varint: 2)
            0xFF, 0xFE, // value
        ];

        let header = Header::decode(&data).unwrap();
        assert_eq!(header.key, "");
        assert_eq!(header.value, Some(vec![0xFF, 0xFE]));
    }

    #[test]
    fn test_header_decode_invalid_utf8() {
        let data = vec![
            0x02, // key length (varint: 2)
            0xFF, 0xFE, // Invalid UTF-8
            0x00, // value length (varint: 0)
        ];

        let result = Header::decode(&data);
        assert!(result.is_err());
        match result {
            Err(KafkaError::DecodeError(msg)) => {
                assert_eq!(msg, "Invalid key header");
            }
            _ => panic!("Expected DecodeError"),
        }
    }

    #[test]
    fn test_header_decode_insufficient_data() {
        let data = vec![
            0x05, // key length (varint: 5)
            b't', b'e', b's', // Only 3 bytes
        ];

        let result = Header::decode(&data);
        assert!(result.is_err());
        match result {
            Err(KafkaError::InsufficientData { needed, available }) => {
                assert_eq!(needed, 5);
                assert_eq!(available, 3);
            }
            _ => panic!("Expected InsufficientData error"),
        }
    }

    #[test]
    fn test_record_decode_minimal() {
        // Minimal record with a null key, empty value, no headers (batch format with zigzag)
        let data = vec![
            0x10, // length (varint: 16)
            0x00, // attributes (i8: 0)
            0x00, // timestamp_delta (zigzag varint: 0)
            0x00, // offset_delta (varint: 0)
            0x01, // key_length (zigzag varint: 1 = -1, meaning null)
            0x01, // value_length (zigzag varint: 1 = -1, meaning null/empty)
            0x00, // headers_count (zigzag varint: 0)
        ];

        let result = Record::decode(&data);
        assert!(result.is_ok());
        let record = result.unwrap();
        assert_eq!(record.length, 16);
        assert_eq!(record.attributes, 0);
        assert_eq!(record.timestamp_delta, 0);
        assert_eq!(record.offset_delta, 0);
        assert_eq!(record.key, None); // null key
        assert_eq!(record.headers.len(), 0);
    }

    #[test]
    fn test_record_decode_with_key_and_value() {
        // Batch format with zigzag encoding for lengths
        // Zigzag: 0->0, 1->-1, 2->1, 4->2, 6->3, 8->4, 10->5
        let data = vec![
            0x20, // length (varint: 32)
            0x00, // attributes
            0x00, // timestamp_delta (zigzag varint: 0)
            0x00, // offset_delta (varint: 0)
            // key
            0x08, // key_length (zigzag varint: 8 = 4 bytes)
            b'k', b'e', b'y', b's', // key: "keys"
            // value
            0x0A, // value_length (zigzag varint: 10 = 5 bytes)
            b'v', b'a', b'l', b'u', b'e', // value: "value"
            0x00, // headers_count (zigzag varint: 0)
        ];

        let result = Record::decode(&data);
        assert!(result.is_ok());
        let record = result.unwrap();
        assert_eq!(record.key, Some(b"keys".to_vec()));
        assert_eq!(record.value, b"value".to_vec());
        assert_eq!(record.headers.len(), 0);
    }

    #[test]
    fn test_record_decode_with_headers() {
        // Record with 2 headers using batch format (zigzag encoding)
        let data = vec![
            0x30, // length (varint: 48)
            0x00, // attributes
            0x00, // timestamp_delta (zigzag varint: 0)
            0x00, // offset_delta (varint: 0)
            0x01, // key_length (zigzag varint: 1 = -1, null)
            0x04, // value_length (zigzag varint: 4 = 2 bytes)
            0x01, 0x02, // value: [1, 2]
            0x04, // headers_count (zigzag varint: 4 = 2 headers)
            // First header
            0x04, // header key length (zigzag varint: 4 = 2 bytes)
            b'k', b'1', // header key: "k1"
            0x04, // header value length (zigzag varint: 4 = 2 bytes)
            0x0A, 0x0B, // header value: [10, 11]
            // Second header
            0x04, // header key length (zigzag varint: 4 = 2 bytes)
            b'k', b'2', // header key: "k2"
            0x04, // header value length (zigzag varint: 4 = 2 bytes)
            0x0C, 0x0D, // header value: [12, 13]
        ];

        let result = Record::decode(&data);
        assert!(result.is_ok());
        let record = result.unwrap();
        assert_eq!(record.headers.len(), 2);
        assert_eq!(record.headers[0].key, "k1");
        assert_eq!(record.headers[0].value, Some(vec![0x0A, 0x0B]));
        assert_eq!(record.headers[1].key, "k2");
        assert_eq!(record.headers[1].value, Some(vec![0x0C, 0x0D]));
    }

    #[test]
    fn test_record_decode_negative_headers_count() {
        // Test FIX for issue #4 - negative headers count validation
        // Using zigzag encoding: -1 encodes to 1
        let data = vec![
            0x10, // length
            0x00, // attributes
            0x00, // timestamp_delta (zigzag: 0)
            0x00, // offset_delta
            0x01, // key_length (zigzag: 1 = -1, null)
            0x01, // value_length (zigzag: 1 = -1, null)
            0x01, // headers_count (zigzag: 1 = -1, invalid!)
        ];

        let result = Record::decode(&data);
        // Should fail with validation error
        assert!(result.is_err());
        match result {
            Err(KafkaError::DecodeError(msg)) => {
                assert!(msg.contains("Invalid headers count"));
            }
            _ => panic!("Expected DecodeError for negative headers count"),
        }
    }

    #[test]
    fn test_record_decode_zigzag_timestamp() {
        // Test zigzag encoding for negative timestamp_delta
        let data = vec![
            0x10, // length
            0x00, // attributes
            0x01, // timestamp_delta (zigzag varint: 1 = -1)
            0x00, // offset_delta
            0x01, // key_length (zigzag: 1 = -1, null)
            0x01, // value_length (zigzag: 1 = -1, null)
            0x00, // headers_count (zigzag: 0)
        ];

        let result = Record::decode(&data);
        assert!(result.is_ok());
        let record = result.unwrap();
        // Zigzag: 1 should decode to -1
        assert_eq!(record.timestamp_delta, -1);
    }

    #[test]
    fn test_record_decode_attributes() {
        let data = vec![
            0x10, // length
            0xFF, // attributes (i8: -1)
            0x00, // timestamp_delta (zigzag: 0)
            0x00, // offset_delta
            0x01, // key_length (zigzag: 1 = -1, null)
            0x01, // value_length (zigzag: 1 = -1, null)
            0x00, // headers_count (zigzag: 0)
        ];

        let result = Record::decode(&data);
        assert!(result.is_ok());
        let record = result.unwrap();
        assert_eq!(record.attributes, -1);
    }

    #[test]
    fn test_record_decode_with_large_offset() {
        // Test with larger varint values
        let data = vec![
            0x80, 0x01, // length (varint: 128)
            0x00, // attributes
            0x00, // timestamp_delta (zigzag: 0)
            0x80, 0x02, // offset_delta (varint: 256)
            0x01, // key_length (zigzag: 1 = -1, null)
            0x01, // value_length (zigzag: 1 = -1, null)
            0x00, // headers_count (zigzag: 0)
        ];

        let result = Record::decode(&data);
        assert!(result.is_ok());
        let record = result.unwrap();
        assert_eq!(record.length, 128);
        assert_eq!(record.offset_delta, 256);
    }

    #[test]
    fn test_record_decode_insufficient_data() {
        // Incomplete record data
        let data = vec![
            0x10, // length
            0x00, // attributes
                  // Missing rest of the fields
        ];

        let result = Record::decode(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_header_decode_from_decoder_advances_position() {
        // Test that decode_from_decoder properly advances the decoder position
        let data = vec![
            // First header
            0x02, // key length
            b'k', b'1', // key
            0x01, // value length
            0x0A, // value
            // Second header
            0x02, // key length
            b'k', b'2', // key
            0x01, // value length
            0x0B, // value
        ];

        let mut decoder = MessageDecoder::new(&data);

        let header1 = Header::decode_from_decoder(&mut decoder).unwrap();
        assert_eq!(header1.key, "k1");
        assert_eq!(header1.value, Some(vec![0x0A]));

        let header2 = Header::decode_from_decoder(&mut decoder).unwrap();
        assert_eq!(header2.key, "k2");
        assert_eq!(header2.value, Some(vec![0x0B]));

        assert_eq!(decoder.remaining(), 0);
    }

    #[test]
    fn test_record_with_multiple_headers_and_values() {
        // Comprehensive test with key, value, and multiple headers using batch format (zigzag)
        let data = vec![
            0x50, // length
            0x08, // attributes (some flags set)
            0x04, // timestamp_delta (zigzag: 4 = 2)
            0x05, // offset_delta (varint: 5)
            // key
            0x06, // key_length (zigzag: 6 = 3 bytes)
            b'm', b's', b'g', // key: "msg"
            // value
            0x0E, // value_length (zigzag: 14 = 7 bytes)
            b'p', b'a', b'y', b'l', b'o', b'a', b'd', // value: "payload"
            0x06, // headers_count (zigzag: 6 = 3 headers)
            // Header 1
            0x08, // key length (zigzag: 8 = 4 bytes)
            b't', b'y', b'p', b'e', // "type"
            0x08, // value length (zigzag: 8 = 4 bytes)
            b'j', b's', b'o', b'n', // "json"
            // Header 2
            0x08, // key length (zigzag: 8 = 4 bytes)
            b'u', b's', b'e', b'r', // "user"
            0x0A, // value length (zigzag: 10 = 5 bytes)
            b'a', b'l', b'i', b'c', b'e', // "alice"
            // Header 3
            0x04, // key length (zigzag: 4 = 2 bytes)
            b'i', b'd', // "id"
            0x06, // value length (zigzag: 6 = 3 bytes)
            b'1', b'2', b'3', // "123"
        ];

        let result = Record::decode(&data);
        assert!(result.is_ok());
        let record = result.unwrap();

        assert_eq!(record.attributes, 8);
        assert_eq!(record.timestamp_delta, 2);
        assert_eq!(record.offset_delta, 5);
        assert_eq!(record.key, Some(b"msg".to_vec()));
        assert_eq!(record.value, b"payload".to_vec());
        assert_eq!(record.headers.len(), 3);

        assert_eq!(record.headers[0].key, "type");
        assert_eq!(record.headers[0].value, Some(b"json".to_vec()));

        assert_eq!(record.headers[1].key, "user");
        assert_eq!(record.headers[1].value, Some(b"alice".to_vec()));

        assert_eq!(record.headers[2].key, "id");
        assert_eq!(record.headers[2].value, Some(b"123".to_vec()));
    }
}
