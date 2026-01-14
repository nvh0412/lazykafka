use crate::error::*;

// SECTION 2: MessageDecoder - Why we need it
//
// Kafka responses need careful parsing:
// 1. We must track position in buffer
// 2. We must check bounds before reading
// 3. We must handle partial messages gracefully
// ============================================================================
pub struct MessageDecoder<'a> {
    data: &'a [u8],
    pos: usize,
}

#[allow(dead_code)]
impl<'a> MessageDecoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    /// Check how many bytes remain unread
    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    #[allow(dead_code)]
    /// Peak at bytes without advancing position (for debugging)
    pub fn peek(&self, count: usize) -> &[u8] {
        let end = std::cmp::min(self.pos + count, self.data.len());
        &self.data[self.pos..end]
    }

    /// Decode bool
    pub fn read_bool(&mut self) -> Result<bool> {
        if self.remaining() < 1 {
            return Err(KafkaError::InsufficientData {
                needed: 1,
                available: self.remaining(),
            });
        }

        let value = self.data[self.pos] != 0;
        self.pos += 1;
        Ok(value)
    }

    /// Decode i8
    pub fn read_i8(&mut self) -> Result<i8> {
        if self.remaining() < 1 {
            return Err(KafkaError::InsufficientData {
                needed: 1,
                available: self.remaining(),
            });
        }
        let value = self.data[self.pos] as i8;
        self.pos += 1;
        Ok(value)
    }

    /// Decode i16
    pub fn read_i16(&mut self) -> Result<i16> {
        if self.remaining() < 2 {
            return Err(KafkaError::InsufficientData {
                needed: 2,
                available: self.remaining(),
            });
        }
        let value = i16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);

        self.pos += 2;
        Ok(value)
    }

    /// Decode i32
    pub fn read_i32(&mut self) -> Result<i32> {
        if self.remaining() < 4 {
            return Err(KafkaError::InsufficientData {
                needed: 4,
                available: self.remaining(),
            });
        }

        let value = i32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(value)
    }

    /// Decode i64
    pub fn read_i64(&mut self) -> Result<i64> {
        if self.remaining() < 8 {
            return Err(KafkaError::InsufficientData {
                needed: 8,
                available: self.remaining(),
            });
        }

        let value = i64::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(value)
    }

    pub fn read_uvarint(&mut self) -> Result<u32> {
        let mut result = 0u32;
        let mut shift = 0;

        loop {
            if self.remaining() < 1 {
                return Err(KafkaError::DecodeError("Incomplete varint".to_string()));
            }

            let byte = self.data[self.pos];
            self.pos += 1;

            result |= ((byte & 0x7F) as u32) << shift;

            if byte & 0x80 == 0 {
                return Ok(result);
            }

            shift += 7;
            if shift >= 32 {
                return Err(KafkaError::DecodeError("Varint too long".to_string()));
            }
        }
    }

    pub fn read_varint(&mut self) -> Result<i64> {
        let mut value: i64 = 0;
        let mut shift = 0;

        loop {
            if self.remaining() < 1 {
                return Err(KafkaError::DecodeError("Incomplete varint".to_string()));
            }

            let byte = self.data[self.pos];
            self.pos += 1;

            value |= ((byte & 0x7F) as i64) << shift;

            if (byte & 0x80) == 0 {
                return Ok(value);
            }

            shift += 7;
            if shift >= 64 {
                return Err(KafkaError::DecodeError("Varint too long".to_string()));
            }
        }
    }

    pub fn read_varint_zigzag(&mut self) -> Result<i64> {
        let unsigned = self.read_varint()?;
        // Zigzag decode: (n >> 1) ^ -(n & 1)
        Ok((unsigned >> 1) ^ -(unsigned & 1))
    }

    pub fn read_string(&mut self) -> Result<String> {
        let len = self.read_i16()?;

        if len < 0 {
            return Err(KafkaError::DecodeError(
                "Unexpected null string".to_string(),
            ));
        }

        let len = len as usize;
        if self.remaining() < len {
            return Err(KafkaError::InsufficientData {
                needed: len,
                available: self.remaining(),
            });
        }

        let s = str::from_utf8(&self.data[self.pos..self.pos + len])
            .map_err(|e| KafkaError::DecodeError(format!("Invalid UTF-8: {}", e)))?
            .to_string();
        self.pos += len;

        Ok(s)
    }

    pub fn read_nullable_string(&mut self) -> Result<Option<String>> {
        let len = self.read_i16()?;
        if len < 0 {
            return Ok(None);
        }

        let len = len as usize;
        if self.remaining() < len {
            return Err(KafkaError::InsufficientData {
                needed: len,
                available: self.remaining(),
            });
        }

        let s = std::str::from_utf8(&self.data[self.pos..self.pos + len])
            .map_err(|e| KafkaError::DecodeError(format!("Invalid UTF-8: {}", e)))?
            .to_string();
        self.pos += len;
        Ok(Some(s))
    }

    pub fn read_bytes_from_length(&mut self, length: i64) -> Result<Option<Vec<u8>>> {
        if length < 0 {
            return Ok(None);
        }

        let len = length as usize;
        if self.remaining() < len {
            return Err(KafkaError::InsufficientData {
                needed: len,
                available: self.remaining(),
            });
        }

        let bytes = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(Some(bytes))
    }

    pub fn read_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let len = self.read_i32()?;
        if len < 0 {
            return Ok(None);
        }

        let len = len as usize;
        if self.remaining() < len {
            return Err(KafkaError::InsufficientData {
                needed: len,
                available: self.remaining(),
            });
        }

        let bytes = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(Some(bytes))
    }
}

#[cfg(test)]
mod decoder_tests {
    use super::*;

    #[test]
    fn test_bool_decoding() {
        let data = vec![0x01, 0x00];
        let mut dec = MessageDecoder::new(&data);

        assert!(dec.read_bool().unwrap());
        assert!(dec.read_bool().unwrap());
    }

    #[test]
    fn test_i8_decoding() {
        let data = vec![0x72, 0xFF];
        let mut dec = MessageDecoder::new(&data);

        assert_eq!(dec.read_i8().unwrap(), 114);
        assert_eq!(dec.read_i8().unwrap(), -1);
    }

    #[test]
    fn test_i16_decoding() {
        let data = vec![0x01, 0x02, 0xFF, 0xFF];
        let mut dec = MessageDecoder::new(&data);

        assert_eq!(dec.read_i16().unwrap(), 258);
        assert_eq!(dec.read_i16().unwrap(), -1);
    }

    #[test]
    fn test_i32_decoding() {
        let data = vec![0x00, 0x00, 0x00, 0x2A, 0xFF, 0xFF, 0xFF, 0xFF];
        let mut decoder = MessageDecoder::new(&data);
        assert_eq!(decoder.read_i32().unwrap(), 42);
        assert_eq!(decoder.read_i32().unwrap(), -1);
    }

    #[test]
    fn test_uvarint_decoding() {
        let data = vec![0x00, 0x7F, 0x80, 0x01, 0xAC, 0x02];
        let mut decoder = MessageDecoder::new(&data);
        assert_eq!(decoder.read_uvarint().unwrap(), 0);
        assert_eq!(decoder.read_uvarint().unwrap(), 127);
        assert_eq!(decoder.read_uvarint().unwrap(), 128);
        assert_eq!(decoder.read_uvarint().unwrap(), 300);
    }

    #[test]
    fn test_string_decoding() {
        let data = vec![0x00, 0x05, b'h', b'e', b'l', b'l', b'o'];
        let mut decoder = MessageDecoder::new(&data);

        assert_eq!(decoder.read_string().unwrap(), "hello");
    }

    #[test]
    fn test_nullable_string_decoding() {
        let data = vec![0x00, 0x04, b't', b'e', b's', b't', 0xFF, 0xFF];
        let mut decoder = MessageDecoder::new(&data);
        assert_eq!(
            decoder.read_nullable_string().unwrap(),
            Some("test".to_string())
        );
        assert_eq!(decoder.read_nullable_string().unwrap(), None);
    }

    #[test]
    fn test_bytes_decoding() {
        let data = vec![
            0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03, 0xFF, 0xFF, 0xFF, 0xFF,
        ];
        let mut decoder = MessageDecoder::new(&data);
        assert_eq!(decoder.read_bytes().unwrap(), Some(vec![1, 2, 3]));
        assert_eq!(decoder.read_bytes().unwrap(), None);
    }

    #[test]
    fn test_insufficient_data_error() {
        let data = vec![0x00]; // Only 1 byte
        let mut decoder = MessageDecoder::new(&data);

        match decoder.read_i32() {
            Err(KafkaError::InsufficientData { needed, available }) => {
                assert_eq!(needed, 4);
                assert_eq!(available, 1);
            }
            _ => panic!("Expected InsufficientData error"),
        }
    }

    #[test]
    fn test_invalid_utf8_error() {
        let data = vec![0x00, 0x02, 0xFF, 0xFE]; // Invalid UTF-8
        let mut decoder = MessageDecoder::new(&data);

        match decoder.read_string() {
            Err(KafkaError::DecodeError(msg)) => {
                assert!(msg.contains("Invalid UTF-8"));
            }
            _ => panic!("Expected DecodeError for invalid UTF-8"),
        }
    }

    #[test]
    fn test_read_bytes_from_length_valid() {
        let data = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let mut decoder = MessageDecoder::new(&data);

        // Read 3 bytes
        let result = decoder.read_bytes_from_length(3).unwrap();
        assert_eq!(result, Some(vec![0x01, 0x02, 0x03]));
        assert_eq!(decoder.pos, 3);
        assert_eq!(decoder.remaining(), 2);

        // Read remaining 2 bytes
        let result = decoder.read_bytes_from_length(2).unwrap();
        assert_eq!(result, Some(vec![0x04, 0x05]));
        assert_eq!(decoder.pos, 5);
        assert_eq!(decoder.remaining(), 0);
    }

    #[test]
    fn test_read_bytes_from_length_null() {
        let data = vec![0x01, 0x02, 0x03];
        let mut decoder = MessageDecoder::new(&data);

        // Negative length should return None without consuming data
        let result = decoder.read_bytes_from_length(-1).unwrap();
        assert_eq!(result, None);
        assert_eq!(decoder.pos, 0);
        assert_eq!(decoder.remaining(), 3);
    }

    #[test]
    fn test_read_bytes_from_length_insufficient_data() {
        let data = vec![0x01, 0x02, 0x03];
        let mut decoder = MessageDecoder::new(&data);

        // Try to read 5 bytes when only 3 are available
        match decoder.read_bytes_from_length(5) {
            Err(KafkaError::InsufficientData { needed, available }) => {
                assert_eq!(needed, 5);
                assert_eq!(available, 3);
            }
            _ => panic!("Expected InsufficientData error"),
        }

        // Position should not have advanced
        assert_eq!(decoder.pos, 0);
    }

    #[test]
    fn test_read_bytes_from_length_after_other_reads() {
        let data = vec![0x00, 0x00, 0x00, 0x2A, 0x01, 0x02, 0x03];
        let mut decoder = MessageDecoder::new(&data);

        // Read i32 first
        let num = decoder.read_i32().unwrap();
        assert_eq!(num, 42);

        // Then read bytes with specific length
        let result = decoder.read_bytes_from_length(3).unwrap();
        assert_eq!(result, Some(vec![0x01, 0x02, 0x03]));
        assert_eq!(decoder.remaining(), 0);
    }
}
