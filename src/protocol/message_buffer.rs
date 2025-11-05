// ============================================================================
// SECTION 1: Why MessageBuffer?
//
// Kafka expects exact byte sequences. We need a buffer that:
// 1. Maintains insertion order
// 2. Handles big-endian conversion
// 3. Tracks position for size calculations
// ============================================================================

pub struct MessageBuffer {
    data: Vec<u8>,
}

impl MessageBuffer {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    // ========================================================================
    // Primitive Type Encoders
    // According to: https://kafka.apache.org/protocol.html#protocol_types
    // ========================================================================

    /// BOOLEAN: Encoded as single byte (0 = false, 1 = true)
    pub fn put_bool(&mut self, value: bool) {
        self.data.push(if value { 1 } else { 0 });
    }

    /// INT8: Single signed byte
    pub fn put_i8(&mut self, value: i8) {
        self.data.push(value as u8);
    }

    /// INT16: 2 bytes, big-endian
    /// Example: 258 (0x0102) becomes [0x01, 0x02] on wire
    pub fn put_i16(&mut self, value: i16) {
        self.data.extend_from_slice(&value.to_be_bytes());
    }

    /// INT32: 4 bytes, big-endian
    /// Used for: array lengths, correlation IDs, partition IDs
    pub fn put_i32(&mut self, value: i32) {
        self.data.extend_from_slice(&value.to_be_bytes());
    }

    /// INT64: 8 bytes, big-endian
    /// Used for: offsets, timestamps
    pub fn put_i64(&mut self, value: i64) {
        self.data.extend_from_slice(&value.to_be_bytes());
    }

    /// UNSIGNED VARINT: Variable-length encoding (for Kafka v2.1+)
    /// Reference: https://developers.google.com/protocol-buffers/docs/encoding#varints
    /// Used in newer protocol versions for compact arrays
    pub fn put_uvarint(&mut self, mut value: u32) {
        while value >= 0x80 {
            self.data.push((value as u8) | 0x80);
            value >>= 7;
        }
        self.data.push(value as u8);
    }

    /// STRING: 2-byte length prefix + UTF-8 bytes
    /// Wire format: [length:INT16][bytes:BYTE_ARRAY]
    /// Example: "hello" becomes [0x00, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F]
    pub fn put_string(&mut self, s: &str) {
        let bytes = s.as_bytes();
        self.put_i16(bytes.len() as i16);
        self.data.extend_from_slice(bytes);
    }

    /// NULLABLE_STRING: Can be null (represented as -1)
    /// Wire format: length = -1 for null, otherwise same as STRING
    pub fn put_nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.put_string(s),
            None => self.put_i16(-1),
        }
    }

    /// BYTES: 4-byte length prefix + raw bytes
    /// Used for: message payloads, compressed data
    pub fn put_bytes(&mut self, bytes: Option<&[u8]>) {
        match bytes {
            Some(b) => {
                let len = b.len();
                self.put_i32(len as i32);
                self.data.extend_from_slice(b);
            }
            None => {
                self.put_i32(-1);
            }
        }
    }

    /// Reserve space for message size (always 4 bytes at start)
    pub fn reserve_size_header(&mut self) {
        self.data.extend_from_slice(&[0, 0, 0, 0]);
    }

    /// Fill in the reserved size header with actual message length
    /// Size does NOT include the 4-byte size field itself
    pub fn write_size_header(&mut self) {
        let size = (self.data.len() - 4) as i32;
        self.data[0..4].copy_from_slice(&size.to_be_bytes());
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod buffer_tests {
    use super::*;

    #[test]
    fn test_bool_encoding() {
        let mut buf = MessageBuffer::new();
        buf.put_bool(true);
        buf.put_bool(false);

        assert_eq!(buf.into_bytes(), vec![0x01, 0x00]);
    }

    #[test]
    fn test_i8_encoding() {
        let mut buf = MessageBuffer::new();
        buf.put_i8(127);
        buf.put_i8(-128);
        buf.put_i8(0);

        assert_eq!(buf.into_bytes(), vec![0x7F, 0x80, 0x00]);
    }

    #[test]
    fn test_i16_encoding() {
        let mut buf = MessageBuffer::new();
        buf.put_i16(258);
        buf.put_i16(-1);

        assert_eq!(buf.into_bytes(), vec![0x01, 0x02, 0xFF, 0xFF]);
    }

    #[test]
    fn test_i32_encoding() {
        let mut buf = MessageBuffer::new();
        buf.put_i32(0x01020304);
        buf.put_i32(-1);

        assert_eq!(
            buf.into_bytes(),
            vec![0x01, 0x02, 0x03, 0x04, 0xFF, 0xFF, 0xFF, 0xFF]
        )
    }

    #[test]
    fn test_i64_encoding() {
        let mut buf = MessageBuffer::new();
        buf.put_i64(0x0102030405060708);
        assert_eq!(
            buf.into_bytes(),
            vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
    }

    #[test]
    fn test_uvarint_encoding() {
        let mut buf = MessageBuffer::new();
        buf.put_uvarint(0); // Single byte: 0x00
        buf.put_uvarint(127); // Single byte: 0x7F
        buf.put_uvarint(128); // Two bytes: 0x80, 0x01
        buf.put_uvarint(300); // Two bytes: 0xAC, 0x02
        assert_eq!(buf.into_bytes(), vec![0x00, 0x7F, 0x80, 0x01, 0xAC, 0x02]);
    }

    #[test]
    fn test_string_encoding() {
        let mut buf = MessageBuffer::new();
        buf.put_string("hello");
        buf.put_string("");
        assert_eq!(
            buf.into_bytes(),
            vec![
                0x00, 0x05, b'h', b'e', b'l', b'l', b'o', // "hello"
                0x00, 0x00, // empty string
            ]
        );
    }

    #[test]
    fn test_nullable_string_encoding() {
        let mut buf = MessageBuffer::new();
        buf.put_nullable_string(Some("test"));
        buf.put_nullable_string(None);
        assert_eq!(
            buf.into_bytes(),
            vec![
                0x00, 0x04, b't', b'e', b's', b't', // "test"
                0xFF, 0xFF, // null
            ]
        );
    }

    #[test]
    fn test_bytes_encoding() {
        let mut buf = MessageBuffer::new();
        buf.put_bytes(Some(&[1, 2, 3]));
        buf.put_bytes(None);
        assert_eq!(
            buf.into_bytes(),
            vec![
                0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03, 0xFF, 0xFF, 0xFF, 0xFF
            ]
        )
    }

    #[test]
    fn test_size_header() {
        let mut buf = MessageBuffer::new();
        buf.reserve_size_header();
        buf.put_i32(42); // 4 bytes
        buf.put_string("test"); // 2 bytes length + 4 bytes content
        buf.write_size_header();

        let bytes = buf.into_bytes();
        let size = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        assert_eq!(size, 10);
    }
}
