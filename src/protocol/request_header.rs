use crate::protocol::message_buffer::MessageBuffer;

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn encode(&self, buf: &mut MessageBuffer) {
        buf.put_i16(self.api_key);
        buf.put_i16(self.api_version);
        buf.put_i32(self.correlation_id);
        buf.put_nullable_string(self.client_id.as_deref());
    }
}
