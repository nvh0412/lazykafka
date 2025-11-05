use std::{
    io::{Read, Result, Write},
    net::TcpStream,
};

mod connection;
mod error;
mod protocol;

fn main() -> Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:9092")?;
    stream.write(b"Hello Kafka");

    let mut buffer = [0; 128];
    stream.read(&mut buffer)?;

    Ok(())
}

#[cfg(test)]
mod round_trip_tests {
    use crate::protocol::{message_buffer::MessageBuffer, message_decoder::MessageDecoder};

    #[test]
    fn test_complete_round_trip() {
        // encode
        let mut buf = MessageBuffer::new();
        buf.put_i32(42);
        buf.put_string("Kafka");
        buf.put_nullable_string(None);
        buf.put_bool(true);
        let encoded = buf.into_bytes();

        // Decode
        let mut decoder = MessageDecoder::new(&encoded);
        assert_eq!(decoder.read_i32().unwrap(), 42);
        assert_eq!(decoder.read_string().unwrap(), "Kafka");
        assert_eq!(decoder.read_nullable_string().unwrap(), None);
        assert_eq!(decoder.read_bool().unwrap(), true);
        assert_eq!(decoder.remaining(), 0);
    }
}
