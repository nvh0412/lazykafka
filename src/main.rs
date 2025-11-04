use std::{
    io::{Read, Result, Write},
    net::TcpStream,
};

mod error;
mod protocol;

fn main() -> Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:9092")?;
    stream.write(b"Hello Kafka");

    let mut buffer = [0; 128];
    stream.read(&mut buffer)?;

    Ok(())
}
