use clap::Parser;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "kafkatui-rs")]
#[command(version, about = "A terminal UI for Apache Kafka")]
pub struct Args {
    /// Kafka broker address (can be specified multiple times)
    /// Example: -b localhost:9092 -b kafka2:9092
    #[arg(short, long = "broker", value_name = "HOST:PORT")]
    pub brokers: Vec<String>,

    /// Client ID for Kafka connections
    #[arg(long, default_value = "kafkatui-rs-client")]
    pub client_id: String,

    /// Connection timeout in seconds
    #[arg(long, default_value = "10")]
    pub timeout: u64,
}

impl Args {
    /// Returns brokers with default fallback to localhost:9092
    pub fn get_brokers(&self) -> Vec<&str> {
        if self.brokers.is_empty() {
            vec!["localhost:9092"]
        } else {
            self.brokers.iter().map(|s| s.as_str()).collect()
        }
    }

    /// Returns connection timeout as Duration
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_secs(self.timeout)
    }
}
