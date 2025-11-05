use kafkatui_rs::{
    connection::*,
    protocol::api_versions::{ApiVersionRequest, ApiVersionsResponse},
};

#[tokio::test]
async fn test_real_kafka_connection() {
    let mut conn = KafkaConnection::new("localhost:9092".to_string());

    match conn.connect().await {
        Ok(_) => {
            println!("Connected to real Kafka");

            let request = ApiVersionRequest::new(1);

            // send request
            conn.send_request(&request.encode()).await.unwrap();

            // read response
            let response = conn.read_response().await.unwrap();
            println!("Got response of {} bytes", response.len());

            let api_response = ApiVersionsResponse::decode(&response).unwrap();

            assert_eq!(api_response.correlation_id, 1);
            assert_eq!(api_response.error_code, 0);
            assert_eq!(api_response.api_versions[0].min_version, 0);
            assert_eq!(api_response.api_versions[0].max_version, 11);

            conn.disconnect().await.unwrap();
        }
        Err(_) => {
            println!("No Kafka broker available, skipping test");
        }
    }
}
