mod authentication;
mod bindings;
mod c;
pub mod client;
mod error;
pub mod logger;
pub mod message;
pub mod producer;

pub use authentication::Authentication;
pub use client::{Client, ClientBuilder};

#[cfg(test)]
mod tests {
    use super::{message::ProducerMessage, Client, ClientBuilder};
    use pretty_env_logger;
    use tokio;

    #[tokio::test]
    async fn it_works() {
        pretty_env_logger::init();
        let client = ClientBuilder::new("pulsar://localhost:6650")
            .with_concurrent_lookup_requests(100)
            .with_io_threads(4)
            .with_operation_timeout_seconds(30)
            .with_stats_interval(30)
            .build()
            .unwrap();

        let producer = client
            .create_producer("persistent://public/default/test")
            .unwrap();

        let message = ProducerMessage::from_payload("Hello".as_bytes()).unwrap();
        producer.send_async(&message).await.unwrap();

        assert!(false);
    }
}
