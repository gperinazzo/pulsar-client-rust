mod authentication;
mod bindings;
mod c;
mod client;
mod error;
pub mod logger;

pub use authentication::Authentication;
pub use client::{Client, ClientConfiguration};

#[cfg(test)]
mod tests {
    use super::{Client, ClientConfiguration};
    use pretty_env_logger;

    #[test]
    fn it_works() {
        pretty_env_logger::init();
        let client = ClientConfiguration::new("pulsar://localhost:6650")
            .with_concurrent_lookup_requests(100)
            .with_io_threads(4)
            .with_operation_timeout_seconds(30)
            .with_stats_interval(30)
            .client()
            .unwrap();

        client.test_producer();
        assert!(false);
    }
}
