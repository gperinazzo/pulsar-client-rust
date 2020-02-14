//! # Producer
//! You can create a producer from a client with the `create_producer` method.
//! ```
//! use pulsar_client::client::Client;
//!
//! let client = Client::from_url("pulsar://localhost:6650").unwrap();
//! let producer = client.create_producer("persistent://public/default/test").unwrap();
//! ```
//!
//! To set extra options on the producer being created, use the `builder` method on producer.
//! ```
//! use pulsar_client::producer::Producer;
//! use std::time::Duration;
//!
//! # let client = Client::from_url("pulsar://localhost:6650").unwrap();
//! let producer = Producer::builder("persistent://public/default/test")
//!     .with_name("my-producer")
//!     .with_initial_sequence_id(3)
//!     .with_batching(true)
//!     .with_batching_max_publish_delay(
//!     .build(&client)
//!     .unwrap();
//! ```
//!
//!  

use crate::c::{ArcProducer, CProducerConfiguration};
use crate::client::Client;
use crate::error::{PulsarError, PulsarResult};

pub use crate::message::ProducerMessage;
pub use crate::c::{CompressionType, HashingScheme, RoutingMode};


use std::collections::HashMap;
use std::time::Duration;
use std::ffi::CString;
use std::convert::TryInto;

/// Builder pattern for producer creation. Requires a valid client to build.
///
/// The builder is most commonly created using the `Producer::builder` function.
///
/// Usage:
/// ```
/// # use pulsar_client::client::Client;
/// # let client = Client::from_url("pulsar://localhost:6650").unwrap();
/// let producer = Producer::builder("persistent://public/default/test")
///     .with_name("my-producer")
///     .with_initial_sequence_id(3)
///     .build(&client)
///     .unwrap();
/// ```
pub struct ProducerBuilder<'a> {
    topic: &'a str,
    name: Option<&'a str>,
    send_timeout: Option<u128>,
    initial_sequence_id: Option<i64>,
    compression_type: Option<CompressionType>,
    max_pending_messages: Option<i32>,
    max_pending_messages_across_partitions: Option<i32>,
    routing_mode: Option<RoutingMode>,
    hashing_scheme: Option<HashingScheme>,
    block_if_queue_full: Option<bool>,
    batching_enabled: Option<bool>,
    batching_max_messages: Option<u32>,
    batching_max_allowed_size_in_bytes: Option<u64>,
    batching_max_publish_delay_ms: Option<u128>,
    properties: Option<HashMap<&'a str, &'a str>>,
}

impl<'a> ProducerBuilder<'a> {
    /// Creates a builder for a producer to the given topic. The topic name 
    /// must not contain any 0 bytes.
    pub fn new(topic: &'a str) -> Self {
        Self {
            topic,
            name: None,
            send_timeout: None,
            initial_sequence_id: None,
            compression_type: None,
            max_pending_messages: None,
            max_pending_messages_across_partitions: None,
            routing_mode: None,
            hashing_scheme: None,
            block_if_queue_full: None,
            batching_enabled: None,
            batching_max_messages: None,
            batching_max_allowed_size_in_bytes: None,
            batching_max_publish_delay_ms: None,
            properties: None,
        }
    }

    /// Sets the producer name. The name must not contain any 0 bytes.
    ///
    /// If not assigned a globally unique name will be generated, which can be accessed
    /// with `producer.name()`.
    ///
    /// Producer name must be unique for a given topic, building the producer will fail
    /// if there's already a producer with the same name for that topic.
    pub fn with_name(mut self, name: &'a str) -> Self {
        self.name = Some(name);
        self
    }

    /// Sets the send timeout (defaults to 30 seconds).
    ///
    /// The producer will use a precision in milliseconds. The total number of milliseconds must be
    /// less than 2147483648 (31 bits).
    /// Setting it to 0 disables the timeout.
    pub fn with_send_timeout(mut self, send_timeout: &Duration) -> Self {
        self.send_timeout = Some(send_timeout.as_millis());
        self
    }

    /// Sets the producer initial sequence id.
    pub fn with_initial_sequence_id(mut self, initial_sequence_id: i64) -> Self {
        self.initial_sequence_id = Some(initial_sequence_id);
        self
    }

    /// Set the compression type for the producer (defaults to no compression)
    ///
    /// Currently supported compression types are:
    /// - LZ4
    /// - ZLIB
    pub fn with_compression_type(mut self, compression_type: CompressionType) -> Self {
        self.compression_type = Some(compression_type);
        self
    }

    /// Sets whether the `send` and `send_async` operations should block if
    /// the queue if full (defaults to false).
    ///
    /// When false, send operations will fail if the queue is full with a 
    /// `PulsarError::ProducerQueueIsFull` error.
    pub fn with_block_if_queue_full(mut self, block_if_queue_full: bool) -> Self {
        self.block_if_queue_full = Some(block_if_queue_full);
        self
    }

    /// Set the max size of the queue for messages per partition that 
    /// are waiting acknowledgement from the broker (defaults to 1000).
    ///
    /// When the queue is full, send operations will fail unless the producer was set
    /// to block when full.
    pub fn with_max_pending_messages(mut self, max_pending_messages: i32) -> Self {
        self.max_pending_messages = Some(max_pending_messages);
        self
    }

    /// Set the number of max pending messages across all the partitions (defaults to 50000).
    ///
    /// This will reduce the maximum pending messages per partition if the total exceeds
    /// the configured value.
    pub fn with_max_pending_messages_across_partitions(mut self, max: i32) -> Self {
        self.max_pending_messages_across_partitions = Some(max);
        self
    }

    /// Set the message routing mode for partitioned producers (defaults to round-robin routing).
    pub fn with_routing_mode(mut self, routing_mode: RoutingMode) -> Self {
        self.routing_mode = Some(routing_mode);
        self
    }

    /// Sets whether or not to use batching (defaults to false).
    ///
    /// When set to true, calls to `send_async` will be batched.
    pub fn with_batching(mut self, batching_enabled: bool) -> Self {
        self.batching_enabled = Some(batching_enabled);
        self
    }

    /// Sets the max number of messages in a batch (defaults to 1000).
    ///
    /// When batching is enabled, the producer will send the current batch once this value is
    /// reached. 
    ///
    /// Set to 0 to disable.
    pub fn with_batching_max_messages(mut self, matching_max_messages: u32) -> Self {
        self.batching_max_messages = Some(matching_max_messages);
        self
    }

    /// Sets the max number of bytes allowed in a batch (defaults to 128Kb).
    /// 
    /// When batching is enabled, the producer will send the current batch once this value is
    /// reached.
    ///
    /// Set to 0 to disable.
    pub fn with_batching_max_allowed_size_in_bytes(
        mut self,
        batching_max_allowed_size_in_bytes: u64,
    ) -> Self {
        self.batching_max_allowed_size_in_bytes = Some(batching_max_allowed_size_in_bytes);
        self
    }

    /// Sets the max publish delay for the message (defaults to 10 milliseconds). The producer uses
    /// millisecond precision, and the number of milliseconds must fit in 64 bits.
    ///
    /// When batching is enabled, the producer will send the current batch once this value is
    /// reached.
    /// 
    /// Set to 0 to disable.
    pub fn with_batching_max_publish_delay(
        mut self,
        batching_max_publish_delay: &Duration,
    ) -> Self {
        self.batching_max_publish_delay_ms = Some(batching_max_publish_delay.as_millis());
        self
    }

    /// Attach a set of application defined properties to the producer.
    /// These properties will be visible in the topic stats.
    pub fn with_properties(mut self, properties: HashMap<&'a str, &'a str>) -> Self {
        self.properties = Some(properties);
        self
    }

    /// Builds the producer with the given client.
    pub fn build(self, client: &Client) -> PulsarResult<Producer> {
        client.create_producer_from_builder(self)
    }

    /// Builds the producer with the given client asyncronously.
    pub async fn build_async(self, client: &Client) -> PulsarResult<Producer> {
        client.create_producer_from_builder_async(self).await
    }
}

#[derive(Clone)]
pub struct Producer {
    internal: ArcProducer,
}

impl Producer {

    /// Creates a producer builder.
    pub fn builder<'a>(topic: &'a str) -> ProducerBuilder<'a> {
        ProducerBuilder::new(topic)
    }

    pub(crate) fn create_producer_configuration(
        builder: ProducerBuilder,
    ) -> PulsarResult<(CString, CProducerConfiguration)> {
        let topic = CString::new(builder.topic).map_err(|_| PulsarError::InvalidTopicName)?;
        let raw_config = CProducerConfiguration::new()?;

        if let Some(name) = builder.name {
            let value = CString::new(name).map_err(|_| PulsarError::InvalidString)?;
            raw_config.set_name(&value);
        }

        if let Some(initial_sequence_id) = builder.initial_sequence_id {
            raw_config.set_initial_sequence_id(initial_sequence_id);
        }

        if let Some(send_timeout) = builder.send_timeout {
            raw_config.set_send_timeout(send_timeout.try_into()?);
        }

        if let Some(compression_type) = builder.compression_type {
            raw_config.set_compression_type(compression_type);
        }

        if let Some(max_pending_messages) = builder.max_pending_messages {
            raw_config.set_max_pending_messages(max_pending_messages);
        }

        if let Some(max) = builder.max_pending_messages_across_partitions {
            raw_config.set_max_pending_messages_across_partitions(max);
        }

        if let Some(routing_mode) = builder.routing_mode {
            raw_config.set_partitions_routing_mode(routing_mode);
        }

        if let Some(hashing_scheme) = builder.hashing_scheme {
            raw_config.set_hashing_scheme(hashing_scheme);
        }

        if let Some(block) = builder.block_if_queue_full {
            raw_config.set_block_if_queue_full(block);
        }

        if let Some(batching) = builder.batching_enabled {
            raw_config.set_batching_enabled(batching);
        }

        if let Some(max) = builder.batching_max_messages {
            raw_config.set_batching_max_messages(max);
        }

        if let Some(max) = builder.batching_max_allowed_size_in_bytes {
            raw_config.set_batching_max_allowed_size_in_bytes(max);
        }

        if let Some(max) = builder.batching_max_publish_delay_ms {
            raw_config.set_batching_max_allowed_size_in_bytes(max.try_into()?);
        }

        if let Some(properties) = builder.properties {
            for (key, value) in properties.iter() {
                let key_string = CString::new(*key).map_err(|_| PulsarError::InvalidString)?;
                let value_string = CString::new(*value).map_err(|_| PulsarError::InvalidString)?;
                raw_config.set_property(&key_string, &value_string);
            }
        }

        Ok((topic, raw_config))
    }

    pub(crate) fn from_internal(internal: ArcProducer) -> Self {
        Self { internal }
    }

    /// Send a message.
    ///
    /// This call will block until the message is successfully acknowledged by the broker, or until
    /// the configured send timeout.
    pub fn send(&self, message: &ProducerMessage) -> PulsarResult<()> {
        self.internal.send(message.get_internal())
    }

    /// Send a message (async).
    pub async fn send_async(&self, message: ProducerMessage) -> PulsarResult<()> {
        self.internal.send_async(message.into_internal()).await
    }


    /// Close the producer and release all resources allocated to it.
    ///
    /// No more writes will be accepted from this producer. Waits until all pending writes are
    /// persisted. In case of errors, pending writes will not be retried.
    ///
    /// This function will block until the producer fully closes.
    pub fn close(&self) -> PulsarResult<()> {
        self.internal.close()
    }

    /// Closes the producer and release all resources allocated to it.
    pub async fn close_async(&self) -> PulsarResult<()> {
        self.internal.close_async().await
    }

    /// Flushes all the messages buffered in the producer.
    ///
    /// This function will block until all messages have been successfully persisted.
    pub fn flush(&self) -> PulsarResult<()> {
        self.internal.flush()
    }

    /// Flushes all the messages buffered in the producer.
    pub async fn flush_async(&self) -> PulsarResult<()> {
        self.internal.flush_async().await
    }

    /// Get the producer name. This may either be the name passed in the producer configuration,
    /// or the name generated for the producer.
    pub fn name<'a>(&'a self) -> &'a str {
        self.internal.get_producer_name().to_str().unwrap()
    }

    /// Gets the sequence id of the last message sent by this producer.
    pub fn last_sequence_id(&self) -> i64 {
        self.internal.get_last_sequence_id()
    }

    /// Gets the topic that this producer will send messages to.
    pub fn topic<'a>(&'a self) -> &'a str {
        self.internal.get_topic().to_str().unwrap()
    }
}
