use crate::bindings::producer;
use crate::c::{ArcClient, ArcProducer, CProducer, CProducerConfiguration};
pub use crate::c::{CompressionType, HashingScheme, RoutingMode};
use crate::client::Client;
use crate::error::{PulsarError, PulsarResult};
use crate::message::ProducerMessage;

use std::collections::HashMap;
use std::ffi::{CStr, CString};

use futures::channel::oneshot;

pub struct ProducerBuilder<'a> {
    topic: &'a str,
    name: Option<&'a str>,
    send_timeout: Option<i32>,
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
    batching_max_publish_delay_ms: Option<u64>,
    properties: Option<HashMap<&'a str, &'a str>>,
}

impl<'a> ProducerBuilder<'a> {
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

    pub fn with_name(mut self, name: &'a str) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_send_timeout(mut self, send_timeout: i32) -> Self {
        self.send_timeout = Some(send_timeout);
        self
    }

    pub fn with_initial_sequence_id(mut self, initial_sequence_id: i64) -> Self {
        self.initial_sequence_id = Some(initial_sequence_id);
        self
    }

    pub fn with_compression_type(mut self, compression_type: CompressionType) -> Self {
        self.compression_type = Some(compression_type);
        self
    }

    pub fn with_max_pending_messages(mut self, max_pending_messages: i32) -> Self {
        self.max_pending_messages = Some(max_pending_messages);
        self
    }

    pub fn with_max_pending_messages_across_partitions(mut self, max: i32) -> Self {
        self.max_pending_messages_across_partitions = Some(max);
        self
    }

    pub fn with_routing_mode(mut self, routing_mode: RoutingMode) -> Self {
        self.routing_mode = Some(routing_mode);
        self
    }

    pub fn with_block_if_queue_full(mut self, block_if_queue_full: bool) -> Self {
        self.block_if_queue_full = Some(block_if_queue_full);
        self
    }

    pub fn with_batching_enabled(mut self, batching_enabled: bool) -> Self {
        self.batching_enabled = Some(batching_enabled);
        self
    }

    pub fn with_batching_max_messages(mut self, matching_max_messages: u32) -> Self {
        self.batching_max_messages = Some(matching_max_messages);
        self
    }

    pub fn with_batching_max_allowed_size_in_bytes(
        mut self,
        batching_max_allowed_size_in_bytes: u64,
    ) -> Self {
        self.batching_max_allowed_size_in_bytes = Some(batching_max_allowed_size_in_bytes);
        self
    }

    pub fn with_batching_max_publish_delay_ms(
        mut self,
        batching_max_publish_delay_ms: u64,
    ) -> Self {
        self.batching_max_publish_delay_ms = Some(batching_max_publish_delay_ms);
        self
    }

    pub fn with_properties(mut self, properties: HashMap<&'a str, &'a str>) -> Self {
        self.properties = Some(properties);
        self
    }

    pub fn build<'c>(self, client: &'c Client) -> PulsarResult<Producer<'c>> {
        client.create_producer_from_builder(self)
    }
}

#[derive(Clone)]
pub struct Producer<'client> {
    internal: ArcProducer<'client>,
}

impl<'client> Producer<'client> {
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
            raw_config.set_send_timeout(send_timeout);
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
            raw_config.set_batching_max_allowed_size_in_bytes(max);
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

    pub(crate) fn from_internal(internal: ArcProducer<'client>) -> Self {
        Self { internal }
    }

    pub fn send(&self, message: &ProducerMessage) -> PulsarResult<()> {
        self.internal.send(message.get_internal())
    }

    pub fn close(&self) -> PulsarResult<()> {
        self.internal.close()
    }

    pub async fn send_async(&self, message: &ProducerMessage) -> PulsarResult<()> {
        self.internal.send_async(message.get_internal()).await
    }

    pub async fn close_async(&self) -> PulsarResult<()> {
        self.internal.close_async().await
    }
}
