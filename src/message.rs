use crate::c::{CMessage, CMessageId};
use crate::error::{PulsarError, PulsarResult};

use std::collections::HashMap;
use std::ffi::CString;

pub struct MessageId {
    internal: CMessageId,
}

pub struct ProducerMessageBuilder<'a> {
    properties: Option<HashMap<&'a str, &'a str>>,
    sequence_id: Option<i64>,
    partition_key: Option<&'a str>,
}

/// A message to be sent by a producer.
///
/// You can create a message from a payload, or use the `builder` method
/// to set extra parameters:
/// ```
/// let message = ProducerMessage::from_payload("Hello, World".to_bytes()).unwrap();
///
/// let message = ProducerMessage::builder()
///     .with_sequence_id(42)
///     .with_partition_key("go to the left")
///     .payload("Hello, left partition")
///     .unwrap();
/// ```
pub struct ProducerMessage {
    internal: CMessage,
}

impl<'a> ProducerMessageBuilder<'a> {
    pub fn new() -> Self {
        Self {
            properties: None,
            sequence_id: None,
            partition_key: None,
        }
    }

    /// Attach application defined properties on the message. These properties will be available
    /// for consumers.
    pub fn with_properties(mut self, properties: HashMap<&'a str, &'a str>) -> Self {
        self.properties = Some(properties);
        self
    }

    /// Set the message sequence id.
    pub fn with_sequence_id(mut self, sequence_id: i64) -> Self {
        self.sequence_id = Some(sequence_id);
        self
    }

    /// Set the partition key to specify which partition the message goes to.
    pub fn with_partition_key(mut self, partition_key: &'a str) -> Self {
        self.partition_key = Some(partition_key);
        self
    }

    /// Creates the message with the given payload. Consumes the builder.
    pub fn payload(self, payload: &[u8]) -> PulsarResult<ProducerMessage> {
        let mut raw_message = CMessage::new();
        if let Some(partition_key) = self.partition_key {
            let value = CString::new(partition_key).map_err(|_| PulsarError::InvalidString)?;
            raw_message.set_partition_key(&value);
        }

        if let Some(sequence_id) = self.sequence_id {
            raw_message.set_sequence_id(sequence_id);
        }

        if let Some(properties) = self.properties {
            for (key, value) in properties.iter() {
                let key_string = CString::new(*key).map_err(|_| PulsarError::InvalidString)?;
                let value_string = CString::new(*value).map_err(|_| PulsarError::InvalidString)?;
                raw_message.set_property(&key_string, &value_string);
            }
        }

        raw_message.set_content(payload);

        Ok(ProducerMessage::from_internal(raw_message))
    }
}

impl ProducerMessage {
    /// Creates a message builder
    pub fn builder<'a>() -> ProducerMessageBuilder<'a> {
        ProducerMessageBuilder::new()
    }

    /// Creates a message from a payload. This is equivalent to:
    /// ```
    /// ProducerMessage::builder().payload(payload);
    /// ```
    pub fn from_payload(payload: &[u8]) -> PulsarResult<Self> {
        ProducerMessageBuilder::new().payload(payload)
    }

    pub(crate) fn from_internal(internal: CMessage) -> Self {
        Self { internal }
    }
    pub(crate) fn get_internal(&self) -> &CMessage {
        &self.internal
    }

    pub(crate) fn into_internal(self) -> CMessage {
        self.internal
    }
}
