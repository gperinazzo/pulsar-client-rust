use crate::bindings::raw::pulsar_result;
use std::convert::From;
use std::ffi::NulError;
use std::fmt;

#[derive(Clone, Copy, Debug)]
pub enum PulsarError {
    NulError = 128,
    InvalidString = 129,
    InvalidDuration = 130,
    UnknownError = 1,
    InvalidConfiguration,
    Timeout,
    LookupError,
    ConnectError,
    ReadError,
    AuthenticationError,
    AuthorizationError,
    ErrorGettingAuthenticationData,
    BrokerMetadataError,
    BrokerPersistenceError,
    ChecksumError,
    ConsumerBusy,
    NotConnected,
    AlreadyClosed,
    InvalidMessage,
    ConsumerNotInitialized,
    ProducerNotInitialized,
    TooManyLookupRequestException,
    InvalidTopicName,
    InvalidUrl,
    ServiceUnitNotReady,
    OperationNotSupported,
    ProducerBlockedQuotaExceededError,
    ProducerBlockedQuotaExceededException,
    ProducerQueueIsFull,
    MessageTooBig,
    TopicNotFound,
    SubscriptionNotFound,
    ConsumerNotFound,
    UnsupportedVersionError,
    TopicTerminated,
    CryptoError,
}

impl PulsarError {
    fn check(&self, value: pulsar_result) -> bool {
        value == *self as u32
    }
}

impl From<pulsar_result> for PulsarError {
    fn from(result: pulsar_result) -> Self {
        use PulsarError::*;
        match result {
            x if InvalidConfiguration.check(x) => InvalidConfiguration,
            x if Timeout.check(x) => Timeout,
            x if LookupError.check(x) => LookupError,
            x if ConnectError.check(x) => ConnectError,
            x if ReadError.check(x) => ReadError,
            x if AuthenticationError.check(x) => AuthenticationError,
            x if AuthorizationError.check(x) => AuthorizationError,
            x if ErrorGettingAuthenticationData.check(x) => ErrorGettingAuthenticationData,
            x if BrokerMetadataError.check(x) => BrokerMetadataError,
            x if BrokerPersistenceError.check(x) => BrokerPersistenceError,
            x if ChecksumError.check(x) => ChecksumError,
            x if ConsumerBusy.check(x) => ConsumerBusy,
            x if NotConnected.check(x) => NotConnected,
            x if AlreadyClosed.check(x) => AlreadyClosed,
            x if InvalidMessage.check(x) => InvalidMessage,
            x if ConsumerNotInitialized.check(x) => ConsumerNotInitialized,
            x if ProducerNotInitialized.check(x) => ProducerNotInitialized,
            x if TooManyLookupRequestException.check(x) => TooManyLookupRequestException,
            x if InvalidTopicName.check(x) => InvalidTopicName,
            x if InvalidUrl.check(x) => InvalidUrl,
            x if ServiceUnitNotReady.check(x) => ServiceUnitNotReady,
            x if OperationNotSupported.check(x) => OperationNotSupported,
            x if ProducerBlockedQuotaExceededError.check(x) => ProducerBlockedQuotaExceededError,
            x if ProducerBlockedQuotaExceededException.check(x) => {
                ProducerBlockedQuotaExceededException
            }
            x if ProducerQueueIsFull.check(x) => ProducerQueueIsFull,
            x if MessageTooBig.check(x) => MessageTooBig,
            x if TopicNotFound.check(x) => TopicNotFound,
            x if SubscriptionNotFound.check(x) => SubscriptionNotFound,
            x if ConsumerNotFound.check(x) => ConsumerNotFound,
            x if UnsupportedVersionError.check(x) => UnsupportedVersionError,
            x if TopicTerminated.check(x) => TopicTerminated,
            x if CryptoError.check(x) => CryptoError,
            _ => UnknownError,
        }
    }
}

impl fmt::Display for PulsarError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PulsarError::NulError => write!(f, "Pulsar client returned a null pointer"),
            PulsarError::InvalidString => write!(
                f,
                "Received invalid string: strings must not contain zero-bytes"
            ),
            PulsarError::InvalidDuration => write!(f, "Received invalid duration"),
            PulsarError::UnknownError => write!(f, "UnknownError"),
            PulsarError::InvalidConfiguration => write!(f, "InvalidConfiguration"),
            PulsarError::Timeout => write!(f, "TimeOut"),
            PulsarError::LookupError => write!(f, "LookupError"),
            PulsarError::ConnectError => write!(f, "ConnectError"),
            PulsarError::ReadError => write!(f, "ReadError"),
            PulsarError::AuthenticationError => write!(f, "AuthenticationError"),
            PulsarError::AuthorizationError => write!(f, "AuthorizationError"),
            PulsarError::ErrorGettingAuthenticationData => {
                write!(f, "ErrorGettingAuthenticationData")
            }
            PulsarError::BrokerMetadataError => write!(f, "BrokerMetadataError "),
            PulsarError::BrokerPersistenceError => write!(f, "BrokerPersistenceError "),
            PulsarError::ChecksumError => write!(f, "ChecksumError "),
            PulsarError::ConsumerBusy => write!(f, "ConsumerBusy "),
            PulsarError::NotConnected => write!(f, "NotConnected "),
            PulsarError::AlreadyClosed => write!(f, "AlreadyClosed "),
            PulsarError::InvalidMessage => write!(f, "InvalidMessage "),
            PulsarError::ConsumerNotInitialized => write!(f, "ConsumerNotInitialized "),
            PulsarError::ProducerNotInitialized => write!(f, "ProducerNotInitialized "),
            PulsarError::TooManyLookupRequestException => {
                write!(f, "TooManyLookupRequestException ")
            }
            PulsarError::InvalidTopicName => write!(f, "InvalidTopicName "),
            PulsarError::InvalidUrl => write!(f, "InvalidUrl "),
            PulsarError::ServiceUnitNotReady => write!(f, "ServiceUnitNotReady "),
            PulsarError::OperationNotSupported => write!(f, "OperationNotSupported "),
            PulsarError::ProducerBlockedQuotaExceededError => {
                write!(f, "ProducerBlockedQuotaExceededError ")
            }
            PulsarError::ProducerBlockedQuotaExceededException => {
                write!(f, "ProducerBlockedQuotaExceededException ")
            }
            PulsarError::ProducerQueueIsFull => write!(f, "ProducerQueueIsFull "),
            PulsarError::MessageTooBig => write!(f, "MessageTooBig "),
            PulsarError::TopicNotFound => write!(f, "TopicNotFound "),
            PulsarError::SubscriptionNotFound => write!(f, "SubscriptionNotFound "),
            PulsarError::ConsumerNotFound => write!(f, "ConsumerNotFound "),
            PulsarError::UnsupportedVersionError => write!(f, "UnsupportedVersionError "),
            PulsarError::TopicTerminated => write!(f, "TopicTerminated "),
            PulsarError::CryptoError => write!(f, "CryptoError "),
        }
    }
}

impl std::error::Error for PulsarError {}

pub type PulsarResult<T> = Result<T, PulsarError>;

pub trait IntoPulsarResult<T> {
    fn into_pulsar_result(self) -> PulsarResult<T>;
}

impl From<NulError> for PulsarError {
    fn from(_: NulError) -> Self {
        Self::NulError
    }
}

impl From<std::num::TryFromIntError> for PulsarError {
    fn from(_: std::num::TryFromIntError) -> Self {
        Self::InvalidDuration
    }
}

impl IntoPulsarResult<()> for pulsar_result {
    fn into_pulsar_result(self) -> PulsarResult<()> {
        if self == 0 {
            Ok(())
        } else {
            Err(self.into())
        }
    }
}
