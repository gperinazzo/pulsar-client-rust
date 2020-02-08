use crate::bindings::raw::pulsar_result;
use std::convert::From;
use std::ffi::NulError;

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
