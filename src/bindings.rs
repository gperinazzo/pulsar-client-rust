#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#[allow(dead_code)]
pub mod raw {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub mod auth {
    pub use super::raw::{
        pulsar_authentication_athenz_create as athenz_create,
        pulsar_authentication_create as create, pulsar_authentication_free as free,
        pulsar_authentication_t as authentication, pulsar_authentication_tls_create as tls_create,
        pulsar_authentication_token_create as token_create,
    };
}

pub mod client_configuration {
    pub use super::raw::{
        pulsar_client_configuration_create as create, pulsar_client_configuration_free as free,
        pulsar_client_configuration_set_auth as set_auth,
        pulsar_client_configuration_set_concurrent_lookup_request as set_concurrent_lookup_request,
        pulsar_client_configuration_set_io_threads as set_io_threads,
        pulsar_client_configuration_set_logger as set_logger,
        pulsar_client_configuration_set_message_listener_threads as set_message_listener_threads,
        pulsar_client_configuration_set_operation_timeout_seconds as set_operation_timeout_seconds,
        pulsar_client_configuration_set_stats_interval_in_seconds as set_stats_interval_in_seconds,
        pulsar_client_configuration_set_tls_allow_insecure_connection as set_tls_allow_insecure_connection,
        pulsar_client_configuration_set_tls_trust_certs_file_path as set_tls_trust_certs_file_path,
        pulsar_client_configuration_set_use_tls as set_use_tls,
        pulsar_client_configuration_set_validate_hostname as set_validate_hostname,
        pulsar_client_configuration_t as ptr,
    };

}

pub mod client {
    pub use super::raw::{
        pulsar_client_create as create, pulsar_client_create_producer as create_producer,
        pulsar_client_create_producer_async as create_producer_async, pulsar_client_free as free,
        pulsar_client_subscribe as subscribe, pulsar_client_subscribe_async as subscribe_async,
        pulsar_client_subscribe_pattern_async as subscribe_pattern_async, pulsar_client_t as ptr,
    };
}

pub mod producer {
    pub use super::raw::{
        pulsar_producer_close as close, pulsar_producer_flush as flush,
        pulsar_producer_flush_async as flush_async, pulsar_producer_free as free,
        pulsar_producer_get_last_sequence_id as get_last_sequence_id,
        pulsar_producer_get_producer_name as get_producer_name,
        pulsar_producer_get_topic as get_topic, pulsar_producer_send as send,
        pulsar_producer_send_async as send_async, pulsar_producer_t as producer,
    };
}

pub mod logger {
    pub use super::raw::{
        pulsar_logger, pulsar_logger_level_t_pulsar_DEBUG as DEBUG,
        pulsar_logger_level_t_pulsar_ERROR as ERROR, pulsar_logger_level_t_pulsar_INFO as INFO,
        pulsar_logger_level_t_pulsar_WARN as WARN,
    };

}
