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
        pulsar_authentication_t as ptr, pulsar_authentication_tls_create as tls_create,
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
        pulsar_client_close_async as close_async, pulsar_client_create as create,
        pulsar_client_create_producer as create_producer,
        pulsar_client_create_producer_async as create_producer_async, pulsar_client_free as free,
        pulsar_client_subscribe as subscribe, pulsar_client_subscribe_async as subscribe_async,
        pulsar_client_subscribe_pattern_async as subscribe_pattern_async, pulsar_client_t as ptr,
    };
}

pub mod producer {
    pub use super::raw::{
        pulsar_producer_close as close, pulsar_producer_close_async as close_async,
        pulsar_producer_flush as flush, pulsar_producer_flush_async as flush_async,
        pulsar_producer_free as free, pulsar_producer_get_last_sequence_id as get_last_sequence_id,
        pulsar_producer_get_producer_name as get_producer_name,
        pulsar_producer_get_topic as get_topic, pulsar_producer_send as send,
        pulsar_producer_send_async as send_async, pulsar_producer_t as ptr,
    };
}

pub mod producer_config {
    pub use super::raw::{
        pulsar_producer_configuration_create as create, pulsar_producer_configuration_free as free,
        pulsar_producer_configuration_get_batching_enabled as get_batching_enabled,
        pulsar_producer_configuration_get_batching_max_allowed_size_in_bytes as get_batching_max_allowed_size_in_bytes,
        pulsar_producer_configuration_get_batching_max_messages as get_batching_max_messages,
        pulsar_producer_configuration_get_batching_max_publish_delay_ms as get_batching_max_publish_delay_ms,
        pulsar_producer_configuration_get_block_if_queue_full as get_block_if_queue_full,
        pulsar_producer_configuration_get_compression_type as get_compression_type,
        pulsar_producer_configuration_get_hashing_scheme as get_hashing_scheme,
        pulsar_producer_configuration_get_initial_sequence_id as get_initial_sequence_id,
        pulsar_producer_configuration_get_max_pending_messages as get_max_pending_messages,
        pulsar_producer_configuration_get_max_pending_messages_across_partitions as get_max_pending_messages_across_partitions,
        pulsar_producer_configuration_get_partitions_routing_mode as get_partitions_routing_mode,
        pulsar_producer_configuration_get_producer_name as get_producer_name,
        pulsar_producer_configuration_get_send_timeout as get_send_timeout,
        pulsar_producer_configuration_set_batching_enabled as set_batching_enabled,
        pulsar_producer_configuration_set_batching_max_allowed_size_in_bytes as set_batching_max_allowed_size_in_bytes,
        pulsar_producer_configuration_set_batching_max_messages as set_batching_max_messages,
        pulsar_producer_configuration_set_batching_max_publish_delay_ms as set_batching_max_publish_delay_ms,
        pulsar_producer_configuration_set_block_if_queue_full as set_block_if_queue_full,
        pulsar_producer_configuration_set_compression_type as set_compression_type,
        pulsar_producer_configuration_set_hashing_scheme as set_hashing_scheme,
        pulsar_producer_configuration_set_initial_sequence_id as set_initial_sequence_id,
        pulsar_producer_configuration_set_max_pending_messages as set_max_pending_messages,
        pulsar_producer_configuration_set_max_pending_messages_across_partitions as set_max_pending_messages_across_partitions,
        pulsar_producer_configuration_set_message_router as set_message_router,
        pulsar_producer_configuration_set_partitions_routing_mode as set_partitions_routing_mode,
        pulsar_producer_configuration_set_producer_name as set_producer_name,
        pulsar_producer_configuration_set_property as set_property,
        pulsar_producer_configuration_set_schema_info as set_schema_info,
        pulsar_producer_configuration_set_send_timeout as set_send_timeout,
        pulsar_producer_configuration_t as ptr,
    };
}

pub mod logger {
    pub use super::raw::{
        pulsar_logger, pulsar_logger_level_t_pulsar_DEBUG as DEBUG,
        pulsar_logger_level_t_pulsar_ERROR as ERROR, pulsar_logger_level_t_pulsar_INFO as INFO,
        pulsar_logger_level_t_pulsar_WARN as WARN,
    };
}

pub mod message {
    pub use super::raw::{
        pulsar_message_create as create, pulsar_message_disable_replication as disable_replication,
        pulsar_message_free as free, pulsar_message_get_data as get_data,
        pulsar_message_get_length as get_length, pulsar_message_get_message_id as get_message_id,
        pulsar_message_get_orderingKey as get_ordering_key,
        pulsar_message_get_partitionKey as get_partition_key,
        pulsar_message_get_properties as get_properties,
        pulsar_message_get_property as get_property,
        pulsar_message_get_publish_timestamp as get_publish_timestamp,
        pulsar_message_get_topic_name as get_topic_name,
        pulsar_message_has_ordering_key as has_ordering_key,
        pulsar_message_has_partition_key as has_partition_key,
        pulsar_message_has_property as has_property,
        pulsar_message_set_allocated_content as set_allocated_content,
        pulsar_message_set_content as set_content,
        pulsar_message_set_event_timestamp as set_event_timestamp,
        pulsar_message_set_ordering_key as set_ordering_key,
        pulsar_message_set_partition_key as set_partition_key,
        pulsar_message_set_property as set_property,
        pulsar_message_set_replication_clusters as set_replication_clusters,
        pulsar_message_set_sequence_id as set_sequence_id, pulsar_message_t as ptr,
    };
}
pub mod message_id {
    pub use super::raw::{
        pulsar_message_id_deserialize as deserialize, pulsar_message_id_earliest as earliest,
        pulsar_message_id_free as free, pulsar_message_id_latest as latest,
        pulsar_message_id_serialize as serialize, pulsar_message_id_str as as_str,
        pulsar_message_id_t as ptr,
    };
}
