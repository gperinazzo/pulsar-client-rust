use crate::authentication::Authentication;
use crate::bindings::*;
use crate::error::{IntoPulsarResult, PulsarError, PulsarResult};
use crate::logger::Logger;

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::ptr::NonNull;
use std::sync::Arc;

use futures::channel::oneshot::{self, Sender};
use log::error;

type LoggerFunc = unsafe extern "C" fn(u32, *const c_char, i32, *const c_char, *mut c_void);

#[derive(Clone, Copy)]
pub enum CompressionType {
    None = 0,
    LZ4 = 1,
    ZLib = 2,
}

#[derive(Clone, Copy)]
pub enum RoutingMode {
    UseSinglePartiton = 0,
    RoundRobinDistribution = 1,
    CustomPartitions = 2,
}

#[derive(Clone, Copy)]
pub enum HashingScheme {
    Murmur3 = 0,
    Boost = 1,
    JavaString = 2,
}

pub(crate) struct CAuthentication {
    pub ptr: NonNull<auth::ptr>,
}

pub(crate) struct CClientConfiguration {
    pub ptr: NonNull<client_configuration::ptr>,
}

pub(crate) struct CMessage {
    pub ptr: NonNull<message::ptr>,
}

pub(crate) struct CMessageId {
    pub ptr: NonNull<message_id::ptr>,
}

pub(crate) struct CClient {
    pub ptr: NonNull<client::ptr>,

    // We only free these pointers after the client has
    // been dropped, as they may contain values that were
    // passed to the C++ library
    #[allow(dead_code)]
    auth: Option<CAuthentication>,
    #[allow(dead_code)]
    logger: Box<dyn Logger>,
}

unsafe impl Send for CClient {}
unsafe impl Sync for CClient {}

pub(crate) type ArcClient = Arc<CClient>;

pub(crate) struct CProducerConfiguration {
    ptr: NonNull<producer_config::ptr>,
}

pub(crate) struct CProducer {
    ptr: NonNull<producer::ptr>,
    #[allow(dead_code)]
    client: ArcClient,
}

unsafe impl Send for CProducer {}
unsafe impl Sync for CProducer {}

pub(crate) type ArcProducer = Arc<CProducer>;

macro_rules! drop_ptr {
    ($(($struct:ident, $mod:ident)),*) => {
        $(
            impl Drop for $struct {
                fn drop(&mut self) {
                    unsafe {
                        $mod::free(self.ptr.as_ptr());
                    }
                }
            }
        )*
    };
}

drop_ptr! {
    (CAuthentication, auth),
    (CMessage, message),
    (CMessageId, message_id),
    (CClientConfiguration, client_configuration),
    (CProducerConfiguration, producer_config),
    (CClient, client),
    (CProducer, producer)
}

impl CAuthentication {
    pub(crate) fn new(auth_type: &Authentication) -> PulsarResult<Self> {
        let ptr = match *auth_type {
            Authentication::Tls {
                certificate_path,
                private_key_path,
            } => {
                let c_certificate = CString::new(&*certificate_path)?;
                let c_key = CString::new(&*private_key_path)?;
                unsafe { auth::tls_create(c_certificate.as_ptr(), c_key.as_ptr()) }
            }
            Authentication::Token(token) => {
                let c_token = CString::new(&*token)?;
                unsafe { auth::token_create(c_token.as_ptr()) }
            }
            Authentication::Athenz(params) => {
                let c_params = CString::new(&*params)?;
                unsafe { auth::athenz_create(c_params.as_ptr()) }
            }
        };

        match NonNull::new(ptr) {
            Some(p) => Ok(CAuthentication { ptr: p }),
            None => Err(PulsarError::UnknownError),
        }
    }
}

impl CClientConfiguration {
    pub(crate) fn new() -> PulsarResult<Self> {
        let ptr = NonNull::new(unsafe { client_configuration::create() });

        match ptr {
            Some(p) => Ok(Self { ptr: p }),
            None => Err(PulsarError::UnknownError),
        }
    }

    pub(crate) fn set_io_threads(&mut self, threads: i32) {
        unsafe {
            client_configuration::set_io_threads(self.ptr.as_ptr(), threads);
        }
    }

    pub(crate) fn set_operation_timeout_seconds(&mut self, seconds: i32) {
        unsafe {
            client_configuration::set_operation_timeout_seconds(self.ptr.as_ptr(), seconds);
        }
    }

    pub(crate) fn set_message_listener_threads(&mut self, threads: i32) {
        unsafe {
            client_configuration::set_message_listener_threads(self.ptr.as_ptr(), threads);
        }
    }

    pub(crate) fn set_concurrent_lookup_requests(&mut self, amount: i32) {
        unsafe {
            client_configuration::set_concurrent_lookup_request(self.ptr.as_ptr(), amount);
        }
    }

    pub(crate) fn set_stats_interval_in_seconds(&mut self, seconds: u32) {
        unsafe {
            client_configuration::set_stats_interval_in_seconds(self.ptr.as_ptr(), seconds);
        }
    }

    pub(crate) fn set_allow_insecure_connection(&mut self) {
        unsafe {
            client_configuration::set_tls_allow_insecure_connection(self.ptr.as_ptr(), 1);
        }
    }

    pub(crate) fn set_validate_hostname(&mut self) {
        unsafe {
            client_configuration::set_validate_hostname(self.ptr.as_ptr(), 1);
        }
    }

    pub(crate) fn set_auth(&mut self, auth: &mut CAuthentication) {
        unsafe {
            client_configuration::set_auth(self.ptr.as_ptr(), auth.ptr.as_ptr());
        }
    }

    pub(crate) unsafe fn set_logger(&mut self, func: LoggerFunc, ctx: *mut c_void) {
        client_configuration::set_logger(self.ptr.as_ptr(), Some(func), ctx);
    }
}

impl CProducerConfiguration {
    pub(crate) fn new() -> PulsarResult<Self> {
        let ptr = NonNull::new(unsafe { producer_config::create() });

        match ptr {
            Some(p) => Ok(CProducerConfiguration { ptr: p }),
            None => Err(PulsarError::UnknownError),
        }
    }

    pub(crate) fn set_name(&self, name: &CStr) {
        unsafe {
            producer_config::set_producer_name(self.ptr.as_ptr(), name.as_ptr());
        }
    }

    pub(crate) fn set_send_timeout(&self, timeout: i32) {
        unsafe {
            producer_config::set_send_timeout(self.ptr.as_ptr(), timeout);
        }
    }

    pub(crate) fn set_initial_sequence_id(&self, initial_id: i64) {
        unsafe {
            producer_config::set_initial_sequence_id(self.ptr.as_ptr(), initial_id);
        }
    }

    pub(crate) fn set_compression_type(&self, compression: CompressionType) {
        unsafe {
            producer_config::set_compression_type(self.ptr.as_ptr(), compression as u32);
        }
    }

    pub(crate) fn set_max_pending_messages(&self, max: i32) {
        unsafe {
            producer_config::set_max_pending_messages(self.ptr.as_ptr(), max);
        }
    }

    pub(crate) fn set_max_pending_messages_across_partitions(&self, max: i32) {
        unsafe {
            producer_config::set_max_pending_messages_across_partitions(self.ptr.as_ptr(), max);
        }
    }

    pub(crate) fn set_partitions_routing_mode(&self, mode: RoutingMode) {
        unsafe {
            producer_config::set_partitions_routing_mode(self.ptr.as_ptr(), mode as u32);
        }
    }

    pub(crate) fn set_hashing_scheme(&self, scheme: HashingScheme) {
        unsafe {
            producer_config::set_hashing_scheme(self.ptr.as_ptr(), scheme as u32);
        }
    }

    pub(crate) fn set_block_if_queue_full(&self, block: bool) {
        unsafe {
            producer_config::set_block_if_queue_full(self.ptr.as_ptr(), i32::from(block));
        }
    }

    pub(crate) fn set_batching_enabled(&self, enabled: bool) {
        unsafe {
            producer_config::set_batching_enabled(self.ptr.as_ptr(), i32::from(enabled));
        }
    }

    pub(crate) fn set_batching_max_messages(&self, max: u32) {
        unsafe {
            producer_config::set_batching_max_messages(self.ptr.as_ptr(), max);
        }
    }

    pub(crate) fn set_batching_max_allowed_size_in_bytes(&self, max: u64) {
        unsafe {
            producer_config::set_batching_max_allowed_size_in_bytes(self.ptr.as_ptr(), max);
        }
    }

    pub(crate) fn set_batching_max_publish_delay_ms(&self, max: u64) {
        unsafe {
            producer_config::set_batching_max_publish_delay_ms(self.ptr.as_ptr(), max);
        }
    }

    pub(crate) fn set_property(&self, name: &CStr, value: &CStr) {
        unsafe {
            producer_config::set_property(self.ptr.as_ptr(), name.as_ptr(), value.as_ptr());
        }
    }
}

unsafe extern "C" fn close_proxy(
    result: raw::pulsar_result,
    ctx: *mut c_void,
) {
    let sender_ptr: *mut Sender<PulsarResult<()>> = std::mem::transmute(ctx);
    let sender = Box::from_raw(sender_ptr);

    if let Err(_) = (*sender).send(result.into_pulsar_result()) {
        error!("close_proxy failed to send");
    }
    
}

unsafe extern "C" fn flush_proxy(
    result: raw::pulsar_result,
    ctx: *mut c_void,
) {
    let sender_ptr: *mut Sender<PulsarResult<()>> = std::mem::transmute(ctx);
    let sender = Box::from_raw(sender_ptr);

    if let Err(_) = (*sender).send(result.into_pulsar_result()) {
        error!("flush_proxy failed to send");
    }
    
}

unsafe extern "C" fn create_producer_proxy(
    result: raw::pulsar_result,
    prod: *mut producer::ptr,
    ctx: *mut c_void,
) {
    let sender_ptr: *mut Sender<PulsarResult<*mut producer::ptr>> = std::mem::transmute(ctx);
    let sender = Box::from_raw(sender_ptr);

    let result = result.into_pulsar_result().map(|_| prod);

    if let Err(value) = (*sender).send(result) {
        error!("create_producer_proxy failed to send producer");
        value.map(|p| producer::free(p));
    }
}

impl CClient {
    pub(crate) fn new(
        url: &CStr,
        config: CClientConfiguration,
        logger: Box<dyn Logger>,
        auth: Option<CAuthentication>,
    ) -> PulsarResult<ArcClient> {
        let ptr = unsafe { client::create(url.as_ptr(), config.ptr.as_ptr()) };

        match NonNull::new(ptr) {
            Some(p) => Ok(Arc::new(Self {
                ptr: p,
                auth,
                logger,
            })),
            None => Err(PulsarError::UnknownError),
        }
    }

    pub(crate) fn create_producer(
        arc_client: &ArcClient,
        topic: &CStr,
        config: &CProducerConfiguration,
    ) -> PulsarResult<ArcProducer> {
        let mut ptr = std::ptr::null_mut();

        let result = unsafe {
            client::create_producer(
                arc_client.ptr.as_ptr(),
                topic.as_ptr(),
                config.ptr.as_ref(),
                &mut ptr,
            )
        };

        result
            .into_pulsar_result()
            .and_then(|_| match NonNull::new(ptr) {
                Some(p) => Ok(Arc::new(CProducer {
                    ptr: p,
                    client: arc_client.clone(),
                })),
                None => Err(PulsarError::UnknownError),
            })
    }

    pub(crate) async fn create_producer_async(
        arc_client: &ArcClient,
        topic: &CStr,
        config: &CProducerConfiguration,
    ) -> PulsarResult<ArcProducer> {
        let (sender, receiver) = oneshot::channel();
        let boxed_sender = Box::new(sender);

        unsafe {
            client::create_producer_async(
                arc_client.ptr.as_ptr(),
                topic.as_ptr(),
                config.ptr.as_ref(),
                Some(create_producer_proxy),
                Box::into_raw(boxed_sender) as *mut c_void,
            )
        };
        let result = receiver.await.unwrap_or(Err(PulsarError::UnknownError));

        result
            .and_then(|ptr| match NonNull::new(ptr) {
                Some(p) => Ok(Arc::new(CProducer {
                    ptr: p,
                    client: arc_client.clone(),
                })),
                None => Err(PulsarError::UnknownError),
            })
    }

    pub(crate) async fn close_async(
        &self
    ) -> PulsarResult<()> {
        let (sender, receiver) = oneshot::channel();
        let boxed_sender = Box::new(sender);
        unsafe {
            client::close_async(self.ptr.as_ptr(), Some(close_proxy), Box::into_raw(boxed_sender) as *mut c_void)
        };
        receiver.await.unwrap_or(Err(PulsarError::UnknownError))
    }
}

unsafe extern "C" fn send_proxy(
    result: raw::pulsar_result,
    id: *mut message_id::ptr,
    ctx: *mut c_void,
) {
    let sender_ptr: *mut Sender<PulsarResult<()>> = std::mem::transmute(ctx);
    let sender = Box::from_raw(sender_ptr);
    if !id.is_null() {
        message_id::free(id);
    }

    if let Err (_) = (*sender).send(result.into_pulsar_result()) {
        error!("send_proxy failed to send");
    }
}

impl CProducer {
    pub(crate) fn get_topic<'a>(&'a self) -> &'a CStr {
        unsafe {
            let ptr = producer::get_topic(self.ptr.as_ptr());
            CStr::from_ptr::<'a>(ptr)
        }
    }

    pub(crate) fn get_producer_name<'a>(&'a self) -> &'a CStr {
        unsafe {
            let ptr = producer::get_producer_name(self.ptr.as_ptr());
            CStr::from_ptr::<'a>(ptr)
        }
    }

    pub(crate) fn get_last_sequence_id(&self) -> i64 {
        unsafe { producer::get_last_sequence_id(self.ptr.as_ptr()) }
    }

    pub(crate) fn flush(&self) -> PulsarResult<()> {
        unsafe { producer::flush(self.ptr.as_ptr()).into_pulsar_result() }
    }

    pub (crate) async fn flush_async(&self) -> PulsarResult<()> {
        let (sender, receiver) = oneshot::channel();
        let boxed_sender = Box::new(sender);
        unsafe {
            producer::flush_async(
                self.ptr.as_ptr(),
                Some(flush_proxy),
                Box::into_raw(boxed_sender) as *mut c_void,
            );
        }

        receiver.await.unwrap_or(Err(PulsarError::UnknownError))
        
    }

    pub(crate) fn send(&self, message: &CMessage) -> PulsarResult<()> {
        unsafe { producer::send(self.ptr.as_ptr(), message.ptr.as_ptr()).into_pulsar_result() }
    }

    pub(crate) async fn send_async(&self, message: &CMessage) -> PulsarResult<()> {
        let (sender, receiver) = oneshot::channel();
        let boxed_sender = Box::new(sender);
        unsafe {
            producer::send_async(
                self.ptr.as_ptr(),
                message.ptr.as_ptr(),
                Some(send_proxy),
                Box::into_raw(boxed_sender) as *mut c_void,
            );
        }

        receiver.await.unwrap_or(Err(PulsarError::UnknownError))
    }

    pub(crate) fn close(&self) -> PulsarResult<()> {
        unsafe { producer::close(self.ptr.as_ptr()).into_pulsar_result() }
    }

    pub(crate) async fn close_async(
        &self
    ) -> PulsarResult<()> {
        let (sender, receiver) = oneshot::channel();
        let boxed_sender = Box::new(sender);
        unsafe {
            producer::close_async(self.ptr.as_ptr(), Some(close_proxy), Box::into_raw(boxed_sender) as *mut c_void)
        };
        receiver.await.unwrap_or(Err(PulsarError::UnknownError))
    }

}

impl CMessageId {
    unsafe fn from_ptr(ptr: *mut message_id::ptr) -> Self {
        match NonNull::new(ptr) {
            Some(p) => CMessageId { ptr: p },
            None => panic!("CMessageId from_ptr called with null pointer"),
        }
    }
}

impl CMessage {
    pub(crate) fn new() -> Self {
        let ptr = unsafe { message::create() };
        match NonNull::new(ptr) {
            Some(p) => Self { ptr: p },
            None => panic!("pulsar_message_create returned null pointer"),
        }
    }

    pub(crate) fn set_content<'a>(&mut self, payload: &'a [u8]) {
        let size = payload.len();
        let payload_ptr = payload.as_ptr();
        unsafe {
            message::set_content(self.ptr.as_ptr(), payload_ptr as *mut c_void, size);
        }
    }

    pub(crate) fn get_data<'s>(&'s self) -> &'s [u8] {
        let length = unsafe { message::get_length(self.ptr.as_ptr()) };
        if length == 0 {
            &[]
        } else {
            let ptr = unsafe { message::get_data(self.ptr.as_ptr()) };
            if ptr.is_null() {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(ptr as *const u8, length as usize) }
            }
        }
    }

    pub(crate) fn set_partition_key(&mut self, key: &CStr) {
        unsafe { message::set_partition_key(self.ptr.as_ptr(), key.as_ptr()) };
    }

    pub(crate) fn set_sequence_id(&mut self, id: i64) {
        unsafe { message::set_sequence_id(self.ptr.as_ptr(), id) };
    }

    pub(crate) fn set_property(&mut self, key: &CStr, value: &CStr) {
        unsafe { message::set_property(self.ptr.as_ptr(), key.as_ptr(), value.as_ptr()) }
    }

    pub(crate) fn get_property<'s>(&'s self, key: &CStr) -> Option<&'s CStr> {
        let has_property = unsafe { message::has_property(self.ptr.as_ptr(), key.as_ptr()) };
        if has_property != 0 {
            let value = unsafe {
                let ptr = message::get_property(self.ptr.as_ptr(), key.as_ptr());
                CStr::from_ptr(ptr)
            };
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn get_topic<'s>(&'s self) -> &'s CStr {
        unsafe {
            let ptr = message::get_topic_name(self.ptr.as_ptr());
            CStr::from_ptr(ptr)
        }
    }

    pub(crate) fn get_message_id(&self) -> CMessageId {
        unsafe {
            let ptr = message::get_message_id(self.ptr.as_ptr());
            CMessageId::from_ptr(ptr)
        }
    }
}
