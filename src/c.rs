use crate::authentication::Authentication;
use crate::bindings::*;
use crate::error::{IntoPulsarResult, PulsarError, PulsarResult};
use crate::logger::Logger;

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::ptr::NonNull;
use std::sync::Arc;

type LoggerFunc = unsafe extern "C" fn(u32, *const c_char, i32, *const c_char, *mut c_void);

pub(crate) struct CAuthentication {
    pub ptr: NonNull<auth::ptr>,
}

pub(crate) struct CClientConfiguration {
    pub ptr: NonNull<client_configuration::ptr>,
}

pub(crate) struct CMessage {
    pub ptr: NonNull<message::ptr>,
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
    client: ArcClient,
}

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
    (CClientConfiguration, client_configuration),
    (CProducerConfiguration, producer_config),
    (CProducer, producer),
    (CClient, client)
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
        self: Arc<Self>,
        topic: &CStr,
        config: &CProducerConfiguration,
    ) -> PulsarResult<CProducer> {
        let mut ptr = std::ptr::null_mut();

        let result = unsafe {
            client::create_producer(
                self.ptr.as_ptr(),
                topic.as_ptr(),
                config.ptr.as_ref(),
                &mut ptr,
            )
        };

        result
            .into_pulsar_result()
            .and_then(|_| match NonNull::new(ptr) {
                Some(p) => Ok(CProducer {
                    ptr: p,
                    client: self.clone(),
                }),
                None => Err(PulsarError::UnknownError),
            })
    }
}

impl CProducer {
    fn get_topic<'a>(&'a self) -> &'a CStr {
        unsafe {
            let ptr = producer::get_topic(self.ptr.as_ptr());
            CStr::from_ptr::<'a>(ptr)
        }
    }

    fn get_producer_name<'a>(&'a self) -> &'a CStr {
        unsafe {
            let ptr = producer::get_producer_name(self.ptr.as_ptr());
            CStr::from_ptr::<'a>(ptr)
        }
    }

    fn get_last_sequence_id(&self) -> i64 {
        unsafe { producer::get_last_sequence_id(self.ptr.as_ptr()) }
    }

    fn flush(&self) -> PulsarResult<()> {
        unsafe { producer::flush(self.ptr.as_ptr()).into_pulsar_result() }
    }

    fn close(&self) -> PulsarResult<()> {
        unsafe { producer::close(self.ptr.as_ptr()).into_pulsar_result() }
    }
}
