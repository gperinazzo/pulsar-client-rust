use crate::authentication::{Authentication, CAuthentication};
use std::boxed::Box;
use std::os::raw::{c_char, c_void};
use std::sync::Arc;

use crate::bindings::{client, client_configuration, logger};
use crate::error::{PulsarError, PulsarResult};
use crate::logger::{DefaultLogger, Level, Logger};
use std::ffi::{CStr, CString};

type LoggerFunc = unsafe extern "C" fn(u32, *const c_char, i32, *const c_char, *mut c_void);

pub struct ClientConfiguration<'a> {
    pub url: &'a str,
    pub auth: Option<Authentication<'a>>,
    pub operation_timeout_seconds: Option<i32>,
    pub io_threads: Option<i32>,
    pub message_listener_threads: Option<i32>,
    pub concurrent_lookup_requests: Option<i32>,
    pub logger: Option<(Box<dyn Logger>, LoggerFunc)>,
    pub tls_trust_certs_file_path: Option<&'a str>,
    pub tls_allow_insecure_connection: Option<bool>,
    pub tls_validate_hostname: Option<bool>,
    pub stats_interval_in_seconds: Option<u32>,
}

struct CClient {
    ptr: *mut client::ptr,

    // We only free these pointers after the client has
    // been dropped, as they may contain values that were
    // passed to the C++ library
    #[allow(dead_code)]
    auth: Option<CAuthentication>,
    #[allow(dead_code)]
    logger: Box<dyn Logger>,
}

impl Drop for CClient {
    fn drop(&mut self) {
        unsafe {
            client::free(self.ptr);
        }
    }
}

unsafe extern "C" fn logger_proxy<L: Logger>(
    level: u32,
    c_file: *const c_char,
    line: i32,
    c_message: *const c_char,
    ctx: *mut c_void,
) {
    let file = CStr::from_ptr(c_file);
    let message = CStr::from_ptr(c_message);
    let log_level = match level {
        logger::DEBUG => Level::Debug,
        logger::WARN => Level::Warn,
        logger::INFO => Level::Info,
        _ => Level::Error,
    };

    let logger: *mut L = std::mem::transmute(ctx);

    (*logger).log(
        log_level,
        file.to_str().unwrap(),
        line,
        message.to_str().unwrap(),
    );
}

#[derive(Clone)]
pub struct Client {
    internal: Arc<CClient>,
}

macro_rules! unsafe_if_let {
    ( $($var:pat = $field:expr => $code:expr,)*) => {
        $(if let $var = $field {
            unsafe { $code }
        })*
    };
}

impl Client {
    pub fn from_config(config: ClientConfiguration) -> PulsarResult<Self> {
        use client_configuration::*;
        let url = CString::new(config.url).map_err(|_| PulsarError::InvalidUrl)?;

        let auth = match config.auth {
            Some(ref auth) => Some(CAuthentication::new(auth)?),
            None => None,
        };

        let raw_config = unsafe { client_configuration::create() };
        unsafe_if_let! {
            Some(timeout) = config.operation_timeout_seconds => {
                set_operation_timeout_seconds(raw_config, timeout);
            },
            Some(threads) = config.io_threads => {
                set_io_threads(raw_config, threads);
            },
            Some(threads) = config.message_listener_threads => {
                set_message_listener_threads(raw_config,threads);
            },
            Some(amount) = config.concurrent_lookup_requests => {
                set_concurrent_lookup_request(raw_config, amount);
            },
            Some(seconds) = config.stats_interval_in_seconds => {
                set_stats_interval_in_seconds(raw_config, seconds);
            },
            Some(true) = config.tls_allow_insecure_connection => {
                set_tls_allow_insecure_connection(raw_config, 1);
            },
            Some(true) = config.tls_validate_hostname => {
                set_validate_hostname(raw_config, 1);
            },
            Some(ref cauth) = auth => {
                set_auth(raw_config, cauth.ptr);
            },
        }

        let (mut logger, func) = config
            .logger
            .unwrap_or_else(|| (Box::new(DefaultLogger {}), logger_proxy::<DefaultLogger>));
        unsafe {
            client_configuration::set_logger(
                raw_config,
                Some(func),
                &mut *logger as *mut dyn Logger as *mut c_void,
            )
        }

        let client_ptr = unsafe { client::create(url.as_ptr(), raw_config) };
        unsafe {
            client_configuration::free(raw_config);
        }

        Ok(Client {
            internal: Arc::new(CClient {
                logger,
                ptr: client_ptr,
                auth,
            }),
        })
    }
    pub fn from_url(url: &str) -> PulsarResult<Self> {
        let config = ClientConfiguration::new(url);
        Self::from_config(config)
    }

    pub fn test_producer(&self) {
        use crate::bindings::raw::{
            pulsar_client_create_producer, pulsar_producer_close,
            pulsar_producer_configuration_create, pulsar_producer_configuration_free,
            pulsar_producer_free, pulsar_producer_t,
        };
        let mut producer: *mut pulsar_producer_t = std::ptr::null_mut();
        let topic = CString::new("persistent://public/default/bla").unwrap();

        let result = unsafe {
            let config = pulsar_producer_configuration_create();
            let r = pulsar_client_create_producer(
                self.internal.ptr,
                topic.as_ptr(),
                config,
                &mut producer,
            );
            pulsar_producer_configuration_free(config);
            r
        };

        if result == 0 {
            unsafe {
                pulsar_producer_close(producer);
                pulsar_producer_free(producer);
            }
        }
    }
}

impl<'a> ClientConfiguration<'a> {
    pub fn new(url: &'a str) -> Self {
        ClientConfiguration {
            url,
            auth: None,
            operation_timeout_seconds: None,
            io_threads: None,
            message_listener_threads: None,
            concurrent_lookup_requests: None,
            logger: None,
            tls_trust_certs_file_path: None,
            tls_allow_insecure_connection: None,
            tls_validate_hostname: None,
            stats_interval_in_seconds: None,
        }
    }

    pub fn with_operation_timeout_seconds(mut self, timeout: i32) -> Self {
        self.operation_timeout_seconds = Some(timeout);
        self
    }
    pub fn with_auth(mut self, auth: Authentication<'a>) -> Self {
        self.auth = Some(auth);
        self
    }
    pub fn with_concurrent_lookup_requests(mut self, num: i32) -> Self {
        self.concurrent_lookup_requests = Some(num);
        self
    }
    pub fn with_io_threads(mut self, num: i32) -> Self {
        self.io_threads = Some(num);
        self
    }
    pub fn with_logger<T: 'static + Logger>(mut self, logger: T) -> Self {
        self.logger = Some((Box::new(logger), logger_proxy::<T>));
        self
    }
    pub fn with_trust_certs_file_path(mut self, path: &'a str) -> Self {
        self.tls_trust_certs_file_path = Some(path);
        self
    }

    pub fn allow_insecure_connections(mut self) -> Self {
        self.tls_allow_insecure_connection = Some(true);
        self
    }

    pub fn validate_hostname(mut self) -> Self {
        self.tls_validate_hostname = Some(true);
        self
    }

    pub fn with_stats_interval(mut self, seconds: u32) -> Self {
        self.stats_interval_in_seconds = Some(seconds);
        self
    }

    pub fn client(self) -> PulsarResult<Client> {
        Client::from_config(self)
    }
}
