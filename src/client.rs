use crate::authentication::Authentication;
use crate::c::{ArcClient, CAuthentication, CClient, CClientConfiguration, CProducerConfiguration};
use crate::producer::{Producer, ProducerBuilder};
use std::boxed::Box;
use std::os::raw::{c_char, c_void};

use crate::bindings::logger;
use crate::error::{PulsarError, PulsarResult};
use crate::logger::{DefaultLogger, Level, Logger};
use std::ffi::{CStr, CString};

type LoggerFunc = unsafe extern "C" fn(u32, *const c_char, i32, *const c_char, *mut c_void);

pub struct ClientBuilder<'a> {
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

unsafe extern "C" fn logger_proxy<L: 'static + Sync + Logger>(
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
    internal: ArcClient,
}

impl Client {
    pub fn from_builder(config: ClientBuilder) -> PulsarResult<Self> {
        let url = CString::new(config.url).map_err(|_| PulsarError::InvalidUrl)?;

        let mut auth = match config.auth {
            Some(ref auth) => Some(CAuthentication::new(auth)?),
            None => None,
        };

        let mut raw_config = CClientConfiguration::new()?;
        if let Some(timeout) = config.operation_timeout_seconds {
            raw_config.set_operation_timeout_seconds(timeout);
        }
        if let Some(threads) = config.io_threads {
            raw_config.set_io_threads(threads);
        }
        if let Some(threads) = config.message_listener_threads {
            raw_config.set_message_listener_threads(threads);
        }
        if let Some(amount) = config.concurrent_lookup_requests {
            raw_config.set_concurrent_lookup_requests(amount);
        }
        if let Some(seconds) = config.stats_interval_in_seconds {
            raw_config.set_stats_interval_in_seconds(seconds);
        }
        if let Some(true) = config.tls_allow_insecure_connection {
            raw_config.set_allow_insecure_connection();
        }
        if let Some(true) = config.tls_validate_hostname {
            raw_config.set_validate_hostname();
        }
        if let Some(ref mut cauth) = auth {
            raw_config.set_auth(cauth);
        }

        let (mut logger, func) = config
            .logger
            .unwrap_or_else(|| (Box::new(DefaultLogger {}), logger_proxy::<DefaultLogger>));
        unsafe { raw_config.set_logger(func, &mut *logger as *mut dyn Logger as *mut c_void) }

        let internal_client = CClient::new(&url, raw_config, logger, auth)?;

        Ok(Client {
            internal: internal_client,
        })
    }
    pub fn from_url(url: &str) -> PulsarResult<Self> {
        let config = ClientBuilder::new(url);
        Self::from_builder(config)
    }

    pub fn create_producer_from_builder<'a, 'c>(
        &'c self,
        config: ProducerBuilder<'a>,
    ) -> PulsarResult<Producer<'c>> {
        let (topic, config) = Producer::create_producer_configuration(config)?;

        let producer = self.internal.create_producer(&topic, &config)?;

        Ok(Producer::from_internal(producer))
    }

    pub fn create_producer<'a, 'c>(&'c self, topic: &str) -> PulsarResult<Producer<'c>> {
        let builder = ProducerBuilder::new(topic);
        self.create_producer_from_builder(builder)
    }
}

impl<'a> ClientBuilder<'a> {
    pub fn new(url: &'a str) -> Self {
        Self {
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
    pub fn with_logger<T: 'static + Logger + Sync>(mut self, logger: T) -> Self {
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

    pub fn build(self) -> PulsarResult<Client> {
        Client::from_builder(self)
    }
}
