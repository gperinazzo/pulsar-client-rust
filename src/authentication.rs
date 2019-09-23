use crate::bindings::auth;
use crate::error::PulsarResult;

use std::ffi::CString;

pub enum Authentication<'a> {
    Tls {
        certificate_path: &'a str,
        private_key_path: &'a str,
    },
    Token(&'a str),
    Athenz(&'a str),
}

pub struct CAuthentication {
    pub ptr: *mut auth::authentication,
}

impl Drop for CAuthentication {
    fn drop(&mut self) {
        unsafe {
            auth::free(self.ptr);
        }
    }
}

impl CAuthentication {
    pub fn new(auth_type: &Authentication) -> PulsarResult<Self> {
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

        Ok(CAuthentication { ptr })
    }
}
