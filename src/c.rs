use crate::bindings::*;

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
