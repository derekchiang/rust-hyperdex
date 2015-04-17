use std::ffi::{CStr, CString};
use std::fmt::{Display, Formatter, Error};

use hyperdex_admin::*;
use hyperdex_client::*;

/// An error related to HyperDex.
#[derive(Debug, Clone)]
pub struct HyperError {
    pub status: u32,
    pub message: String,
    pub location: String,
}

impl Display for HyperError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        Display::fmt(format!("HyperDex error:\n\tstatus: {}\n\tmessage: {}\n\tlocation: {}\n",
                             self.status, self.message, self.location).as_str(), f)
    }
}

pub fn get_admin_error(admin: *mut Struct_hyperdex_admin, status: u32) -> HyperError {
    unsafe {
        HyperError {
            status: status,
            message: to_string(hyperdex_admin_error_message(admin)),
            location: to_string(hyperdex_admin_error_location(admin)),
        }
    }
}

pub fn get_client_error(client: *mut Struct_hyperdex_client, status: u32) -> HyperError {
    unsafe {
        HyperError {
            status: status,
            message: to_string(hyperdex_client_error_message(client)),
            location: to_string(hyperdex_client_error_location(client)),
        }
    }
}

pub unsafe fn to_bytes_with_len(ptr: *const ::libc::c_char, len: u64) -> Vec<u8> {
    return Vec::from_raw_buf(ptr as *const u8, len as usize);
}

pub unsafe fn to_string(ptr: *const ::libc::c_char) -> String {
    let cstr = CStr::from_ptr(ptr);
    String::from_utf8(cstr.to_bytes().to_vec()).unwrap()
}

pub trait ToCStr {
    fn to_c_str(self) -> CString;
}

impl ToCStr for Vec<u8> {
    fn to_c_str(self) -> CString {
        unsafe {
            CString::from_vec_unchecked(self)
        }
    }
}

impl ToCStr for String {
    fn to_c_str(self) -> CString {
        CString::new(self).unwrap()
    }
}

impl<'a> ToCStr for &'a str {
    fn to_c_str(self) -> CString {
        self.to_string().to_c_str()
    }
}

// impl<T: ToString> ToCStr for T {
    // fn to_c_str(self) -> CString {
        // CString::from_vec(self.to_string().into_bytes())
    // }
// }
