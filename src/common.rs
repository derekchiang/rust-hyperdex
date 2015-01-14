use std::ffi::{CString, c_str_to_bytes};

use hyperdex_admin::*;
use hyperdex_client::*;

#[deriving(Show)]
pub struct HyperError {
    pub status: u32,
    pub message: String,
    pub location: String,
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
    let bytes = c_str_to_bytes(ptr).into_vec();
    String::from_utf8(bytes).unwrap()
}

pub trait ToCStr {
    fn to_c_str(self) -> CString;
}

impl<T: ToString> ToCStr for T {
    fn to_c_str(self) -> CString {
        CString::from_vec(self.to_string().into_bytes())
    }
}

impl ToCStr for Vec<u8> {
    fn to_c_str(self) -> CString {
        CString::from_vec(self)
    }
}

