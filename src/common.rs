use std::c_str::CString;
use std::c_vec::CVec;

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

pub unsafe fn to_bytes(ptr: *const ::libc::c_char) -> Vec<u8> {
    CString::new(ptr, false).as_bytes().to_vec()
}

pub unsafe fn to_bytes_with_len(ptr: *const ::libc::c_char, len: u64) -> Vec<u8> {
    let cvec = CVec::new(ptr as *mut u8, len as uint);
    let mut vec = Vec::with_capacity(len as uint);
    vec.push_all(cvec.as_slice());
    return vec;
}

pub unsafe fn to_string(ptr: *const ::libc::c_char) -> String {
    String::from_utf8(to_bytes(ptr)).unwrap()  // TODO: better error handling
}
