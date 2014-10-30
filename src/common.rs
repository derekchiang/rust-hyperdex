use std::c_str::CString;
use std::c_vec::CVec;
use std::path::BytesContainer;

pub struct HyperError {
    pub status: u32,
    pub message: String,
    pub location: String,
}

pub unsafe fn to_bytes(ptr: *const ::libc::c_char) -> Vec<u8> {
    CString::new(ptr, true).container_into_owned_bytes()
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
