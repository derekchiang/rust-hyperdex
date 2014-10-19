use std::io::net::ip::SocketAddr;
use std::os::{num_cpus, errno};
use std::comm::{Empty, Disconnected};
use std::ptr::null_mut;
use std::collections::{HashMap, TreeSet};
use std::c_str::CString;
use std::vec::raw::from_buf;
use std::path::BytesContainer;
use std::ptr::null;
use std::c_vec::CVec;
use std::mem::transmute;

use sync::deque::{BufferPool, Stealer, Worker};
use sync::{Arc, Mutex};

use libc::*;

use hyperdex::*;
use hyperdex_client::*;
use hyperdex_datastructures::*;

/// Unfortunately floats do not implement Ord nor Eq, so we have to do it for them
/// by wrapping them in a struct and implement those traits
struct F64 {
    value: f64,
}

impl PartialEq for F64 {
    fn eq(&self, other: &F64) -> bool {
        if self.value == other.value {
            true
        } else {
            false
        }
    }
}

impl PartialOrd for F64 {
    fn partial_cmp(&self, other: &F64) -> Option<Ordering> {
        // Kinda hacky, but I think this should work...
        if self.value > other.value {
            Some(Greater)
        } else if self.value < other.value {
            Some(Less)
        } else {
            Some(Equal)
        }
    }
}

impl Eq for F64 {}

impl Ord for F64 {
    fn cmp(&self, other: &F64) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

enum HyperValue {
    HyperString(Vec<u8>),
    HyperInt(i64),
    HyperFloat(f64),

    HyperListString(Vec<Vec<u8>>),
    HyperListInt(Vec<i64>),
    HyperListFloat(Vec<f64>),

    HyperSetString(TreeSet<Vec<u8>>),
    HyperSetInt(TreeSet<i64>),
    HyperSetFloat(TreeSet<F64>),

    HyperMapString(HashMap<Vec<u8>, Vec<u8>>),
    HyperMapInt(HashMap<Vec<u8>, i64>),
    HyperMapFloat(HashMap<Vec<u8>, f64>),
}

type Attributes = HashMap<String, HyperValue>;

struct HyperError {
    status: u32,
    message: String,
    location: String,
}

#[deriving(Clone)]
struct SearchState {
    status: Enum_hyperdex_client_returncode,
    attrs: *const Struct_hyperdex_client_attribute,
    attrs_sz: size_t,
    val_tx: Sender<Attributes>,
    err_tx: Sender<HyperError>,
}

#[deriving(Clone)]
enum HyperState {
    HyperStateOp(Sender<HyperError>),  // for calls that don't return values
    HyperStateSearch(SearchState),  // for calls that do return values
}

struct Request {
    id: int64_t,
    confirm_tx: Sender<bool>,
}

#[deriving(Clone)]
struct InnerClient {
    ptr: *mut Struct_hyperdex_client,
    ops: Arc<Mutex<HashMap<int64_t, HyperState>>>,
    err_tx: Sender<HyperError>,
}

unsafe fn to_bytes(ptr: *const ::libc::c_char) -> Vec<u8> {
    CString::new(ptr, true).container_into_owned_bytes()
}

unsafe fn to_bytes_with_len(ptr: *const ::libc::c_char, len: u64) -> Vec<u8> {
    let cvec = CVec::new(ptr as *mut u8, len as uint);
    let mut vec = Vec::with_capacity(len as uint);
    vec.push_all(cvec.as_slice());
    return vec;
}

unsafe fn to_string(ptr: *const ::libc::c_char) -> String {
    String::from_utf8(to_bytes(ptr)).unwrap()  // TODO: better error handling
}

unsafe fn build_attrs(c_attrs: *const Struct_hyperdex_client_attribute, c_attrs_sz: size_t) -> Result<Attributes, String> {
    let mut attrs = HashMap::new();

    for i in range(0, c_attrs_sz) {
        let attr = *c_attrs.offset(i as int);
        let name = to_string(attr.attr);
        match attr.datatype {
            HYPERDATATYPE_STRING => {
                attrs.insert(name,
                             HyperString(from_buf(attr.value as *const u8, attr.value_sz as uint)));
            },
            HYPERDATATYPE_INT64 => {
                let mut cint = 0i64;
                if hyperdex_ds_unpack_int(attr.value as *const i8, attr.value_sz, &mut cint) < 0 {
                    return Err("Server sent a malformed int".into_string());
                }
                attrs.insert(name, HyperInt(cint));
            },
            HYPERDATATYPE_FLOAT => {
                let mut cdouble = 0f64;
                if hyperdex_ds_unpack_float(attr.value as *const i8,
                                            attr.value_sz, &mut cdouble) < 0 {
                    return Err("Server sent a malformed float".into_string());
                }
                attrs.insert(name, HyperFloat(cdouble));
            },
            HYPERDATATYPE_LIST_STRING => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, HYPERDATATYPE_LIST_STRING,
                                          attr.value, attr.value_sz);
                let mut lst = Vec::new();
                loop {
                    let mut cstr = null();
                    let mut cstr_sz = 0;
                    let status =
                        hyperdex_ds_iterate_list_string_next(&mut citer, &mut cstr, &mut cstr_sz);
                    if status > 0 {
                        lst.push(to_bytes_with_len(cstr, cstr_sz));
                    } else if status < 0 {
                        return Err("Server sent a corrupted list of strings".into_string());
                    } else {
                        break;
                    }
                }
                attrs.insert(name, HyperListString(lst));
            },
            HYPERDATATYPE_LIST_INT64 => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, HYPERDATATYPE_LIST_INT64,
                                          attr.value, attr.value_sz);
                let mut lst = Vec::new();
                loop {
                    let mut num = 0i64;
                    let status = hyperdex_ds_iterate_list_int_next(&mut citer, &mut num);
                    if status > 0 {
                        lst.push(num);
                    } else if status < 0 {
                        return Err("Server sent a corrupted list of integers".into_string());
                    } else {
                        break;
                    }
                }

                attrs.insert(name, HyperListInt(lst));
            },
            HYPERDATATYPE_LIST_FLOAT => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, HYPERDATATYPE_LIST_FLOAT,
                                          attr.value, attr.value_sz);
                let mut lst = Vec::new();
                loop {
                    let mut num = 0f64;
                    let status = hyperdex_ds_iterate_list_float_next(&mut citer, &mut num);
                    if status > 0 {
                        lst.push(num);
                    } else if status < 0 {
                        return Err("Server sent a corrupted list of floats".into_string());
                    } else {
                        break;
                    }
                }

                attrs.insert(name, HyperListFloat(lst));
            },
            HYPERDATATYPE_SET_STRING => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, HYPERDATATYPE_SET_STRING,
                                          attr.value, attr.value_sz);
                let mut set = TreeSet::new();
                loop {
                    let mut cstr = null();
                    let mut cstr_sz = 0;
                    let status =
                        hyperdex_ds_iterate_set_string_next(&mut citer, &mut cstr, &mut cstr_sz);
                    if status > 0 {
                        set.insert(to_bytes_with_len(cstr, cstr_sz));
                    } else if status < 0 {
                        return Err("Server sent a corrupted set of strings".into_string());
                    } else {
                        break;
                    }
                }
                attrs.insert(name, HyperSetString(set));
            },
            HYPERDATATYPE_SET_INT64 => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, HYPERDATATYPE_SET_INT64,
                                          attr.value, attr.value_sz);
                let mut set = TreeSet::new();
                loop {
                    let mut num = 0i64;
                    let status = hyperdex_ds_iterate_set_int_next(&mut citer, &mut num);
                    if status > 0 {
                        set.insert(num);
                    } else if status < 0 {
                        return Err("Server sent a corrupted set of integers".into_string());
                    } else {
                        break;
                    }
                }

                attrs.insert(name, HyperSetInt(set));
            },
            HYPERDATATYPE_SET_FLOAT => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, HYPERDATATYPE_SET_FLOAT,
                                          attr.value, attr.value_sz);
                let mut set = TreeSet::new();
                loop {
                    let mut num = 0f64;
                    let status = hyperdex_ds_iterate_set_float_next(&mut citer, &mut num);
                    if status > 0 {
                        set.insert(F64 { value: num });
                    } else if status < 0 {
                        return Err("Server sent a corrupted set of floats".into_string());
                    } else {
                        break;
                    }
                }

                attrs.insert(name, HyperSetFloat(set));
            },
            HYPERDATATYPE_MAP_STRING_STRING => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, HYPERDATATYPE_MAP_STRING_STRING,
                                          attr.value, attr.value_sz);
                let mut map = HashMap::new();
                loop {
                    let mut ckey = null();
                    let mut ckey_sz = 0;
                    let mut cval = null();
                    let mut cval_sz = 0;
                    let status =
                        hyperdex_ds_iterate_map_string_string_next(&mut citer, &mut ckey, &mut ckey_sz,
                                                                   &mut cval, &mut cval_sz);
                    if status > 0 {
                        map.insert(to_bytes_with_len(ckey, ckey_sz),
                                   to_bytes_with_len(cval, cval_sz));
                    } else if status < 0 {
                        return Err("Server sent a corrupted map of strings to strings".into_string());
                    } else {
                        break;
                    }
                }
                attrs.insert(name, HyperMapString(map));
            },
            HYPERDATATYPE_MAP_STRING_INT64 => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, HYPERDATATYPE_MAP_STRING_INT64,
                                          attr.value, attr.value_sz);
                let mut map = HashMap::new();
                loop {
                    let mut ckey = null();
                    let mut ckey_sz = 0;
                    let mut cval = 0;
                    let status =
                        hyperdex_ds_iterate_map_string_int_next(&mut citer, &mut ckey, &mut ckey_sz,
                                                                &mut cval);
                    if status > 0 {
                        map.insert(to_bytes_with_len(ckey, ckey_sz), cval);
                    } else if status < 0 {
                        return Err("Server sent a corrupted map of strings to integers".into_string());
                    } else {
                        break;
                    }
                }
                attrs.insert(name, HyperMapInt(map));
            },
            HYPERDATATYPE_MAP_STRING_FLOAT => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, HYPERDATATYPE_MAP_STRING_FLOAT,
                                          attr.value, attr.value_sz);
                let mut map = HashMap::new();
                loop {
                    let mut ckey = null();
                    let mut ckey_sz = 0;
                    let mut cval = 0f64;
                    let status =
                        hyperdex_ds_iterate_map_string_float_next(&mut citer, &mut ckey, &mut ckey_sz,
                                                                  &mut cval);
                    if status > 0 {
                        map.insert(to_bytes_with_len(ckey, ckey_sz), cval);
                    } else if status < 0 {
                        return Err("Server sent a corrupted map of strings to floats".into_string());
                    } else {
                        break;
                    }
                }
                attrs.insert(name, HyperMapFloat(map));
            },
            _ => { return Err(format!("Unrecognized datatype: {}", attr.datatype)); }
        }
    }

    return Ok(attrs);
}

impl InnerClient {

    fn new(ptr: *mut Struct_hyperdex_client, err_tx: Sender<HyperError>) -> InnerClient {
        InnerClient {
            ptr: ptr,
            ops: Arc::new(Mutex::new(HashMap::new())),
            err_tx: err_tx,
        }
    }

    fn run_forever(&mut self) {
        unsafe {
            loop {
                hyperdex_client_block(self.ptr, 250);  // prevent busy spinning
                let mut loop_status = 0u32;
                let reqid = hyperdex_client_loop(self.ptr, 0, &mut loop_status);
                if reqid < 0 && loop_status == HYPERDEX_CLIENT_TIMEOUT {
                    // pass
                } else if reqid < 0 && loop_status == HYPERDEX_CLIENT_NONEPENDING {
                    // pass
                } else if reqid < 0 {
                    let e = HyperError {
                        status: loop_status,
                        message: to_string(hyperdex_client_error_message(self.ptr)),
                        location: to_string(hyperdex_client_error_location(self.ptr)),
                    };
                    self.err_tx.send(e);
                } else {
                    let mut ops = &mut*self.ops.lock();
                    match ops.find_copy(&reqid) {
                        None => {},  // TODO: this seems to be an error case... might want to do something
                        Some(HyperStateOp(op_tx)) => {
                            op_tx.send(HyperError {
                                status: loop_status,
                                message: to_string(hyperdex_client_error_message(self.ptr)),
                                location: to_string(hyperdex_client_error_location(self.ptr)),
                            });
                            ops.remove(&reqid);
                        },
                        Some(HyperStateSearch(state)) => {
                        },
                    }
                }
            }
        }
    }
}

pub struct Client {
    inner_clients: Vec<InnerClient>
}

impl Client {

    pub fn new(coordinator: SocketAddr) -> Result<Client, String> {
        let ip = format!("{}", coordinator.ip).to_c_str().as_ptr();
        let port = coordinator.port;

        let (err_tx, err_rx) = channel();

        let mut inner_clients = Vec::new();
        for _ in range(0, num_cpus()) {
            let ptr = unsafe { hyperdex_client_create(ip, port) };
            if ptr.is_null() {
                return Err(format!("Unable to create client.  errno is: {}", errno()));
            } else {
                let ops = Arc::new(Mutex::new(HashMap::new()));
                let mut inner_client = InnerClient {
                    ptr: ptr,
                    ops: ops.clone(),
                    err_tx: err_tx.clone(),
                };
                let mut ic_clone = inner_client.clone();
                spawn(proc() {
                    ic_clone.run_forever();
                });
                inner_clients.push(inner_client);
            }
        };

        Ok(Client {
            inner_clients: inner_clients
        })
    }

    // pub fn new_from_conn_str(conn: String) -> Result<Client, String> {
        // let conn_str = conn.to_c_str().as_ptr();
        // let ptr = unsafe { hyperdex_client_create_conn_str(conn_str) };
        // if ptr.is_null() {
            // Err(format!("Unable to create client.  errno is: {}", errno()))
        // } else {
            // unsafe {
                // Ok(Client {
                    // ptr: ptr
                // })
            // }
        // }
    // }

}

// impl Drop for Client {
    // fn drop(&mut self) {
        // unsafe {
            // hyperdex_client_destroy(self.ptr);
        // }
    // }
// }

