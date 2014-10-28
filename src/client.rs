use std::io::net::ip::SocketAddr;
use std::os::{num_cpus, errno};
use std::comm::{Empty, Disconnected};
use std::collections::{HashMap, TreeSet};
use std::c_str::CString;
use std::vec::raw::from_buf;
use std::path::BytesContainer;
use std::ptr::{null, null_mut};
use std::c_vec::CVec;
use std::mem::transmute;
use std::hash::Hash;
use std::hash::sip::SipState;
use std::sync::atomic;
use std::sync::atomic::AtomicInt;
use std::sync::Future;

use sync::deque::{BufferPool, Stealer, Worker};
use sync::{Arc, Mutex};

use libc::*;

use hyperdex::*;
use hyperdex_client::*;
use hyperdex_datastructures::*;

/// Unfortunately floats do not implement Ord nor Eq, so we have to do it for them
/// by wrapping them in a struct and implement those traits
struct F64(f64);

impl PartialEq for F64 {
    fn eq(&self, other: &F64) -> bool {
        if self == other {
            true
        } else {
            false
        }
    }
}

impl PartialOrd for F64 {
    fn partial_cmp(&self, other: &F64) -> Option<Ordering> {
        // Kinda hacky, but I think this should work...
        if self > other {
            Some(Greater)
        } else if self < other {
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

impl Hash for F64 {
    fn hash(&self, state: &mut SipState) {
        unsafe {
            let x: u64 = transmute(self);
            x.hash(state);
        }
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

    HyperMapStringString(HashMap<Vec<u8>, Vec<u8>>),
    HyperMapStringInt(HashMap<Vec<u8>, i64>),
    HyperMapStringFloat(HashMap<Vec<u8>, f64>),

    HyperMapIntString(HashMap<i64, Vec<u8>>),
    HyperMapIntInt(HashMap<i64, i64>),
    HyperMapIntFloat(HashMap<i64, f64>),

    HyperMapFloatString(HashMap<F64, Vec<u8>>),
    HyperMapFloatInt(HashMap<F64, i64>),
    HyperMapFloatFloat(HashMap<F64, f64>),
}

pub type HyperObject = HashMap<String, HyperValue>;

pub struct HyperError {
    status: u32,
    message: String,
    location: String,
}

#[deriving(Clone)]
struct SearchState {
    status: Enum_hyperdex_client_returncode,
    attrs: *const Struct_hyperdex_client_attribute,
    attrs_sz: size_t,
    val_tx: Sender<HyperObject>,
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

unsafe fn build_hyperobject(c_attrs: *const Struct_hyperdex_client_attribute, c_attrs_sz: size_t) -> Result<HyperObject, String> {
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
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
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
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
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
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
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
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
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
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
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
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
                let mut set = TreeSet::new();
                loop {
                    let mut num = 0f64;
                    let status = hyperdex_ds_iterate_set_float_next(&mut citer, &mut num);
                    if status > 0 {
                        set.insert(F64(num));
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
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
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
                attrs.insert(name, HyperMapStringString(map));
            },
            HYPERDATATYPE_MAP_STRING_INT64 => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
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
                attrs.insert(name, HyperMapStringInt(map));
            },
            HYPERDATATYPE_MAP_STRING_FLOAT => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
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
                attrs.insert(name, HyperMapStringFloat(map));
            },

            HYPERDATATYPE_MAP_INT64_STRING => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
                let mut map = HashMap::new();
                loop {
                    let mut ckey = 0;
                    let mut cval = null();
                    let mut cval_sz = 0;
                    let status = hyperdex_ds_iterate_map_int_string_next(&mut citer, &mut ckey,
                                                                         &mut cval, &mut cval_sz);
                    if status > 0 {
                        map.insert(ckey, to_bytes_with_len(cval, cval_sz));
                    } else if status < 0 {
                        return Err("Server sent a corrupted map of integers to strings".into_string());
                    } else {
                        break;
                    }
                }
                attrs.insert(name, HyperMapIntString(map));
            },
            HYPERDATATYPE_MAP_INT64_INT64 => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
                let mut map = HashMap::new();
                loop {
                    let mut ckey = 0;
                    let mut cval = 0;
                    let status = hyperdex_ds_iterate_map_int_int_next(&mut citer, &mut ckey, &mut cval);
                    if status > 0 {
                        map.insert(ckey, cval);
                    } else if status < 0 {
                        return Err("Server sent a corrupted map of integers to integers".into_string());
                    } else {
                        break;
                    }
                }
                attrs.insert(name, HyperMapIntInt(map));
            },
            HYPERDATATYPE_MAP_INT64_FLOAT => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
                let mut map = HashMap::new();
                loop {
                    let mut ckey = 0;
                    let mut cval = 0f64;
                    let status =
                        hyperdex_ds_iterate_map_int_float_next(&mut citer, &mut ckey, &mut cval);
                    if status > 0 {
                        map.insert(ckey, cval);
                    } else if status < 0 {
                        return Err("Server sent a corrupted map of integers to floats".into_string());
                    } else {
                        break;
                    }
                }
                attrs.insert(name, HyperMapIntFloat(map));
            },

            HYPERDATATYPE_MAP_FLOAT_STRING => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
                let mut map = HashMap::new();
                loop {
                    let mut ckey = 0f64;
                    let mut cval = null();
                    let mut cval_sz = 0;
                    let status = hyperdex_ds_iterate_map_float_string_next(&mut citer, &mut ckey,
                                                                         &mut cval, &mut cval_sz);
                    if status > 0 {
                        map.insert(F64(ckey), to_bytes_with_len(cval, cval_sz));
                    } else if status < 0 {
                        return Err("Server sent a corrupted map of floats to strings".into_string());
                    } else {
                        break;
                    }
                }
                attrs.insert(name, HyperMapFloatString(map));
            },
            HYPERDATATYPE_MAP_FLOAT_INT64 => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
                let mut map = HashMap::new();
                loop {
                    let mut ckey = 0f64;
                    let mut cval = 0;
                    let status = hyperdex_ds_iterate_map_float_int_next(&mut citer, &mut ckey,
                                                                        &mut cval);
                    if status > 0 {
                        map.insert(F64(ckey), cval);
                    } else if status < 0 {
                        return Err("Server sent a corrupted map of floats to integers".into_string());
                    } else {
                        break;
                    }
                }
                attrs.insert(name, HyperMapFloatInt(map));
            },
            HYPERDATATYPE_MAP_FLOAT_FLOAT => {
                let mut citer = Struct_hyperdex_ds_iterator::new();
                hyperdex_ds_iterator_init(&mut citer, attr.datatype, attr.value, attr.value_sz);
                let mut map = HashMap::new();
                loop {
                    let mut ckey = 0f64;
                    let mut cval = 0f64;
                    let status =
                        hyperdex_ds_iterate_map_float_float_next(&mut citer, &mut ckey, &mut cval);
                    if status > 0 {
                        map.insert(F64(ckey), cval);
                    } else if status < 0 {
                        return Err("Server sent a corrupted map of floats to floats".into_string());
                    } else {
                        break;
                    }
                }
                attrs.insert(name, HyperMapFloatFloat(map));
            },

            _ => { return Err(format!("Unrecognized datatype: {}", attr.datatype)); }
        }
    }

    return Ok(attrs);
}

unsafe fn convert_cstring(arena: *mut Struct_hyperdex_ds_arena, s: String) -> Result<*const i8, String> {
    let cstr = s.to_c_str();
    let mut err = 0;
    let mut cs = null();
    let mut sz = 0;
    if hyperdex_ds_copy_string(arena, cstr.as_ptr(), (cstr.len() + 1) as u64, &mut err, &mut cs, &mut sz) < 0 {
        Err("failed to allocate memory".into_string())
    } else {
        Ok(cs)
    }
}

unsafe fn convert_type(arena: *mut Struct_hyperdex_ds_arena, val: HyperValue) -> Result<(*const i8, size_t, Enum_hyperdatatype), String> {
    let mut status = 0;
    let mut cs = null();
    let mut sz = 0;
    let mem_err = Err("failed to allocate memory".into_string());

    match val {
        HyperString(s) => {
            let cstr = CString::new(s.as_ptr() as *const i8, false);
            if hyperdex_ds_copy_string(arena, cstr.as_ptr(), cstr.len() as u64,
                                       &mut status, &mut cs, &mut sz) < 0 {
                mem_err
            } else {
                Ok((cs, sz, HYPERDATATYPE_STRING))
            }
        },
        HyperInt(i) => {
            if hyperdex_ds_copy_int(arena, i, &mut status, &mut cs, &mut sz) < 0 {
                mem_err
            } else {
                Ok((cs, sz, HYPERDATATYPE_INT64))
            }
        },
        HyperFloat(f) => {
            if hyperdex_ds_copy_float(arena, f, &mut status, &mut cs, &mut sz) < 0 {
                mem_err
            } else {
                Ok((cs, sz, HYPERDATATYPE_FLOAT))
            }
        },
        HyperListString(ls) => {
            let ds_lst = hyperdex_ds_allocate_list(arena);
            if ds_lst.is_null() {
                mem_err
            } else {
                for s in ls.iter() {
                    let cstr = CString::new(s.as_ptr() as *const i8, false);
                    if hyperdex_ds_list_append_string(ds_lst, cstr.as_ptr(),
                                                      cstr.len() as u64, &mut status) < 0 {
                        return mem_err;
                    }
                }
                let mut dt = 0;

                if hyperdex_ds_list_finalize(ds_lst, &mut status, &mut cs, &mut sz, &mut dt) < 0 {
                    mem_err
                } else {
                    Ok((cs, sz, dt))
                }
            }
        },
        _ => {
            fail!("TODO");
        }
        // HyperSetString(ss) => {
            // let ds_set = hyperdex_ds_allocate_set(arena);
            // for s in ss.iter() {
                // let cstr = s.to_c_str();
                // if hyperdex_ds_set_insert_string(ds_set, cstr.as_ptr(), cstr.len(), &status)
            // }
        // }
    }
}

unsafe fn convert_hyperobject(arena: *mut Struct_hyperdex_ds_arena, obj: HyperObject) -> Result<Vec<Struct_hyperdex_client_attribute>, String> {
    let mut attrs = Vec::new();

    for (key, val) in obj.into_iter() {
        let mut ckey = try!(convert_cstring(arena, key));
        let (cval, cval_sz, dt) = try!(convert_type(arena, val));
        attrs.push(Struct_hyperdex_client_attribute {
            attr: ckey,
            value: cval,
            value_sz: cval_sz,
            datatype: dt,
        });
    }

    Ok(attrs)
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
                            if state.status == HYPERDEX_CLIENT_SUCCESS {
                                match build_hyperobject(state.attrs, state.attrs_sz) {
                                    Ok(attrs) => {
                                        state.val_tx.send(attrs);
                                    },
                                    Err(err) => {
                                        let herr = HyperError {
                                            status: HYPERDEX_CLIENT_SERVERERROR,
                                            message: err,
                                            location: String::new(),
                                        };
                                        state.err_tx.send(herr);
                                    }
                                }
                                hyperdex_client_destroy_attrs(state.attrs, state.attrs_sz);
                            } else if state.status == HYPERDEX_CLIENT_SEARCHDONE {
                                ops.remove(&reqid);
                            } else {
                                let err = HyperError {
                                    status: state.status,
                                    message: to_string(hyperdex_client_error_message(self.ptr)),
                                    location: to_string(hyperdex_client_error_location(self.ptr)),
                                };
                                state.err_tx.send(err);
                            }
                        },
                    }
                }
            }
        }
    }
}

pub struct Client {
    counter: AtomicInt,
    inner_clients: Vec<InnerClient>,
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
            counter: AtomicInt::new(0),
            inner_clients: inner_clients,
        })
    }

    pub fn get(&mut self, space: String, key: Vec<u8>) -> Result<HyperObject, HyperError> {
        self.get_async(space, key).unwrap()
    }

    pub fn get_async(&mut self, space: String, key: Vec<u8>) -> Future<Result<HyperObject, HyperError>> {
        let space_cstr = space.to_c_str().as_ptr();
        let key_cstr = key.as_ptr() as *const i8;
        let key_sz = key.len() as u64;
        let mut status = 0;
        let mut attrs = null();
        let mut attrs_sz = 0;

        // TODO: Is "Relaxed" good enough?
        let inner_client =
            self.inner_clients[self.counter.fetch_add(1, atomic::Relaxed) as uint].clone();
        let (err_tx, err_rx) = channel();

        let mut ops_mutex = inner_client.ops.clone();
        {
            let mut ops = &mut*ops_mutex.lock();
            let req_id = unsafe {
                hyperdex_client_get(inner_client.ptr, space_cstr, key_cstr, key_sz,
                                    &mut status, &mut attrs, &mut attrs_sz)
            };
            ops.insert(req_id, HyperStateOp(err_tx));
        }

        Future::from_fn(proc() {
            let err = err_rx.recv();
            if err.status != HYPERDEX_CLIENT_SUCCESS {
                Err(err)
            } else {
                unsafe {
                    match build_hyperobject(attrs, attrs_sz) {
                        Ok(obj) => {
                            Ok(obj)
                        },
                        Err(msg) => {
                            Err(HyperError {
                                status: HYPERDEX_CLIENT_SERVERERROR,
                                message: msg,
                                location: String::new(),
                            })
                        }
                    }
                }
            }
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

