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
use std::time::duration::Duration;
use std::io::timer::sleep;

use sync::deque::{BufferPool, Stealer, Worker};
use sync::{Arc, Mutex};

use libc::*;

use super::*;
use common::*;
use hyperdex::*;
use hyperdex_client::*;
use hyperdex_datastructures::*;

/// Unfortunately floats do not implement Ord nor Eq, so we have to do it for them
/// by wrapping them in a struct and implement those traits
#[deriving(Show, Clone)]
pub struct F64(f64);

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

#[deriving(Clone)]
struct SearchState {
    status: Box<Enum_hyperdex_client_returncode>,
    attrs: Box<*const Struct_hyperdex_client_attribute>,
    attrs_sz: Box<size_t>,
    res_tx: Sender<Result<HyperObject, HyperError>>,
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
            if hyperdex_ds_copy_string(arena, s.as_ptr() as *const i8, s.len() as u64,
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
            panic!("TODO");
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

unsafe fn convert_attributenames(arena: *mut Struct_hyperdex_ds_arena, attrs: Vec<String>)
    -> Result<Vec<*const i8>, String> {
    let mut res = Vec::with_capacity(attrs.len());
    for attr in attrs.into_iter() {
        res.push(try!(convert_cstring(arena, attr)));
    }
    Ok(res)
}

unsafe fn convert_predicates(arena: *mut Struct_hyperdex_ds_arena, predicates: Vec<HyperPredicate>)
    -> Result<Vec<Struct_hyperdex_client_attribute_check>, String> {
    let mut res = Vec::with_capacity(predicates.len());
    for p in predicates.into_iter() {
        let attr = try!(convert_cstring(arena, p.attr));
        let (val, val_sz, dt) = try!(convert_type(arena, p.value));
        res.push(Struct_hyperdex_client_attribute_check {
            attr: attr,
            value: val,
            value_sz: val_sz,
            datatype: dt,
            predicate: p.predicate as u32,
        });
    }
    Ok(res)
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
        let lolkey = CString::new(ckey, false);
        let lol = Vec::from_raw_parts(cval as *mut i8, cval_sz as uint, cval_sz as uint);
    }

    Ok(attrs)
}

#[deriving(Show, Clone)]
pub enum HyperValue {
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

pub enum HyperPredicateType {
	FAIL = HYPERPREDICATE_FAIL as int,
	EQUALS = HYPERPREDICATE_EQUALS as int,
	LESS_THAN = HYPERPREDICATE_LESS_THAN as int,
	LESS_EQUAL = HYPERPREDICATE_LESS_EQUAL as int,
	GREATER_EQUAL = HYPERPREDICATE_GREATER_EQUAL as int,
	GREATER_THAN = HYPERPREDICATE_GREATER_THAN as int,
	REGEX = HYPERPREDICATE_REGEX as int,
	LENGTH_EQUALS = HYPERPREDICATE_LENGTH_EQUALS as int,
	LENGTH_LESS_EQUAL = HYPERPREDICATE_LENGTH_LESS_EQUAL as int,
	LENGTH_GREATER_EQUAL = HYPERPREDICATE_LENGTH_GREATER_EQUAL as int,
	CONTAINS = HYPERPREDICATE_CONTAINS as int,
}

pub struct HyperPredicate {
    pub attr: String,
    pub value: HyperValue,
    pub predicate: HyperPredicateType,
}

pub type HyperObject = HashMap<String, HyperValue>;

pub type HyperMap = HashMap<HyperValue, HyperValue>;

impl InnerClient {

    fn run_forever(&mut self, shutdown_rx: Receiver<()>) {
        unsafe {
            loop {
                match shutdown_rx.try_recv() {
                    Err(Empty) => (),
                    // Otherwise, the client has been dropped
                    _ => {
                        hyperdex_client_destroy(self.ptr);
                        return;
                    }
                }

                hyperdex_client_block(self.ptr, 250);  // prevent busy spinning
                let mut loop_status = 0u32;
                let reqid = hyperdex_client_loop(self.ptr, 0, &mut loop_status);
                if reqid < 0 && loop_status == HYPERDEX_CLIENT_TIMEOUT {
                    // pass
                } else if reqid < 0 && loop_status == HYPERDEX_CLIENT_NONEPENDING {
                    // pass
                } else if reqid < 0 {
                    self.err_tx.send(get_client_error(self.ptr, loop_status));
                } else {
                    let mut ops = &mut*self.ops.lock();
                    match ops.find_copy(&reqid) {
                        None => {},  // Is this an error case?  It happens occationally

                        Some(HyperStateOp(op_tx)) => {
                            op_tx.send(get_client_error(self.ptr, loop_status));
                            ops.remove(&reqid);
                        },

                        Some(HyperStateSearch(state)) => {
                            if *state.status == HYPERDEX_CLIENT_SUCCESS {
                                match build_hyperobject(*state.attrs, *state.attrs_sz) {
                                    Ok(attrs) => {
                                        state.res_tx.send(Ok(attrs));
                                    },
                                    Err(err) => {
                                        let herr = HyperError {
                                            status: HYPERDEX_CLIENT_SERVERERROR,
                                            message: err,
                                            location: String::new(),
                                        };
                                        state.res_tx.send(Err(herr));
                                    }
                                }
                                hyperdex_client_destroy_attrs(*state.attrs, *state.attrs_sz);
                            } else if *state.status == HYPERDEX_CLIENT_SEARCHDONE {
                                match ops.remove(&reqid) {
                                    Some(HyperStateSearch(state)) => state,
                                    x => panic!(x),
                                };
                                let res_tx = state.res_tx;
                                // this seems to be a bug in Rust... state.res_tx sometimes
                                // doesn't get dropped properly
                            } else {
                                state.res_tx.send(Err(get_client_error(self.ptr, *state.status)));
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
    shutdown_txs: Vec<Sender<()>>,
    inner_clients: Vec<InnerClient>,
}

macro_rules! make_fn_spacename_key_status_attributes(
    ($fn_name: ident) => (
        impl Client {
        pub fn $fn_name(&mut self, space: String, key: Vec<u8>) -> Future<Result<HyperObject, HyperError>> {
            unsafe {
            // TODO: Is "Relaxed" good enough?
            let inner_client =
                self.inner_clients[self.counter.fetch_add(1, atomic::Relaxed) as uint].clone();

            let key_cstr = key.as_ptr() as *const i8;
            let key_sz = key.len() as u64;

            let status_ptr = transmute(box 0u32);

            let attrs_ptr = transmute(box null::<*mut Struct_hyperdex_client_attribute>());
            let attrs_sz_ptr = transmute(box 0u32);

            let (err_tx, err_rx) = channel();

            let mut ops_mutex = inner_client.ops.clone();
            {
                let mut ops = &mut*ops_mutex.lock();
                let req_id =
                    concat_idents!(hyperdex_client_, $fn_name)(inner_client.ptr,
                                                               space.as_ptr() as *const i8,
                                                               key_cstr, key_sz,
                                                               status_ptr, attrs_ptr, attrs_sz_ptr);
                if req_id < 0 {
                    return Future::from_value(Err(get_client_error(inner_client.ptr, 0)));
                }
                ops.insert(req_id, HyperStateOp(err_tx));
            }

            Future::from_fn(proc() {
                let status: Box<u32> = transmute(status_ptr);
                let attrs: Box<*mut Struct_hyperdex_client_attribute> = transmute(attrs_ptr);
                let attrs_sz: Box<u32> = transmute(attrs_sz_ptr);
                let err = err_rx.recv();
                if err.status != HYPERDEX_CLIENT_SUCCESS {
                    Err(err)
                } else if *status != HYPERDEX_CLIENT_SUCCESS {
                    Err(get_client_error(inner_client.ptr, *status))
                } else {
                    let res = match build_hyperobject(*attrs_ptr, *attrs_sz_ptr) {
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
                    };
                    hyperdex_client_destroy_attrs(*attrs_ptr, *attrs_sz_ptr);
                    res
                }
            })
            }
        }
        }
    );
)

macro_rules! make_fn_spacename_key_attributenames_status_attributes(
    ($fn_name: ident) => (
        impl Client {
        pub fn $fn_name(&mut self, space: String, key: Vec<u8>, attrs: Vec<String>) -> Future<Result<HyperObject, HyperError>> {
            unsafe {
            // TODO: Is "Relaxed" good enough?
            let inner_client =
                self.inner_clients[self.counter.fetch_add(1, atomic::Relaxed) as uint].clone();

            let key_cstr = key.as_ptr() as *const i8;
            let key_sz = key.len() as u64;

            let status_ptr = transmute(box 0u32);

            let attrs_ptr = transmute(box null::<*mut Struct_hyperdex_client_attribute>());
            let attrs_sz_ptr = transmute(box 0u32);

            let arena = hyperdex_ds_arena_create();
            let mut c_attrs = match convert_attributenames(arena, attrs) {
                Ok(x) => x,
                Err(err) => return Future::from_value(Err(HyperError {
                    status: 0,
                    message: err,
                    location: String::new(),
                })),
            };

            let (err_tx, err_rx) = channel();

            let mut ops_mutex = inner_client.ops.clone();
            {
                let mut ops = &mut*ops_mutex.lock();
                let req_id =
                    concat_idents!(hyperdex_client_, $fn_name)(inner_client.ptr,
                                                               space.as_ptr() as *const i8,
                                                               key_cstr, key_sz,
                                                               c_attrs.as_mut_ptr(),
                                                               c_attrs.len() as u64,
                                                               status_ptr, attrs_ptr, attrs_sz_ptr);
                if req_id < 0 {
                    return Future::from_value(Err(get_client_error(inner_client.ptr, 0)));
                }
                ops.insert(req_id, HyperStateOp(err_tx));
            }
            hyperdex_ds_arena_destroy(arena);

            Future::from_fn(proc() {
                let status: Box<u32> = transmute(status_ptr);
                let attrs: Box<*mut Struct_hyperdex_client_attribute> = transmute(attrs_ptr);
                let attrs_sz: Box<u32> = transmute(attrs_sz_ptr);
                let err = err_rx.recv();
                if err.status != HYPERDEX_CLIENT_SUCCESS {
                    Err(err)
                } else if *status != HYPERDEX_CLIENT_SUCCESS {
                    Err(get_client_error(inner_client.ptr, *status))
                } else {
                    let res = match build_hyperobject(*attrs_ptr, *attrs_sz_ptr) {
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
                    };
                    hyperdex_client_destroy_attrs(*attrs_ptr, *attrs_sz_ptr);
                    res
                }
            })
            }
        }
        }
    );
)

macro_rules! make_fn_spacename_key_attributes_status(
    ($fn_name: ident) => (
        impl Client {
        pub fn $fn_name(&mut self, space: String, key: Vec<u8>, value: HyperObject)
            -> Future<Result<(), HyperError>> { unsafe {
            // TODO: Is "Relaxed" good enough?
            let inner_client =
                self.inner_clients[self.counter.fetch_add(1, atomic::Relaxed) as uint].clone();

            let key_cstr = key.as_ptr() as *const i8;
            let key_sz = key.len() as u64;

            let status_ptr = transmute(box 0u32);

            let arena = hyperdex_ds_arena_create();
            let obj = match convert_hyperobject(arena, value) {
                Ok(x) => x,
                Err(err) => panic!(err),
            };

            let (err_tx, err_rx) = channel();

            let mut ops_mutex = inner_client.ops.clone();
            {
                let mut ops = &mut*ops_mutex.lock();
                let req_id =
                    concat_idents!(hyperdex_client_, $fn_name)(inner_client.ptr,
                                                               space.as_ptr() as *const i8,
                                                               key_cstr, key_sz,
                                                               obj.as_ptr(), obj.len() as u64,
                                                               status_ptr);
                if req_id < 0 {
                    return Future::from_value(Err(get_client_error(inner_client.ptr, 0)));
                }
                ops.insert(req_id, HyperStateOp(err_tx));
            }

            hyperdex_ds_arena_destroy(arena);
            Future::from_fn(proc() {
                let err = err_rx.recv();
                let status: Box<u32> = transmute(status_ptr);
                if err.status != HYPERDEX_CLIENT_SUCCESS {
                    Err(err)
                } else if *status != HYPERDEX_CLIENT_SUCCESS {
                    Err(get_client_error(inner_client.ptr, *status))
                } else {
                    Ok(())
                }
            })
        }}
        }
    );
)

impl Client {

    pub fn new(coordinator: SocketAddr) -> Result<Client, String> {
        let ip_str = format!("{}", coordinator.ip).to_c_str();

        let (err_tx, err_rx) = channel();

        let mut inner_clients = Vec::new();
        let mut shutdown_txs = Vec::new();
        for _ in range(0, num_cpus()) {
            let ptr = unsafe { hyperdex_client_create(ip_str.as_ptr(), coordinator.port) };
            if ptr.is_null() {
                return Err(format!("Unable to create client.  errno is: {}", errno()));
            } else {
                let ops = Arc::new(Mutex::new(HashMap::new()));
                let (shutdown_tx, shutdown_rx) = channel();
                let mut inner_client = InnerClient {
                    ptr: ptr,
                    ops: ops.clone(),
                    err_tx: err_tx.clone(),
                };
                let mut ic_clone = inner_client.clone();
                spawn(proc() {
                    ic_clone.run_forever(shutdown_rx);
                });
                inner_clients.push(inner_client);
                shutdown_txs.push(shutdown_tx);
            }
        };

        Ok(Client {
            counter: AtomicInt::new(0),
            inner_clients: inner_clients,
            shutdown_txs: shutdown_txs,
        })
    }

    pub fn search(&mut self, space: String, predicates: Vec<HyperPredicate>)
        -> Receiver<Result<HyperObject, HyperError>> { unsafe {
            // TODO: Is "Relaxed" good enough?
            let inner_client =
                self.inner_clients[self.counter.fetch_add(1, atomic::Relaxed) as uint].clone();

            let (res_tx, res_rx) = channel();

            let arena = hyperdex_ds_arena_create();
            let checks = match convert_predicates(arena, predicates) {
                Ok(x) => x,
                Err(err) => {
                    res_tx.send(Err(HyperError {
                        status: 0,
                        message: err,
                        location: String::new(),
                    }));
                    return res_rx;
                },
            };

            let status_ptr = transmute(box 0u32);
            let attrs_ptr = transmute(box null::<*mut Struct_hyperdex_client_attribute>());
            let attrs_sz_ptr = transmute(box 0u32);

            let mut ops_mutex = inner_client.ops.clone();
            {
                let mut ops = &mut*ops_mutex.lock();
                let req_id =
                    hyperdex_client_search(inner_client.ptr,
                                           space.as_ptr() as *const i8,
                                           checks.as_ptr(),
                                           checks.len() as u64,
                                           status_ptr, attrs_ptr, attrs_sz_ptr);
                if req_id < 0 {
                    res_tx.send(Err(get_client_error(inner_client.ptr, 0)));
                    return res_rx;
                }

                let mut state = SearchState {
                    status: transmute(status_ptr),
                    attrs: transmute(attrs_ptr),
                    attrs_sz: transmute(attrs_sz_ptr),
                    res_tx: res_tx,
                };

                ops.insert(req_id, HyperStateSearch(state));
            }
            hyperdex_ds_arena_destroy(arena);
            return res_rx;
        }
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

make_fn_spacename_key_status_attributes!(get)

make_fn_spacename_key_attributenames_status_attributes!(get_partial)

make_fn_spacename_key_attributes_status!(put)
make_fn_spacename_key_attributes_status!(put_if_not_exist)
make_fn_spacename_key_attributes_status!(atomic_add)
make_fn_spacename_key_attributes_status!(atomic_sub)
make_fn_spacename_key_attributes_status!(atomic_mul)
make_fn_spacename_key_attributes_status!(atomic_div)
make_fn_spacename_key_attributes_status!(atomic_mod)
make_fn_spacename_key_attributes_status!(atomic_and)
make_fn_spacename_key_attributes_status!(atomic_or)
make_fn_spacename_key_attributes_status!(atomic_xor)
make_fn_spacename_key_attributes_status!(string_prepend)
make_fn_spacename_key_attributes_status!(string_append)
make_fn_spacename_key_attributes_status!(list_lpush)
make_fn_spacename_key_attributes_status!(list_rpush)
make_fn_spacename_key_attributes_status!(set_add)
make_fn_spacename_key_attributes_status!(set_remove)
make_fn_spacename_key_attributes_status!(set_intersect)
make_fn_spacename_key_attributes_status!(set_union)
make_fn_spacename_key_attributes_status!(map_remove)
