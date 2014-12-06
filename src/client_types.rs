use std::collections::{HashMap, TreeSet};
use std::hash::sip::SipState;
use std::mem::transmute;
use std::hash::Hash;

use sync::{Arc, Mutex};

use libc::*;

use common::*;

use hyperdex_client::*;
use hyperdex_datastructures::*;
use hyperdex::*;

use self::HyperValue::*;
use self::HyperState::*;

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

#[deriving(Clone)]
pub struct SearchState {
    pub status: Box<Enum_hyperdex_client_returncode>,
    pub attrs: Box<*const Struct_hyperdex_client_attribute>,
    pub attrs_sz: Box<size_t>,
    pub res_tx: Sender<Result<HyperObject, HyperError>>,
}

#[deriving(Clone)]
pub enum HyperState {
    HyperStateOp(Sender<HyperError>),  // for calls that don't return values
    HyperStateSearch(SearchState),  // for calls that do return values
}

pub struct Request {
    id: int64_t,
    confirm_tx: Sender<bool>,
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

pub struct HyperMapAttribute {
    pub attr: String,
    pub key: HyperValue,
    pub value: HyperValue,
}

pub type HyperObject = HashMap<String, HyperValue>;

pub type HyperMap = HashMap<HyperValue, HyperValue>;

pub trait ToHyperValue {
    fn to_hyper(self) -> HyperValue;
}

impl<'a> ToHyperValue for &'a str {
    fn to_hyper(self) -> HyperValue {
        HyperValue::HyperString(self.as_bytes().to_vec())
    }
}

impl ToHyperValue for String {
    fn to_hyper(self) -> HyperValue {
        HyperValue::HyperString(self.into_bytes())
    }
}

impl ToHyperValue for Vec<u8> {
    fn to_hyper(self) -> HyperValue {
        HyperString(self)
    }
}

impl ToHyperValue for i64 {
    fn to_hyper(self) -> HyperValue {
        HyperInt(self)
    }
}

impl ToHyperValue for f64 {
    fn to_hyper(self) -> HyperValue {
        HyperFloat(self)
    }
}

impl<'a> ToHyperValue for Vec<&'a str> {
    fn to_hyper(self) -> HyperValue {
        HyperListString(self.into_iter().map(|s| {
            s.as_bytes().to_vec()
        }).collect())
    }
}


impl ToHyperValue for Vec<String> {
    fn to_hyper(self) -> HyperValue {
        HyperListString(self.into_iter().map(|s| {
            s.into_bytes()
        }).collect())
    }
}

impl ToHyperValue for Vec<Vec<u8>> {
    fn to_hyper(self) -> HyperValue {
        HyperListString(self)
    }
}

impl ToHyperValue for Vec<i64> {
    fn to_hyper(self) -> HyperValue {
        HyperListInt(self)
    }
}

impl ToHyperValue for Vec<f64> {
    fn to_hyper(self) -> HyperValue {
        HyperListFloat(self)
    }
}

impl<'a> ToHyperValue for TreeSet<&'a str> {
    fn to_hyper(self) -> HyperValue {
        HyperSetString(FromIterator::from_iter(self.into_iter().map(|s| {
            s.as_bytes().to_vec()
        })))
    }
}


impl ToHyperValue for TreeSet<String> {
    fn to_hyper(self) -> HyperValue {
        HyperSetString(FromIterator::from_iter(self.into_iter().map(|s| {
            s.into_bytes()
        })))
    }
}

impl ToHyperValue for TreeSet<Vec<u8>> {
    fn to_hyper(self) -> HyperValue {
        HyperSetString(self)
    }
}

impl ToHyperValue for TreeSet<i64> {
    fn to_hyper(self) -> HyperValue {
        HyperSetInt(self)
    }
}

impl ToHyperValue for TreeSet<F64> {
    fn to_hyper(self) -> HyperValue {
        HyperSetFloat(self)
    }
}

impl ToHyperValue for HashMap<Vec<u8>, Vec<u8>> {
    fn to_hyper(self) -> HyperValue {
        HyperMapStringString(self)
    }
}

impl ToHyperValue for HashMap<Vec<u8>, i64> {
    fn to_hyper(self) -> HyperValue {
        HyperMapStringInt(self)
    }
}

impl ToHyperValue for HashMap<Vec<u8>, f64> {
    fn to_hyper(self) -> HyperValue {
        HyperMapStringFloat(self)
    }
}

impl ToHyperValue for HashMap<i64, Vec<u8>> {
    fn to_hyper(self) -> HyperValue {
        HyperMapIntString(self)
    }
}

impl ToHyperValue for HashMap<i64, i64> {
    fn to_hyper(self) -> HyperValue {
        HyperMapIntInt(self)
    }
}

impl ToHyperValue for HashMap<i64, f64> {
    fn to_hyper(self) -> HyperValue {
        HyperMapIntFloat(self)
    }
}

impl ToHyperValue for HashMap<F64, Vec<u8>> {
    fn to_hyper(self) -> HyperValue {
        HyperMapFloatString(self)
    }
}

impl ToHyperValue for HashMap<F64, i64> {
    fn to_hyper(self) -> HyperValue {
        HyperMapFloatInt(self)
    }
}

impl ToHyperValue for HashMap<F64, f64> {
    fn to_hyper(self) -> HyperValue {
        HyperMapFloatFloat(self)
    }
}

/// Unfortunately floats do not implement Ord nor Eq, so we have to do it for them
/// by wrapping them in a struct and implement those traits
#[deriving(Show, Clone)]
pub struct F64(pub f64);

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

