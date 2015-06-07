use std::collections::{HashMap, BTreeSet};
use std::mem::transmute;
use std::hash::{Hash, Hasher};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::iter::FromIterator;
use std::cmp::Ordering;
use std::ptr::Unique;
use std::hash;
use std::fmt::Debug;

use rustc_serialize::json::Json;

use libc::*;

use common::*;

use hyperdex_client::*;
use hyperdex_datastructures::*;
use hyperdex::*;

use self::HyperValue::*;
use self::HyperState::*;
use self::HyperObjectKeyError::*;

/// Types of values that HyperDex accepts.
#[derive(Debug, Clone, PartialEq)]
pub enum HyperValue {
    HyperString(Vec<u8>),
    HyperInt(i64),
    HyperFloat(f64),

    HyperListString(Vec<Vec<u8>>),
    HyperListInt(Vec<i64>),
    HyperListFloat(Vec<f64>),

    HyperSetString(BTreeSet<Vec<u8>>),
    HyperSetInt(BTreeSet<i64>),
    HyperSetFloat(BTreeSet<F64>),

    HyperMapStringString(HashMap<Vec<u8>, Vec<u8>>),
    HyperMapStringInt(HashMap<Vec<u8>, i64>),
    HyperMapStringFloat(HashMap<Vec<u8>, f64>),

    HyperMapIntString(HashMap<i64, Vec<u8>>),
    HyperMapIntInt(HashMap<i64, i64>),
    HyperMapIntFloat(HashMap<i64, f64>),

    HyperMapFloatString(HashMap<F64, Vec<u8>>),
    HyperMapFloatInt(HashMap<F64, i64>),
    HyperMapFloatFloat(HashMap<F64, f64>),

    HyperDocument(Json)
}

pub struct SearchState {
    pub status: Box<Enum_hyperdex_client_returncode>,
    pub attrs: Box<AttributePtr>,
    pub attrs_sz: Box<size_t>,
    pub res_tx: Sender<Result<HyperObject, HyperError>>,
}

pub enum HyperState {
    HyperStateOp(Sender<HyperError>),  // for calls that don't return values
    HyperStateSearch(SearchState),  // for calls that do return values
}

pub struct Request {
    id: int64_t,
    confirm_tx: Sender<bool>,
}

/// Predicates that HyperDex supports.
pub enum HyperPredicateType {
    FAIL = HYPERPREDICATE_FAIL as isize,
    EQUALS = HYPERPREDICATE_EQUALS as isize,
    LESS_THAN = HYPERPREDICATE_LESS_THAN as isize,
    LESS_EQUAL = HYPERPREDICATE_LESS_EQUAL as isize,
    GREATER_EQUAL = HYPERPREDICATE_GREATER_EQUAL as isize,
    GREATER_THAN = HYPERPREDICATE_GREATER_THAN as isize,
    REGEX = HYPERPREDICATE_REGEX as isize,
    LENGTH_EQUALS = HYPERPREDICATE_LENGTH_EQUALS as isize,
    LENGTH_LESS_EQUAL = HYPERPREDICATE_LENGTH_LESS_EQUAL as isize,
    LENGTH_GREATER_EQUAL = HYPERPREDICATE_LENGTH_GREATER_EQUAL as isize,
    CONTAINS = HYPERPREDICATE_CONTAINS as isize,
}

/// A predicate used for search.
///
/// # Examples
/// 
/// ```
/// let predicates = vec!(HyperPredicate::new("age", LESS_EQUAL, 25));
/// let res = client.search(space_name, predicates);
/// ```
pub struct HyperPredicate {
    pub attr: String,
    pub value: HyperValue,
    pub predicate: HyperPredicateType,
}

impl HyperPredicate {
    pub fn new<A, T>(attr: A, predicate: HyperPredicateType, value: T)
        -> HyperPredicate where A: ToString, T: ToHyperValue {
        HyperPredicate {
            attr: attr.to_string(),
            value: value.to_hyper(),
            predicate: predicate,
        }
    }
}

/// A key-value pair associated with a specific map attribute
pub struct HyperMapAttribute {
    pub attr: String,
    pub key: HyperValue,
    pub value: HyperValue,
}

/// The errors that can occur upon a lookup from a HyperObject.
#[derive(Debug)]
pub enum HyperObjectKeyError {
    /// The key does not exist.
    KeyDoesNotExist,

    /// The key does exist, but the value is not the type that you think it is.
    ObjectIsAnotherType,
}

use std::fmt::{Display, Formatter, Error};

impl Display for HyperObjectKeyError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        Display::fmt(match *self {
            HyperObjectKeyError::KeyDoesNotExist => "the key does not exist",
            HyperObjectKeyError::ObjectIsAnotherType => "the object is of another type"
        }, f)
    }
}

pub trait FromHyperValue {
    fn from_hyper(val: HyperValue) -> Result<Self, HyperObjectKeyError>;
}

macro_rules! from_hypervalue_impl(
    ($t: ty, $hyper_name: ident) => (
        impl FromHyperValue for $t {
            fn from_hyper(val: HyperValue) -> Result<$t, HyperObjectKeyError> {
                match val {
                    $hyper_name(s) => {
                        Ok(s)
                    },
                    _ => Err(ObjectIsAnotherType),
                }
            }
        }
    )
);

from_hypervalue_impl!(Vec<u8>, HyperString);
from_hypervalue_impl!(i64, HyperInt);
from_hypervalue_impl!(f64, HyperFloat);

from_hypervalue_impl!(Vec<Vec<u8>>, HyperListString);
from_hypervalue_impl!(Vec<i64>, HyperListInt);
from_hypervalue_impl!(Vec<f64>, HyperListFloat);

from_hypervalue_impl!(BTreeSet<Vec<u8>>, HyperSetString);
from_hypervalue_impl!(BTreeSet<i64>, HyperSetInt);
from_hypervalue_impl!(BTreeSet<F64>, HyperSetFloat);

from_hypervalue_impl!(HashMap<Vec<u8>, Vec<u8>>, HyperMapStringString);
from_hypervalue_impl!(HashMap<Vec<u8>, i64>, HyperMapStringInt);
from_hypervalue_impl!(HashMap<Vec<u8>, f64>, HyperMapStringFloat);

from_hypervalue_impl!(HashMap<i64, Vec<u8>>, HyperMapIntString);
from_hypervalue_impl!(HashMap<i64, i64>, HyperMapIntInt);
from_hypervalue_impl!(HashMap<i64, f64>, HyperMapIntFloat);

from_hypervalue_impl!(HashMap<F64, Vec<u8>>, HyperMapFloatString);
from_hypervalue_impl!(HashMap<F64, i64>, HyperMapFloatInt);
from_hypervalue_impl!(HashMap<F64, f64>, HyperMapFloatFloat);

from_hypervalue_impl!(Json, HyperDocument);

/// A HyperDex object.
///
/// # Examples
///
/// ```
/// let mut obj = HyperObject::new();
/// obj.insert("first", "Emin");
/// obj.insert("last", "Sirer");
/// obj.insert("age", 30);
/// ```
/// 
/// Or, using the macro:
/// 
/// ```
/// match client.put(space_name, "robert", NewHyperObject!(
///     "first", "Robert",
///     "last", "Escriva",
///     "age", 25,
/// )) {
///     Ok(()) => (),
///     Err(err) => panic!(err),
/// }
/// ```
#[derive(Debug, PartialEq)]
pub struct HyperObject {
    pub map: HashMap<String, HyperValue>,
}

impl HyperObject {
    pub fn new() -> HyperObject {
        HyperObject {
            map: HashMap::new()
        }
    }

    pub fn insert<K, V>(&mut self, attr: K, val: V) where K: ToString, V: ToHyperValue {
        self.map.insert(attr.to_string(), val.to_hyper());
    }

    pub fn get<K, T>(&self, attr: K) -> Result<T, HyperObjectKeyError> where K: ToString, T: FromHyperValue {
        let val_opt = self.map.get(&attr.to_string());
        match val_opt {
            Some(&ref val) => {
                match FromHyperValue::from_hyper(val.clone()) {
                    Ok(ok) => Ok(ok),
                    Err(err) => Err(err),
                }
            },
            None => {
                Err(KeyDoesNotExist)
            }
        }
    }
}

pub trait ToByteVec {
    fn to_bytes(&self) -> Vec<u8>;
}

impl<'a> ToByteVec for &'a str {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl<'a> ToByteVec for &'a [u8] {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_vec()
    }
}

impl ToByteVec for Vec<u8> {
    fn to_bytes(&self) -> Vec<u8> {
        self.clone()
    }
}

impl ToByteVec for String {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

pub trait ToHyperValue {
    fn to_hyper(self) -> HyperValue;
}

impl<'a> ToHyperValue for &'a str {
    fn to_hyper(self) -> HyperValue {
        let s = self.to_string();
        HyperValue::HyperString(s.into_bytes())
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

impl<'a> ToHyperValue for BTreeSet<&'a str> {
    fn to_hyper(self) -> HyperValue {
        HyperSetString(FromIterator::from_iter(self.into_iter().map(|s| {
            s.as_bytes().to_vec()
        })))
    }
}


impl ToHyperValue for BTreeSet<String> {
    fn to_hyper(self) -> HyperValue {
        HyperSetString(FromIterator::from_iter(self.into_iter().map(|s| {
            s.into_bytes()
        })))
    }
}

impl ToHyperValue for BTreeSet<Vec<u8>> {
    fn to_hyper(self) -> HyperValue {
        HyperSetString(self)
    }
}

impl ToHyperValue for BTreeSet<i64> {
    fn to_hyper(self) -> HyperValue {
        HyperSetInt(self)
    }
}

impl ToHyperValue for BTreeSet<F64> {
    fn to_hyper(self) -> HyperValue {
        HyperSetFloat(self)
    }
}

impl<K: ToByteVec + Hash + Eq, V: ToByteVec> ToHyperValue for HashMap<K, V> {
    fn to_hyper(self) -> HyperValue {
        let mut m = HashMap::new();
        for (k, v) in self.into_iter() {
            m.insert(k.to_bytes(), v.to_bytes());
        }
        HyperMapStringString(m)
    }
}

impl<K: ToByteVec + Hash + Eq> ToHyperValue for HashMap<K, i64> {
    fn to_hyper(self) -> HyperValue {
        let mut m = HashMap::new();
        for (k, v) in self.into_iter() {
            m.insert(k.to_bytes(), v);
        }
        HyperMapStringInt(m)
    }
}

impl<K: ToByteVec + Hash + Eq> ToHyperValue for HashMap<K, f64> {
    fn to_hyper(self) -> HyperValue {
        let mut m = HashMap::new();
        for (k, v) in self.into_iter() {
            m.insert(k.to_bytes(), v);
        }
        HyperMapStringFloat(m)
    }
}

impl<V: ToByteVec> ToHyperValue for HashMap<i64, V> {
    fn to_hyper(self) -> HyperValue {
        let mut m = HashMap::new();
        for (k, v) in self.into_iter() {
            m.insert(k, v.to_bytes());
        }
        HyperMapIntString(m)
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

impl<V: ToByteVec> ToHyperValue for HashMap<F64, V> {
    fn to_hyper(self) -> HyperValue {
        let mut m = HashMap::new();
        for (k, v) in self.into_iter() {
            m.insert(k, v.to_bytes());
        }
        HyperMapFloatString(m)
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

impl ToHyperValue for Json {
    fn to_hyper(self) -> HyperValue {
        HyperDocument(self)
    }
}

/// A wrapper around f64.
/// 
/// Unfortunately f64 does not implement Ord nor Eq, so we have to do it manually
/// by wrapping f64 in a struct and implement those traits
#[derive(Debug, Clone)]
pub struct F64(pub f64);

impl PartialEq for F64 {
    fn eq(&self, other: &F64) -> bool {
        if self.0 == other.0 {
            true
        } else {
            false
        }
    }
}

impl PartialOrd for F64 {
    fn partial_cmp(&self, other: &F64) -> Option<Ordering> {
        // Kinda hacky, but I think this should work...
        if self.0 > other.0 {
            Some(Ordering::Greater)
        } else if self.0 < other.0 {
            Some(Ordering::Less)
        } else {
            Some(Ordering::Equal)
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
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        unsafe {
            transmute::<f64, u64>(self.0)
        }.hash(state)
    }
}
