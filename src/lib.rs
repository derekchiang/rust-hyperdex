#![feature(concat_idents)]
#![feature(slicing_syntax)]
#![feature(box_syntax)]
#![feature(libc)]
#![feature(unique)]
#![feature(std_misc)]
#![feature(collections)]
#![feature(ip_addr)]
#![feature(convert)]

extern crate libc;
extern crate rustc_serialize;

pub use common::HyperError;
pub use client::{Client};
pub use client_types::{F64, HyperMapAttribute, HyperObject, HyperPredicate, HyperObjectKeyError, HyperPredicateType, HyperValue};
pub use admin::Admin;

mod client;
mod admin;

mod hyperdex;
mod hyperdex_client;
mod hyperdex_admin;
mod hyperdex_datastructures;
mod hyperdex_hyperspace_builder;
mod common;
mod test;
mod client_types;
