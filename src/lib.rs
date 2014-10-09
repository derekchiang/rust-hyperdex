#![feature(globs)]

extern crate libc;
extern crate sync;

pub mod client;
pub mod admin;

mod hyperdex;
mod hyperdex_client;
mod hyperdex_admin;
mod hyperdex_datastructures;
mod hyperdex_hyperspace_builder;
