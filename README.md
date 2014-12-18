# rust-hyperdex [![Build Status](https://travis-ci.org/derekchiang/rust-hyperdex.svg?branch=master)](https://travis-ci.org/derekchiang/rust-hyperdex)

Rust binding for [HyperDex](http://hyperdex.org/).

## Prerequisites

You need to install the HyperDex development files.  Assuming you have added HyperDex's PPA by following [these instructions](http://hyperdex.org/download/), you may simply install the following packages:

    sudo apt-get install libhyperdex-dev libhyperdex-client-dev libhyperdex-admin-dev

## Installation

Using [Cargo](https://crates.io/), the Rust package manager, it's as easy as adding the following lines to your `Cargo.toml` file:

    [dependencies]
    hyperdex = "*"

## Examples

You may find complete examples in [this repo](https://github.com/derekchiang/rust-hyperdex-examples/).  The [tests](src/test.rs) may also be worth a look.
