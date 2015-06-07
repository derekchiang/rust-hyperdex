# rust-hyperdex

Rust binding for [HyperDex](http://hyperdex.org/).

## Prerequisites

This binding makes use of several experimental features including macros, which are only available on Rust Beta and Rust Nightly.  [Refer to here for installing Rust](http://www.rust-lang.org/install.html).

To compile the binding, you need to install the HyperDex development files.  Assuming you are using Ubuntu and you have added HyperDex's PPA by following [these instructions](http://hyperdex.org/download/), you may simply install the following packages:

    sudo apt-get install libhyperdex-dev libhyperdex-client-dev libhyperdex-admin-dev

## Installation

Using [Cargo](https://crates.io/), the Rust package manager, it's as easy as adding the following lines to your `Cargo.toml` file:

    [dependencies]
    hyperdex = "*"

Then, to use the crate in your code, add this line:

    #[macro_use] extern crate hyperdex;  // use #[macro_use] if you want to use the macros

## Documentation

http://derekchiang.github.io/rust-hyperdex/

## Examples

The [tests](src/test.rs) are worth a look.

Here is a simple application using the binding: https://github.com/derekchiang/rust-hyperdex-example

## Testing

Most of the tests are included in the HyperDex repo itself.  The repo also includes a few test cases.

Before you run the tests, you need to start the HyperDex coordinator at `127.0.0.1:1982` and also start a daemon.

Then, set the following environment variable to make the tests run in series:

    export RUST_TEST_THREADS=1

Now you may run the tests:

    cargo test --lib
