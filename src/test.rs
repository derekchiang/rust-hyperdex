use std::collections::HashMap;

use admin::*;
use client::*;
use super::*;
use super::HyperValue::*;
use super::HyperPredicateType::*;
use hyperdex_client::*;

static coord_addr: &'static str = "127.0.0.1:1982";

static space_name: &'static str = "phonebook";

static space_desc: &'static str = "
space phonebook
key username
attributes first, last, int age
subspace first, last
create 8 partitions
tolerate 2 failures";

#[test]
fn test_add_and_rm_space() {
    let admin = Admin::new(from_str(coord_addr).unwrap()).unwrap();

    match admin.add_space(space_desc) {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    admin.remove_space(space_name).unwrap();
} 

#[test]
fn test_get_nonexistent_objects() {
    let admin = Admin::new(from_str(coord_addr).unwrap()).unwrap();

    match admin.add_space(space_desc) {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    let mut client = Client::new(from_str(coord_addr).unwrap()).unwrap();
    match client.get(space_name, "lol".as_bytes().to_vec()) {
        Ok(obj) => panic!("wrongly getting an object: {}", obj),
        Err(err) => assert!(err.status == HYPERDEX_CLIENT_NOTFOUND),
    }

    admin.remove_space(space_name).unwrap();
}

#[test]
fn test_add_and_get_objects() {
    let admin = Admin::new(from_str(coord_addr).unwrap()).unwrap();
    match admin.add_space(space_desc) {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    let key = "derek".as_bytes().to_vec();
    let value = NewHyperObject!(
        "first": "Derek",
        "last": "Chiang",
    );

    let mut client = Client::new(from_str(coord_addr).unwrap()).unwrap();
    match client.put(space_name, key, value) {
        Ok(()) => (),
        Err(err) => panic!("{}", err),
    }

    match client.get(space_name, "derek".as_bytes().to_vec()) {
        Ok(mut obj) => {
            let first_str = "first".into_string();
            let last_str = "last".into_string();
            let first = match obj.find_copy(&first_str).unwrap() {
                HyperString(s) => s,
                x => panic!(x),
            };

            let last = match obj.find_copy(&last_str).unwrap() {
                HyperString(s) => s,
                x => panic!(x),
            };

            assert_eq!(first, "Derek".as_bytes().to_vec());
            assert_eq!(last, "Chiang".as_bytes().to_vec());
        },
        Err(err) => panic!(err),
    }

    admin.remove_space(space_name).unwrap();
}

#[test]
fn test_add_and_search_objects() {
    let admin = Admin::new(from_str(coord_addr).unwrap()).unwrap();
    match admin.add_space(space_desc) {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    let mut client = Client::new(from_str(coord_addr).unwrap()).unwrap();

    match put!(client, space_name, "derek", NewHyperObject!(
        "first": "Derek",
        "last": "Chiang",
        "age": 20,
    )) {
        Ok(()) => (),
        Err(err) => panic!("{}", err),
    }

    match put!(client, space_name, "nemo", NewHyperObject!(
        "first": "Derek",
        "last": "Chiang",
        "age": 30,
    )) {
        Ok(()) => (),
        Err(err) => panic!("{}", err),
    }

    match put!(client, space_name, "ohwell", NewHyperObject!(
        "first": "Derek",
        "last": "Chiang",
        "age": 40,
    )) {
        Ok(()) => (),
        Err(err) => panic!("{}", err),
    }

    match put!(client, space_name, "whatup", NewHyperObject!(
        "first": "Derek",
        "last": "Chiang",
        "age": 50,
    )) {
        Ok(()) => (),
        Err(err) => panic!("{}", err),
    }

    let mut predicates = Vec::new();
    predicates.push(HyperPredicate {
        attr: "age".into_string(),
        value: HyperInt(30),
        predicate: LESS_EQUAL,
    });

    let res = client.search(space_name, predicates);

    for obj in res.iter() {
        println!("{}", obj.unwrap());
    }

    // match res.recv().unwrap().remove(&"age".into_string()).unwrap() {
        // HyperInt(i) => assert_eq!(i, 20),
        // x => panic!(x),
    // }

    // match res.recv().unwrap().remove(&"age".into_string()).unwrap() {
        // HyperInt(i) => assert_eq!(i, 30),
        // x => panic!(x),
    // }

    println!("the space name is: {}", space_name);
    admin.remove_space(space_name).unwrap();
}
