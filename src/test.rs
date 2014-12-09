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
    match get!(client, space_name, "lol") {
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

    let mut client = Client::new(from_str(coord_addr).unwrap()).unwrap();
    match put!(client, space_name, "derek", NewHyperObject!(
        "first": "Derek",
        "last": "Chiang",
    )) {
        Ok(()) => (),
        Err(err) => panic!("{}", err),
    }

    match get!(client, space_name, "derek") {
        Ok(mut obj) => {
            let first_str = "first".into_string();
            let last_str = "last".into_string();
            let first: Vec<u8> = match obj.get(first_str) {
                Ok(s) => s,
                Err(err) => panic!(err),
            };

            let last: Vec<u8> = match obj.get(last_str) {
                Ok(s) => s,
                Err(err) => panic!(err),
            };

            assert_eq!(first, "Derek".to_bytes());
            assert_eq!(last, "Chiang".to_bytes());
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

    let mut obj = HyperObject::new();
    obj.insert("first".into_string(), "Derek");
    obj.insert("last".into_string(), "Chiang");
    obj.insert("age".into_string(), 40);
    match put!(client, space_name, "ohwell", obj) {
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

    let whatever: Result<u64, String> = Ok(6u64);
    println!("{}", whatever.unwrap());
    
    for obj_res in res.iter() {
        let obj = obj_res.unwrap();
        let name: Vec<u8> = obj.get("first".into_string()).unwrap();
        let age: i64 = obj.get("age".into_string()).unwrap();
        assert!(age <= 30);
        println!("{} is {} years old", name, age);
    }

    admin.remove_space(space_name).unwrap();
}
