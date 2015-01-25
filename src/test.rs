use std::collections::HashMap;
use std::str::FromStr;

use super::*;
use super::HyperValue::*;
use super::HyperPredicateType::*;
use hyperdex_client::*;

static coord_addr: &'static str = "127.0.0.1:1982";

static space_name: &'static str = "contacts";

static space_desc: &'static str = "
space contacts
key username
attributes first, last, int age
subspace first, last
create 2 partitions
tolerate 2 failures";

#[test]
fn test_add_and_rm_space() {
    let admin = Admin::new(FromStr::from_str(coord_addr).unwrap()).unwrap();

    match admin.add_space(space_desc) {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    admin.remove_space(space_name).unwrap();
}

#[test]
fn test_get_nonexistent_objects() {
    let admin = Admin::new(FromStr::from_str(coord_addr).unwrap()).unwrap();

    match admin.add_space(space_desc) {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    let mut client = Client::new(FromStr::from_str(coord_addr).unwrap()).unwrap();
    match client.get(space_name, "lol") {
        Ok(obj) => panic!("wrongly getting an object: {:?}", obj),
        Err(err) => assert!(err.status == HYPERDEX_CLIENT_NOTFOUND),
    }

    admin.remove_space(space_name).unwrap();
}

#[test]
fn test_add_and_get_objects() {
    let admin = Admin::new(FromStr::from_str(coord_addr).unwrap()).unwrap();
    match admin.add_space(space_desc) {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    let mut client = Client::new(FromStr::from_str(coord_addr).unwrap()).unwrap();
    match client.put(space_name, "derek", NewHyperObject!(
        "first", "Derek",
        "last", "Chiang",
    )) {
        Ok(()) => (),
        Err(err) => panic!(err),
    }

    match client.get(space_name, "derek") {
        Ok(mut obj) => {
            let first: Vec<u8> = match obj.get("first") {
                Ok(s) => s,
                Err(err) => panic!(err),
            };

            let last: Vec<u8> = match obj.get("last") {
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
    let admin = Admin::new(FromStr::from_str(coord_addr).unwrap()).unwrap();
    match admin.add_space(space_desc) {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    let mut client = Client::new(FromStr::from_str(coord_addr).unwrap()).unwrap();

    match client.put(space_name, "derek", NewHyperObject!(
        "first", "Derek",
        "last", "Chiang",
        "age", 20,
    )) {
        Ok(()) => (),
        Err(err) => panic!(err),
    }

    match client.put(space_name, "robert", NewHyperObject!(
        "first", "Robert",
        "last", "Escriva",
        "age", 25,
    )) {
        Ok(()) => (),
        Err(err) => panic!(err),
    }

    let mut obj = HyperObject::new();
    obj.insert("first", "Emin");
    obj.insert("last", "Sirer");
    obj.insert("age", 30);

    let fut = client.async_put(space_name, "emin", obj);
    match fut.into_inner() {
        Ok(()) => (),
        Err(err) => panic!(err),
    }

    let predicates = vec!(HyperPredicate::new("age", LESS_EQUAL, 25));

    let res = client.search(space_name, predicates);

    for obj_res in res.iter() {
        let obj = obj_res.unwrap();
        let name: Vec<u8> = obj.get("first").unwrap();
        let age: i64 = obj.get("age").unwrap();
        assert!(age <= 25);
        println!("{:?} is {} years old", name, age);
    }

    admin.remove_space(space_name).unwrap();
}
