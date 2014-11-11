use std::collections::HashMap;

use admin::*;
use client::*;
use super::*;
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
    match admin.add_space(space_desc.into_string()).recv() {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    admin.remove_space(space_name.into_string()).recv().unwrap();
}

#[test]
fn test_get_nonexistent_objects() {
    let admin = Admin::new(from_str(coord_addr).unwrap()).unwrap();
    match admin.add_space(space_desc.into_string()).recv() {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    let mut client = Client::new(from_str(coord_addr).unwrap()).unwrap();
    match client.get(space_name.into_string(), "lol".as_bytes().to_vec()).unwrap() {
        Ok(obj) => panic!("wrongly getting an object: {}", obj),
        Err(err) => assert!(err.status == HYPERDEX_CLIENT_NOTFOUND),
    }

    admin.remove_space(space_name.into_string()).recv().unwrap();
}

#[test]
fn test_add_and_get_objects() {
    let admin = Admin::new(from_str(coord_addr).unwrap()).unwrap();
    match admin.add_space(space_desc.into_string()).recv() {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    let key = "derek".as_bytes().to_vec();
    let mut value = HashMap::new();
    value.insert("first".into_string(), HyperString("Derek".as_bytes().to_vec()));
    value.insert("last".into_string(), HyperString("Chiang".as_bytes().to_vec()));

    let mut client = Client::new(from_str(coord_addr).unwrap()).unwrap();
    match client.put(space_name.into_string(), key, value).unwrap() {
        Ok(()) => (),
        Err(err) => panic!("{}", err),
    }

    match client.get(space_name.into_string(), "derek".as_bytes().to_vec()).unwrap() {
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

    admin.remove_space(space_name.into_string()).recv().unwrap();
}

#[test]
fn test_add_and_search_objects() {
    let admin = Admin::new(from_str(coord_addr).unwrap()).unwrap();
    match admin.add_space(space_desc.into_string()).recv() {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    let k1 = "derek".as_bytes().to_vec();
    let mut v1 = HashMap::new();
    v1.insert("first".into_string(), HyperString("Derek".as_bytes().to_vec()));
    v1.insert("last".into_string(), HyperString("Chiang".as_bytes().to_vec()));
    v1.insert("age".into_string(), HyperInt(20));

    let k2 = "nemo".as_bytes().to_vec();
    let mut v2 = HashMap::new();
    v2.insert("first".into_string(), HyperString("Derek".as_bytes().to_vec()));
    v2.insert("last".into_string(), HyperString("Chiang".as_bytes().to_vec()));
    v2.insert("age".into_string(), HyperInt(30));

    let k3 = "ohwell".as_bytes().to_vec();
    let mut v3 = HashMap::new();
    v3.insert("first".into_string(), HyperString("Derek".as_bytes().to_vec()));
    v3.insert("last".into_string(), HyperString("Chiang".as_bytes().to_vec()));
    v3.insert("age".into_string(), HyperInt(40));

    let mut client = Client::new(from_str(coord_addr).unwrap()).unwrap();

    match client.put(space_name.into_string(), k1, v1).unwrap() {
        Ok(()) => (),
        Err(err) => panic!("{}", err),
    }
    match client.put(space_name.into_string(), k2, v2).unwrap() {
        Ok(()) => (),
        Err(err) => panic!("{}", err),
    }
    match client.put(space_name.into_string(), k3, v3).unwrap() {
        Ok(()) => (),
        Err(err) => panic!("{}", err),
    }

    let mut predicates = Vec::new();
    predicates.push(HyperPredicate {
        attr: "age".into_string(),
        value: HyperInt(30),
        predicate: LESS_EQUAL,
    });

    let res = client.search(space_name.into_string(), predicates);

    match res.recv().unwrap().remove(&"age".into_string()).unwrap() {
        HyperInt(i) => assert_eq!(i, 20),
        x => panic!(x),
    }

    match res.recv().unwrap().remove(&"age".into_string()).unwrap() {
        HyperInt(i) => assert_eq!(i, 30),
        x => panic!(x),
    }

    admin.remove_space(space_name.into_string()).recv().unwrap();
}

