use admin::*;
use client::*;

// #[test]
// fn test_add_and_rm_space() {
    // let admin = Admin::new(from_str("127.0.0.1:1982").unwrap()).unwrap();
    // match admin.add_space("
        // space phonebook
        // key username
        // attributes first, last, int phone
        // subspace first, last, phone
        // create 8 partitions
        // tolerate 2 failures".into_string()).recv() {
        // Ok(()) => (),
        // Err(err) => panic!(format!("{}", err)),
    // };

    // admin.remove_space("phonebook".into_string()).recv().unwrap();
// }

#[test]
fn test_add_objects() {
    let admin = Admin::new(from_str("127.0.0.1:1982").unwrap()).unwrap();
    match admin.add_space("
        space phonebook
        key username
        attributes first, last, int phone
        subspace first, last, phone
        create 8 partitions
        tolerate 2 failures".into_string()).recv() {
        Ok(()) => (),
        Err(err) => panic!(format!("{}", err)),
    };

    let mut client = Client::new(from_str("127.0.0.1:1982").unwrap()).unwrap();
    match client.get("phonebook".into_string(), "lol".as_bytes().to_vec()) {
        Ok(obj) => println!("obj: {}", obj),
        Err(err) => println!("err: {}", err),
    }

    admin.remove_space("phonebook".into_string()).recv().unwrap();
}
