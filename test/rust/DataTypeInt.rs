
extern crate hyperdex;

use std::os;

use hyperdex::{Client, NewHyperObject};

fn main() {
    let args = os::args();
    let mut client = Client::new(from_str(format!("{}:{}", args[1], args[2])).unwrap()).unwrap();

                match client.put("kv", "k", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", 0 as i64);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v", 1 as i64)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", 1 as i64);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v", -1 as i64)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", -1 as i64);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v", 0 as i64)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", 0 as i64);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v", 9223372036854775807 as i64)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", 9223372036854775807 as i64);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v", -9223372036854775808 as i64)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", -9223372036854775808 as i64);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            }
