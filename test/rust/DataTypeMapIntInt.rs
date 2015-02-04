
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
            let expected = NewHyperObject!("v", NewHyperObject!());
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v", NewHyperObject!(1 as i64, 7 as i64, 2 as i64, 8 as i64, 3 as i64, 9 as i64))) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", NewHyperObject!(1 as i64, 7 as i64, 2 as i64, 8 as i64, 3 as i64, 9 as i64));
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v", NewHyperObject!())) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", NewHyperObject!());
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
