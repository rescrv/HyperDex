
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
            let expected = NewHyperObject!("v", vec!(););
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v", vec!("A", "B", "C");)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", vec!("A", "B", "C"););
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v", vec!();)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", vec!(););
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
