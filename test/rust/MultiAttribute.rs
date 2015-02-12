
#[macro_use] extern crate hyperdex;

use std::os;
use std::str::FromStr;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::iter::FromIterator;

use hyperdex::*;
use hyperdex::HyperPredicateType::*;

fn main() {
    let args = os::args();
    let mut client = Client::new(FromStr::from_str(format!("{}:{}", args[1], args[2]).as_slice()).unwrap()).unwrap();

                match client.get("kv", "k") {
                    Ok(obj) => {
                         panic!("this object should not be found!");
                    },
                    Err(err) => (),
                }
            
                match client.put("kv", "k", NewHyperObject!("v1", "ABC",)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v1", "ABC", "v2", "",);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v2", "123",)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v1", "ABC", "v2", "123",);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v1", "ABC",);
                match client.get_partial("kv", "k", vec!("v1")) {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            }
