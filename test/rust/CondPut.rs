
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
            
                match client.put("kv", "k", NewHyperObject!("v", "v1",)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", "v1",);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.cond_put("kv", "k", vec!(HyperPredicate::new("v", EQUALS, "v2")), NewHyperObject!("v", "v3",)) {
                    Ok(()) => panic!("this CONDPUT operation should have failed"),
                    Err(err) => (),
                }
            let expected = NewHyperObject!("v", "v1",);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.cond_put("kv", "k", vec!(HyperPredicate::new("v", EQUALS, "v1")), NewHyperObject!("v", "v3",)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", "v3",);
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
