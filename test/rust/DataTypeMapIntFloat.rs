
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

                match client.put("kv", "k", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", HashMap::<Vec<u8>, Vec<u8>>::new(),);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v", {
let mut m = HashMap::new();
m.insert(1 as i64, 3.14 as f64);
m.insert(2 as i64, 0.25 as f64);
m.insert(3 as i64, 1.0 as f64);
m
}
,)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", {
let mut m = HashMap::new();
m.insert(1 as i64, 3.14 as f64);
m.insert(2 as i64, 0.25 as f64);
m.insert(3 as i64, 1.0 as f64);
m
}
,);
                match client.get("kv", "k") {
                    Ok(obj) => {
                        if obj != expected {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "k", NewHyperObject!("v", HashMap::<Vec<u8>, Vec<u8>>::new(),)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!("v", HashMap::<Vec<u8>, Vec<u8>>::new(),);
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
