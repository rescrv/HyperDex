
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

            let res = client.search("kv", vec!(HyperPredicate::new("v", EQUALS, "v1")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == BTreeSet::<Vec<u8>>::new().len());
        
                match client.put("kv", "k1", NewHyperObject!("v", "v1",)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
            let res = client.search("kv", vec!(HyperPredicate::new("v", EQUALS, "v1")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "k1", "v", "v1",)).len());
        
                match client.put("kv", "k2", NewHyperObject!("v", "v1",)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
            let res = client.search("kv", vec!(HyperPredicate::new("v", EQUALS, "v1")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "k1", "v", "v1",), NewHyperObject!("k", "k2", "v", "v1",)).len());
        }
