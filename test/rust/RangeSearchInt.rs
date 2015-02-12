
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

                match client.put("kv", -2 as i64, NewHyperObject!("v", -2 as i64,)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", -1 as i64, NewHyperObject!("v", -1 as i64,)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", 0 as i64, NewHyperObject!("v", 0 as i64,)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", 1 as i64, NewHyperObject!("v", 1 as i64,)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", 2 as i64, NewHyperObject!("v", 2 as i64,)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
            let res = client.search("kv", vec!(HyperPredicate::new("k", LESS_EQUAL, 0 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", -2 as i64, "v", -2 as i64,), NewHyperObject!("k", -1 as i64, "v", -1 as i64,), NewHyperObject!("k", 0 as i64, "v", 0 as i64,)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", LESS_EQUAL, 0 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", -2 as i64, "v", -2 as i64,), NewHyperObject!("k", -1 as i64, "v", -1 as i64,), NewHyperObject!("k", 0 as i64, "v", 0 as i64,)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", GREATER_EQUAL, 0 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", 0 as i64, "v", 0 as i64,), NewHyperObject!("k", 1 as i64, "v", 1 as i64,), NewHyperObject!("k", 2 as i64, "v", 2 as i64,)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", GREATER_EQUAL, 0 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", 0 as i64, "v", 0 as i64,), NewHyperObject!("k", 1 as i64, "v", 1 as i64,), NewHyperObject!("k", 2 as i64, "v", 2 as i64,)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LESS_THAN, 0 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", -2 as i64, "v", -2 as i64,), NewHyperObject!("k", -1 as i64, "v", -1 as i64,)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", LESS_THAN, 0 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", -2 as i64, "v", -2 as i64,), NewHyperObject!("k", -1 as i64, "v", -1 as i64,)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", GREATER_THAN, 0 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", 1 as i64, "v", 1 as i64,), NewHyperObject!("k", 2 as i64, "v", 2 as i64,)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", GREATER_THAN, 0 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", 1 as i64, "v", 1 as i64,), NewHyperObject!("k", 2 as i64, "v", 2 as i64,)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", GREATER_EQUAL, -1 as i64), HyperPredicate::new("k", LESS_EQUAL, 1 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", -1 as i64, "v", -1 as i64,), NewHyperObject!("k", 0 as i64, "v", 0 as i64,), NewHyperObject!("k", 1 as i64, "v", 1 as i64,)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", GREATER_EQUAL, -1 as i64), HyperPredicate::new("v", LESS_EQUAL, 1 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", -1 as i64, "v", -1 as i64,), NewHyperObject!("k", 0 as i64, "v", 0 as i64,), NewHyperObject!("k", 1 as i64, "v", 1 as i64,)).len());
        }
