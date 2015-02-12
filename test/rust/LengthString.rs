
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

                match client.put("kv", "A", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "AB", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "ABC", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "ABCD", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "ABCDE", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_EQUALS, 1 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_EQUALS, 2 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "AB",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_EQUALS, 3 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "ABC",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_EQUALS, 4 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "ABCD",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_EQUALS, 5 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "ABCDE",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_LESS_EQUAL, 3 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A",), NewHyperObject!("k", "AB",), NewHyperObject!("k", "ABC",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_GREATER_EQUAL, 3 as i64)));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "ABC",), NewHyperObject!("k", "ABCD",), NewHyperObject!("k", "ABCDE",)).len());
        }
