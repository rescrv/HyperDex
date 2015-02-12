
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

                match client.put("kv", "A", NewHyperObject!("v", "A",)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "B", NewHyperObject!("v", "B",)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "C", NewHyperObject!("v", "C",)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "D", NewHyperObject!("v", "D",)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "E", NewHyperObject!("v", "E",)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
            let res = client.search("kv", vec!(HyperPredicate::new("k", LESS_EQUAL, "C")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A", "v", "A",), NewHyperObject!("k", "B", "v", "B",), NewHyperObject!("k", "C", "v", "C",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", LESS_EQUAL, "C")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A", "v", "A",), NewHyperObject!("k", "B", "v", "B",), NewHyperObject!("k", "C", "v", "C",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", GREATER_EQUAL, "C")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "C", "v", "C",), NewHyperObject!("k", "D", "v", "D",), NewHyperObject!("k", "E", "v", "E",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", GREATER_EQUAL, "C")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "C", "v", "C",), NewHyperObject!("k", "D", "v", "D",), NewHyperObject!("k", "E", "v", "E",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LESS_THAN, "C")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A", "v", "A",), NewHyperObject!("k", "B", "v", "B",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", LESS_THAN, "C")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A", "v", "A",), NewHyperObject!("k", "B", "v", "B",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", GREATER_THAN, "C")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "D", "v", "D",), NewHyperObject!("k", "E", "v", "E",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", GREATER_THAN, "C")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "D", "v", "D",), NewHyperObject!("k", "E", "v", "E",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", GREATER_EQUAL, "B"), HyperPredicate::new("k", LESS_EQUAL, "D")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "B", "v", "B",), NewHyperObject!("k", "C", "v", "C",), NewHyperObject!("k", "D", "v", "D",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", GREATER_EQUAL, "B"), HyperPredicate::new("v", LESS_EQUAL, "D")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "B", "v", "B",), NewHyperObject!("k", "C", "v", "C",), NewHyperObject!("k", "D", "v", "D",)).len());
        }
