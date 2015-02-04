
extern crate hyperdex;

use std::os;

use hyperdex::{Client, NewHyperObject};

fn main() {
    let args = os::args();
    let mut client = Client::new(from_str(format!("{}:{}", args[1], args[2])).unwrap()).unwrap();

                match client.put("kv", "A", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "AB", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "ABC", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "ABCD", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "ABCDE", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_EQUALS, 1 as i64)));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_EQUALS, 2 as i64)));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "AB"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_EQUALS, 3 as i64)));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "ABC"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_EQUALS, 4 as i64)));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "ABCD"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_EQUALS, 5 as i64)));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "ABCDE"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_LESS_EQUAL, 3 as i64)));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A"), NewHyperObject!("k", "AB"), NewHyperObject!("k", "ABC"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LENGTH_GREATER_EQUAL, 3 as i64)));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "ABC"), NewHyperObject!("k", "ABCD"), NewHyperObject!("k", "ABCDE"));.len());
        }
