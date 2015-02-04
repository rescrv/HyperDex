
extern crate hyperdex;

use std::os;

use hyperdex::{Client, NewHyperObject};

fn main() {
    let args = os::args();
    let mut client = Client::new(from_str(format!("{}:{}", args[1], args[2])).unwrap()).unwrap();

                match client.put("kv", "A", NewHyperObject!("v", "A")) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "B", NewHyperObject!("v", "B")) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "C", NewHyperObject!("v", "C")) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "D", NewHyperObject!("v", "D")) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "E", NewHyperObject!("v", "E")) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
            let res = client.search("kv", vec!(HyperPredicate::new("k", LESS_EQUAL, "C")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A", "v", "A"), NewHyperObject!("k", "B", "v", "B"), NewHyperObject!("k", "C", "v", "C"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", LESS_EQUAL, "C")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A", "v", "A"), NewHyperObject!("k", "B", "v", "B"), NewHyperObject!("k", "C", "v", "C"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", GREATER_EQUAL, "C")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "C", "v", "C"), NewHyperObject!("k", "D", "v", "D"), NewHyperObject!("k", "E", "v", "E"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", GREATER_EQUAL, "C")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "C", "v", "C"), NewHyperObject!("k", "D", "v", "D"), NewHyperObject!("k", "E", "v", "E"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", LESS_THAN, "C")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A", "v", "A"), NewHyperObject!("k", "B", "v", "B"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", LESS_THAN, "C")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "A", "v", "A"), NewHyperObject!("k", "B", "v", "B"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", GREATER_THAN, "C")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "D", "v", "D"), NewHyperObject!("k", "E", "v", "E"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", GREATER_THAN, "C")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "D", "v", "D"), NewHyperObject!("k", "E", "v", "E"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", "B", GREATER_EQUAL), HyperPredicate::new("k", "D", LESS_EQUAL)));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "B", "v", "B"), NewHyperObject!("k", "C", "v", "C"), NewHyperObject!("k", "D", "v", "D"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("v", "B", GREATER_EQUAL), HyperPredicate::new("v", "D", LESS_EQUAL)));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "B", "v", "B"), NewHyperObject!("k", "C", "v", "C"), NewHyperObject!("k", "D", "v", "D"));.len());
        }
