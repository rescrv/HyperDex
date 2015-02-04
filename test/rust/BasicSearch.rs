
extern crate hyperdex;

use std::os;

use hyperdex::{Client, NewHyperObject};

fn main() {
    let args = os::args();
    let mut client = Client::new(from_str(format!("{}:{}", args[1], args[2])).unwrap()).unwrap();

            let res = client.search("kv", vec!(HyperPredicate::new("v", EQUALS, "v1")));
            let elems = res.iter().collect();
            assert!(elems.len() == BTreeSet::from_iter(vec!().iter());.len());
        
                match client.put("kv", "k1", NewHyperObject!("v", "v1")) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
            let res = client.search("kv", vec!(HyperPredicate::new("v", EQUALS, "v1")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "k1", "v", "v1"));.len());
        
                match client.put("kv", "k2", NewHyperObject!("v", "v1")) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
            let res = client.search("kv", vec!(HyperPredicate::new("v", EQUALS, "v1")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "k1", "v", "v1"), NewHyperObject!("k", "k2", "v", "v1"));.len());
        }
