
extern crate hyperdex;

use std::os;

use hyperdex::{Client, NewHyperObject};

fn main() {
    let args = os::args();
    let mut client = Client::new(from_str(format!("{}:{}", args[1], args[2])).unwrap()).unwrap();

                match client.put("kv", "foo/foo/foo", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/foo/bar", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/foo/baz", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/bar/foo", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/bar/bar", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/bar/baz", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/baz/foo", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/baz/bar", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/baz/baz", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/foo/foo", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/foo/bar", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/foo/baz", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/bar/foo", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/bar/bar", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/bar/baz", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/baz/foo", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/baz/bar", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/baz/baz", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/foo/foo", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/foo/bar", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/foo/baz", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/bar/foo", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/bar/bar", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/bar/baz", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/baz/foo", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/baz/bar", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/baz/baz", NewHyperObject!()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
            let res = client.search("kv", vec!(HyperPredicate::new("k", REGEX, "^foo")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "foo/foo/foo"), NewHyperObject!("k", "foo/foo/bar"), NewHyperObject!("k", "foo/foo/baz"), NewHyperObject!("k", "foo/bar/foo"), NewHyperObject!("k", "foo/bar/bar"), NewHyperObject!("k", "foo/bar/baz"), NewHyperObject!("k", "foo/baz/foo"), NewHyperObject!("k", "foo/baz/bar"), NewHyperObject!("k", "foo/baz/baz"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", REGEX, "foo$")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "foo/foo/foo"), NewHyperObject!("k", "foo/bar/foo"), NewHyperObject!("k", "foo/baz/foo"), NewHyperObject!("k", "bar/foo/foo"), NewHyperObject!("k", "bar/bar/foo"), NewHyperObject!("k", "bar/baz/foo"), NewHyperObject!("k", "baz/foo/foo"), NewHyperObject!("k", "baz/bar/foo"), NewHyperObject!("k", "baz/baz/foo"));.len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", REGEX, "^b.*/foo/.*$")));
            let elems = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "bar/foo/foo"), NewHyperObject!("k", "bar/foo/bar"), NewHyperObject!("k", "bar/foo/baz"), NewHyperObject!("k", "baz/foo/foo"), NewHyperObject!("k", "baz/foo/bar"), NewHyperObject!("k", "baz/foo/baz"));.len());
        }
