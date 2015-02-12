
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

                match client.put("kv", "foo/foo/foo", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/foo/bar", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/foo/baz", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/bar/foo", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/bar/bar", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/bar/baz", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/baz/foo", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/baz/bar", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "foo/baz/baz", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/foo/foo", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/foo/bar", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/foo/baz", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/bar/foo", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/bar/bar", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/bar/baz", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/baz/foo", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/baz/bar", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "bar/baz/baz", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/foo/foo", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/foo/bar", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/foo/baz", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/bar/foo", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/bar/bar", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/bar/baz", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/baz/foo", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/baz/bar", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
                match client.put("kv", "baz/baz/baz", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            
            let res = client.search("kv", vec!(HyperPredicate::new("k", REGEX, "^foo")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "foo/foo/foo",), NewHyperObject!("k", "foo/foo/bar",), NewHyperObject!("k", "foo/foo/baz",), NewHyperObject!("k", "foo/bar/foo",), NewHyperObject!("k", "foo/bar/bar",), NewHyperObject!("k", "foo/bar/baz",), NewHyperObject!("k", "foo/baz/foo",), NewHyperObject!("k", "foo/baz/bar",), NewHyperObject!("k", "foo/baz/baz",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", REGEX, "foo$")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "foo/foo/foo",), NewHyperObject!("k", "foo/bar/foo",), NewHyperObject!("k", "foo/baz/foo",), NewHyperObject!("k", "bar/foo/foo",), NewHyperObject!("k", "bar/bar/foo",), NewHyperObject!("k", "bar/baz/foo",), NewHyperObject!("k", "baz/foo/foo",), NewHyperObject!("k", "baz/bar/foo",), NewHyperObject!("k", "baz/baz/foo",)).len());
        
            let res = client.search("kv", vec!(HyperPredicate::new("k", REGEX, "^b.*/foo/.*$")));
            let elems: Vec<Result<HyperObject, HyperError>> = res.iter().collect();
            assert!(elems.len() == vec!(NewHyperObject!("k", "bar/foo/foo",), NewHyperObject!("k", "bar/foo/bar",), NewHyperObject!("k", "bar/foo/baz",), NewHyperObject!("k", "baz/foo/foo",), NewHyperObject!("k", "baz/foo/bar",), NewHyperObject!("k", "baz/foo/baz",)).len());
        }
