
#![feature(convert)]
#[macro_use] extern crate hyperdex;

use std::os;
use std::str::FromStr;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::mem::transmute;
use std::any::Any;

use hyperdex::*;
use hyperdex::HyperValue::*;
use hyperdex::HyperPredicateType::*;

macro_rules! sloppyCompare {
    ($a: ident, $b: ident) => (
        if $a.len() == 0 && $b.len() == 0 {
            true
        } else {
            false
        }
    );
}

fn sloppyCompareHyper(a: &HyperObject, b: &HyperObject) -> bool {
    for (ak, av) in a.map.iter() {
        let bv = match b.map.get(ak) {
            None => return false,
            Some(x) => x,
        };

        let av = av.clone();
        let bv = bv.clone();

        // this is dumb but I do not see a better way... fortunately the following lines are generated
        // by a python script
        match (av, bv) {
            (HyperListString(x), HyperListInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListString(x), HyperListFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListInt(x), HyperListString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListInt(x), HyperListFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListFloat(x), HyperListString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperListFloat(x), HyperListInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetString(x), HyperSetInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetString(x), HyperSetFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetInt(x), HyperSetString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetInt(x), HyperSetFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetFloat(x), HyperSetString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperSetFloat(x), HyperSetInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringString(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringInt(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapStringFloat(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntString(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntInt(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapIntFloat(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatString(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatInt(x), HyperMapFloatFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapStringString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapStringInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapStringFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapIntString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapIntInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapIntFloat(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapFloatString(y)) => if !sloppyCompare!(x, y) { return false; },
            (HyperMapFloatFloat(x), HyperMapFloatInt(y)) => if !sloppyCompare!(x, y) { return false; },
            (x, y) => if x != y { return false; },
        }
    }
    return true;
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut client = Client::new(FromStr::from_str(format!("{}:{}", args[1], args[2]).as_str()).unwrap()).unwrap();

                match client.put(r"kv", r"k", HyperObject::new()) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!(r"v", Vec::<i64>::new(),);
                match client.get(r"kv", r"k") {
                    Ok(obj) => {
                        if !sloppyCompareHyper(&obj, &expected) {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put(r"kv", r"k", NewHyperObject!(r"v", vec!(1 as i64, 2 as i64, 3 as i64),)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!(r"v", vec!(1 as i64, 2 as i64, 3 as i64),);
                match client.get(r"kv", r"k") {
                    Ok(obj) => {
                        if !sloppyCompareHyper(&obj, &expected) {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            
                match client.put(r"kv", r"k", NewHyperObject!(r"v", Vec::<i64>::new(),)) {
                    Ok(()) => (),
                    Err(err) => panic!(err),
                }
            let expected = NewHyperObject!(r"v", Vec::<i64>::new(),);
                match client.get(r"kv", r"k") {
                    Ok(obj) => {
                        if !sloppyCompareHyper(&obj, &expected) {
                         panic!("expected: {:?}
actual: {:?}", expected, obj);
                        }
                    },
                    Err(err) => panic!(err),
                }
            }
