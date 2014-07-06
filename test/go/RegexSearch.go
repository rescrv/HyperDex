package main

import "fmt"
import "os"
import "reflect"
import "strconv"
import "hyperdex/client"

func sloppyEqual(lhs map[interface{}]interface{}, rhs map[interface{}]interface{}) bool {
	if reflect.DeepEqual(lhs, rhs) || fmt.Sprintln(lhs) == fmt.Sprintln(rhs) {
		return true
	}
	for key, lval := range lhs {
		if rval, ok := rhs[key]; ok {
            lstr := fmt.Sprintln(lval)
            rstr := fmt.Sprintln(rval)
            for i := 0; i < 1000; i++ {
                if lstr != rstr {
                    rstr = fmt.Sprintln(rval)
                }
            }
            lmap, lok := lval.(client.Map)
            rmap, rok := rval.(client.Map)
			if !(lval == rval || reflect.DeepEqual(lval, rval) ||
                (lok && rok && sloppyEqualMap(lmap, rmap)) ||
                lstr == rstr) {
				return false
            }
		} else {
			return false
		}
	}
	for key, rval := range rhs {
		if lval, ok := lhs[key]; ok {
            lstr := fmt.Sprintln(lval)
            rstr := fmt.Sprintln(rval)
            for i := 0; i < 1000; i++ {
                if lstr != rstr {
                    rstr = fmt.Sprintln(rval)
                }
            }
            lmap, lok := lval.(client.Map)
            rmap, rok := rval.(client.Map)
			if !(lval == rval || reflect.DeepEqual(lval, rval) ||
                (lok && rok && sloppyEqualMap(lmap, rmap)) ||
                lstr == rstr) {
				return false
            }
		} else {
			return false
		}
	}
	return true
}

func sloppyEqualMap(lhs client.Map, rhs client.Map) bool {
    lmap := map[interface{}]interface{}{}
    rmap := map[interface{}]interface{}{}
    for k, v := range lhs {
        lmap[k] = v
    }
    for k, v := range rhs {
        rmap[k] = v
    }
    return sloppyEqual(lmap, rmap)
}

func sloppyEqualAttributes(lhs client.Attributes, rhs client.Attributes) bool {
    lmap := map[interface{}]interface{}{}
    rmap := map[interface{}]interface{}{}
    for k, v := range lhs {
        lmap[k] = v
    }
    for k, v := range rhs {
        rmap[k] = v
    }
    return sloppyEqual(lmap, rmap)
}

func main() {
	var attrs client.Attributes
	var objs chan client.Attributes
	var errs chan client.Error
	_ = attrs
	_ = objs
	_ = errs
	var c *client.Client
	var err client.Error
	port, _ := strconv.Atoi(os.Args[2])
	c, er, _ := client.NewClient(os.Args[1], port)
	if er != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = c.Put("kv", "foo/foo/foo", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "foo/foo/bar", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "foo/foo/baz", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "foo/bar/foo", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "foo/bar/bar", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "foo/bar/baz", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "foo/baz/foo", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "foo/baz/bar", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "foo/baz/baz", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "bar/foo/foo", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "bar/foo/bar", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "bar/foo/baz", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "bar/bar/foo", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "bar/bar/bar", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "bar/bar/baz", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "bar/baz/foo", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "bar/baz/bar", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "bar/baz/baz", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "baz/foo/foo", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "baz/foo/bar", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "baz/foo/baz", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "baz/bar/foo", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "baz/bar/bar", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "baz/bar/baz", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "baz/baz/foo", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "baz/baz/bar", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	err = c.Put("kv", "baz/baz/baz", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	objs, errs = c.Search("kv", []client.Predicate{{"k", "^foo", client.REGEX}})
	objs, errs = c.Search("kv", []client.Predicate{{"k", "foo$", client.REGEX}})
	objs, errs = c.Search("kv", []client.Predicate{{"k", "^b.*/foo/.*$", client.REGEX}})
	os.Exit(0)
}
