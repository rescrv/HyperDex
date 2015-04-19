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
	err = c.Put("kv", "k", client.Attributes{})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	attrs, err = c.Get("kv", "k")
	if err.Status != client.SUCCESS {
		fmt.Printf("bad status: %d (should be SUCCESS)\n", err.Status)
	}
	if !sloppyEqualAttributes(attrs, client.Attributes{"v": ""}) {
		fmt.Printf("%s %s\n", attrs, client.Map{"v": ""})
		panic("objects not equal")
	}
	err = c.Put("kv", "k", client.Attributes{"v": "xxx"})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	attrs, err = c.Get("kv", "k")
	if err.Status != client.SUCCESS {
		fmt.Printf("bad status: %d (should be SUCCESS)\n", err.Status)
	}
	if !sloppyEqualAttributes(attrs, client.Attributes{"v": "xxx"}) {
		fmt.Printf("%s %s\n", attrs, client.Map{"v": "xxx"})
		panic("objects not equal")
	}
	err = c.Put("kv", "k", client.Attributes{"v": "\xde\xad\xbe\xef"})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	attrs, err = c.Get("kv", "k")
	if err.Status != client.SUCCESS {
		fmt.Printf("bad status: %d (should be SUCCESS)\n", err.Status)
	}
	if !sloppyEqualAttributes(attrs, client.Attributes{"v": "\xde\xad\xbe\xef"}) {
		fmt.Printf("%s %s\n", attrs, client.Map{"v": "\xde\xad\xbe\xef"})
		panic("objects not equal")
	}
	os.Exit(0)
}
