package main

import "fmt"
import "os"
import "reflect"
import "strconv"
import "hyperdex/client"

func sloppyEqual(lhs client.Attributes, rhs client.Attributes) bool {
	if reflect.DeepEqual(lhs, rhs) || fmt.Sprintf("%s", lhs) == fmt.Sprintf("%s", rhs) {
		return true
	}
	for key, val := range lhs {
		if _, ok := rhs[key]; ok {
			if rhs[key] != val {
				return false
			}
			if !reflect.DeepEqual(rhs[key], val) {
				return false
			}
			if fmt.Sprintf("%s", rhs[key]) != fmt.Sprintf("%s", val) {
				return false
			}
		}
	}
	for key, val := range rhs {
		if _, ok := lhs[key]; ok {
			if lhs[key] != val {
				return false
			}
			if !reflect.DeepEqual(lhs[key], val) {
				return false
			}
			if fmt.Sprintf("%s", lhs[key]) != fmt.Sprintf("%s", val) {
				return false
			}
		}
	}
	return false
}

func main() {
	var attrs client.Attributes
	var c *client.Client
	var err client.Error
	port, _ := strconv.Atoi(os.Args[2])
	c, er := client.NewClient(os.Args[1], port)
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
	panic("MISSING SEARCH XXX")
	panic("MISSING SEARCH XXX")
	panic("MISSING SEARCH XXX")
	os.Exit(0)
}
