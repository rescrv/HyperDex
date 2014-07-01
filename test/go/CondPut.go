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
	attrs, err = c.Get("kv", "k")
	if err.Status != client.NOTFOUND {
		fmt.Printf("bad status: %d (should be NOTFOUND)\n", err)
	}
	err = c.Put("kv", "k", client.Attributes{"v": "v1"})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	attrs, err = c.Get("kv", "k")
	if err.Status != client.SUCCESS {
		fmt.Printf("bad status: %d (should be SUCCESS)\n", err.Status)
	}
	if !sloppyEqual(attrs, client.Attributes{"v": "v1"}) {
		fmt.Printf("%s %s\n", attrs, client.Map{"v": "v1"})
		panic("objects not equal")
	}
	err = c.CondPut("kv", "k", []client.Predicate{{"v", "v2", client.EQUALS}}, client.Attributes{"v": "v3"})
	if err.Status != client.CMPFAIL {
		os.Exit(1)
	}
	attrs, err = c.Get("kv", "k")
	if err.Status != client.SUCCESS {
		fmt.Printf("bad status: %d (should be SUCCESS)\n", err.Status)
	}
	if !sloppyEqual(attrs, client.Attributes{"v": "v1"}) {
		fmt.Printf("%s %s\n", attrs, client.Map{"v": "v1"})
		panic("objects not equal")
	}
	err = c.CondPut("kv", "k", []client.Predicate{{"v", "v1", client.EQUALS}}, client.Attributes{"v": "v3"})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	attrs, err = c.Get("kv", "k")
	if err.Status != client.SUCCESS {
		fmt.Printf("bad status: %d (should be SUCCESS)\n", err.Status)
	}
	if !sloppyEqual(attrs, client.Attributes{"v": "v3"}) {
		fmt.Printf("%s %s\n", attrs, client.Map{"v": "v3"})
		panic("objects not equal")
	}
	os.Exit(0)
}
