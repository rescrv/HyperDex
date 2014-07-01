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
	panic("MISSING SEARCH XXX")
	err = c.Put("kv", "k1", client.Attributes{"v": "v1"})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	panic("MISSING SEARCH XXX")
	err = c.Put("kv", "k2", client.Attributes{"v": "v1"})
	if err.Status != client.SUCCESS {
		os.Exit(1)
	}
	panic("MISSING SEARCH XXX")
	os.Exit(0)
}
