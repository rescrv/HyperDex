package main

import "fmt"
import "github.com/rescrv/HyperDex/bindings/go/client"

func main() {
	c, e, errChan := client.NewClient("127.0.0.1", 1982)
	// permanent error connecting from the cluster
	if e != nil {
		// handle the error here
	}
	// handle transient errors connecting to the cluster
	go func() {
		for e := range errChan {
			// handle the error here
			fmt.Println("error", e)
		}
	}()
	// do the operations
	err := c.Put("kv", "some key", client.Attributes{"v": "Hello World!"})
	if err != nil {
		// handle the error here
	}
	fmt.Println("put: \"Hello World!\"")
	obj, err := c.Get("kv", "some key")
	if err != nil {
		// handle the error here
	}
	fmt.Println("got:", obj)
}
