package hypergo

import (
	"fmt"
	"testing"
)

func TestGetAndPut(t *testing.T) {
	println("BP1")
	client, err := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}

	println("BP2")
	attrs := Attributes{
		"first": "john",
		"last":  "smith",
		"phone": "6075551024",
	}

	err = client.Put("phonebook", "jsmith", attrs)

	println("BP3")
	if err != nil {
		t.Fatal(err)
	}

	obj := client.Get("phonebook", "jsmith")
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	println("BP4")
	fmt.Printf("%v\n", obj)
}
