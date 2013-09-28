package hypergo

import (
	"fmt"
	"log"
	"testing"
)

func TestGetAndPut(t *testing.T) {
	log.Println("BP1")
	client, err := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}

	attrs := Attributes{
		"first": "john",
		"last":  "smith",
		"phone": 6075551024,
	}

	log.Println("BP2")
	err = client.Put("phonebook", "jsmith", attrs)

	if err != nil {
		t.Fatal(err)
	}

	log.Println("BP3")
	obj := client.Get("phonebook", "jsmith")
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	fmt.Printf("%v\n", obj)

	log.Println("BP4")
	err = client.Put("phonebook", "derek", Attributes{
		"first": "john",
		"last":  "derek",
		"phone": 24212141,
	})
	if err != nil {
		t.Fatal(err)
	}

	log.Println("BP5")
	objCh := client.Search("phonebook", []SearchCriterion{
		SearchCriterion{
			"first", "john", EQUALS,
		},
	})

	for {
		obj, ok := <-objCh
		if !ok {
			break
		}

		fmt.Printf("%v\n", obj)
	}

	err = client.Delete("phonebook", "jsmith")

	if err != nil {
		t.Fatal(err)
	}

	log.Println("BP6")
	obj = client.Get("phonebook", "jsmith")
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	fmt.Printf("%v\n", obj)

	log.Println("BP7")
	err = client.AtomicAdd("phonebook", "derek", Attributes{
		"phone": 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	log.Println("BP8")
	obj = client.Get("phonebook", "derek")
	fmt.Printf("%v\n", obj)

	log.Println("BP7")
	err = client.AtomicSub("phonebook", "derek", Attributes{
		"phone": 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	log.Println("BP8")
	obj = client.Get("phonebook", "derek")
	fmt.Printf("%v\n", obj)

	log.Println("BPX")
	client.Destroy()
}
