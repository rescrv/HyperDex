package hypergo

import (
	// "fmt"
	// "log"
	"testing"
)

func TestGetPutDelete(t *testing.T) {
	client, err := NewClient("127.0.0.1", 1982)
	defer client.Destroy()

	if err != nil {
		t.Fatal(err)
	}

	attrs := Attributes{
		"first":     "john",
		"last":      "smith",
		"phone":     int64(6075551024),
		"floatAttr": float64(241.12421),
	}

	err = client.Put("phonebook", "jsmith", attrs)

	if err != nil {
		t.Fatal(err)
	}

	obj := client.Get("phonebook", "jsmith")
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	for key, value := range obj.Attrs {
		if attrs[key] != value {
			t.Fatalf("%v != %v", attrs[key], value)
		}
	}

	err = client.Put("phonebook", "derek", Attributes{
		"first":     "john",
		"last":      "derek",
		"phone":     24212141,
		"floatAttr": 32.43141,
	})

	if err != nil {
		t.Fatal(err)
	}

	objCh := client.Search("phonebook", []Condition{
		Condition{
			"first", "john", EQUALS,
		},
	})

	counter := 0
	for {
		_, ok := <-objCh
		if !ok {
			break
		}
		counter++
	}

	if counter != 2 {
		t.Fatalf("Should return 2 objects.  Instead, we got %d", counter)
	}

	err = client.Delete("phonebook", "jsmith")

	if err != nil {
		t.Fatal(err)
	}

	obj = client.Get("phonebook", "jsmith")
	if obj.Err != nil {
		hyperErr := obj.Err.(HyperError)
		if hyperErr.returnCode != 8449 {
			t.Fatal("Return code should be NOT_FOUND; instead we got: %d",
				hyperErr.returnCode)
		}
	} else {
		t.Fatal("There should be an error.")
	}
}
