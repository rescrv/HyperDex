package hypergo

import (
	// "fmt"
	// "log"
	"testing"
)

// The following tests use the following space schema:
/*
space profiles
key username
attributes
    string name,
    float height,
    int profile_views,
    list(string) pending_requests,
    list(float) ratings,
    set(string) hobbies,
    set(int) ages,
    map(string, string) unread_messages,
    map(string, int) upvotes
subspace name
subspace height
subspace profile_views
*/

// Put something for testing
func putSomething(client *Client, t *testing.T) {
	attrs := Attributes{
		"name":          "john",
		"height":        float64(241.12421),
		"profile_views": int64(6075551024),
		"pending_requests": List{
			"haha",
			"hehe",
		},
		"ratings": List{
			1.22141,
			-5235.241,
			92804.14,
		},
		"hobbies": Set{
			"qowue",
			"waoihdwao",
		},
		"ages": Set{
			-41,
			284,
			2304,
		},
		"unread_messages": Map{
			"oahd":      "waohdaw",
			"wapodajwp": "waohdwoqpd",
		},
		"upvotes": Map{
			"wadwa": 10294,
			"aowid": 98571,
		},
	}

	err := client.Put("profiles", "jsmith", attrs)

	if err != nil {
		t.Fatal(err)
	}
}

func TestGetPutDelete(t *testing.T) {
	client, err := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()

	putSomething(client, t)

	obj := client.Get("profiles", "jsmith")
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	for key, value := range obj.Attrs {
		t.Logf("Key: %v\n", key)
		t.Logf("Value: %v\n", value)
		// if !((attrs[key] == value) || (attrs[key] == nil)) {
		// 	t.Fatalf("%v != %v", attrs[key], value)
		// }
	}

	err = client.Put("profiles", "derek", Attributes{
		"name": "john",
	})

	if err != nil {
		t.Fatal(err)
	}

	objCh := client.Search("profiles", []Condition{
		Condition{
			"name", "john", EQUALS,
		},
	})

	counter := 0
	for {
		obj, ok := <-objCh
		if obj.Err != nil {
			t.Fatal(obj.Err)
		}
		if !ok {
			break
		}
		counter++
	}

	if counter != 2 {
		t.Fatalf("Should return 2 objects.  Instead, we got %d", counter)
	}

	err = client.Delete("profiles", "jsmith")

	if err != nil {
		t.Fatal(err)
	}

	obj = client.Get("profiles", "jsmith")
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

func TestAtomicAddSub(t *testing.T) {
	client, err := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()

	putSomething(client, t)
	obj := client.Get("profiles", "jsmith")
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	original := obj.Attrs["height"].(float64)
	delta := 10.2

	err = client.AtomicAdd("profiles", "jsmith", Attributes{
		"height": delta,
	})
	if err != nil {
		t.Fatal(err)
	}

	obj = client.Get("profiles", "jsmith")

	if obj.Err != nil {
		t.Fatal(err)
	}

	if obj.Attrs["height"] != original+delta {
		t.Fatal("Atomic add failed.")
	}

	err = client.AtomicSub("profiles", "jsmith", Attributes{
		"height": delta,
	})
	if err != nil {
		t.Fatal(err)
	}

	obj = client.Get("profiles", "jsmith")

	if obj.Err != nil {
		t.Fatal(err)
	}

	if obj.Attrs["height"] != original {
		t.Fatal("Atomic add failed.")
	}
}

func TestCondPut(t *testing.T) {
	client, err := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()

	putSomething(client, t)

	value := 214.15

	err = client.CondPut("profiles", "jsmith", Attributes{
		"height": value,
	}, []Condition{
		Condition{
			Attr:      "profile_views",
			Value:     124312141241,
			Predicate: LESS_EQUAL,
		},
	})

	if err == nil {
		t.Fatalf("There should be a comparison failure")
	} else {
		hyperErr := err.(HyperError)
		if hyperErr.returnCode != 8451 {
			t.Fatalf("The failure code should be C.HYPERDEX_CLIENT_COMFAIL")
		}
	}

	obj := client.Get("profiles", "jsmith")
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	if obj.Attrs["height"].(float64) == value {
		t.Fatal("The value shouldn't be set because the conditions did not hold")
	}
}
