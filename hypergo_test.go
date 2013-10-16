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

const (
	SPACE = "profiles"
	KEY   = "jsmith"
)

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

func TestListOps(t *testing.T) {
	client, err := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()

	putSomething(client, t)

	// Test ListLPush and ListRPush
	obj := client.Get(SPACE, KEY)
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	oldSize := len(obj.Attrs["ratings"].(ListF64))

	floatToInsert := 2141.2142

	err = client.ListLPush(SPACE, KEY, Attributes{
		"ratings": floatToInsert,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = client.ListRPush(SPACE, KEY, Attributes{
		"ratings": floatToInsert,
	})
	if err != nil {
		t.Fatal(err)
	}

	obj = client.Get(SPACE, KEY)
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	ratings := obj.Attrs["ratings"].(ListF64)
	newSize := len(ratings)
	if newSize != oldSize+2 {
		t.Fatal("New size should be old size + 2")
	}

	if ratings[0] != floatToInsert || ratings[len(ratings)-1] != floatToInsert {
		t.Fatal("The first and the last elements should both be floatToInsert.")
	}
}

func TestSetOps(t *testing.T) {
	client, err := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()

	putSomething(client, t)

	// Test SetAdd
	obj := client.Get(SPACE, KEY)
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	oldSize := len(obj.Attrs["ages"].(SetI64))

	client.SetAdd(SPACE, KEY, Attributes{
		"ages": 0, // 0 is not in the set
	})

	obj = client.Get(SPACE, KEY)
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	newSize := len(obj.Attrs["ages"].(SetI64))

	if newSize != oldSize+1 {
		t.Fatal("New size should be old size + 1")
	}

	found := false
	for _, elem := range obj.Attrs["ages"].(SetI64) {
		if elem == 0 {
			found = true
		}
	}

	if !found {
		t.Fatal("0 should be in the set.")
	}

	// Test SetIntersect
	newSet := Set{
		0, 284, // these two elements are in the original set
		41, 532, // these two are not
	}

	err = client.SetIntersect(SPACE, KEY, Attributes{
		"ages": newSet,
	})
	if err != nil {
		t.Fatal(err)
	}

	obj = client.Get(SPACE, KEY)
	set := obj.Attrs["ages"].(SetI64)

	if len(set) != 2 {
		t.Fatal("The length of set should be 2.")
	}

	if !((set[0] == 0 && set[1] == 284) || (set[0] == 284 && set[1] == 0)) {
		t.Fatal("The set should contain 0 and 284.")
	}
}

func TestMapOps(t *testing.T) {
	client, err := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()

	putSomething(client, t)

	// Test MapAdd
	obj := client.Get(SPACE, KEY)
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	// oldSize := obj.Attrs["upvotes"].(map[string]int64)
	// println(oldSize)
}
