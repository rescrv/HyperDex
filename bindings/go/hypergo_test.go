package hyperdex

import (
	"testing"

	. "github.com/rescrv/HyperDex/bindings/go/client"
)

const (
	SPACE = "profiles"
	KEY   = "jsmith"
	IP    = "127.0.0.1"
	PORT  = 1982
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

	err := client.Put("profiles", KEY, attrs)

	if err != nil {
		t.Fatal(err)
	}
}

func init() {
	admin, err := NewAdmin(IP, PORT)
	if err != nil {
		panic(err)
	}

	err = admin.RemoveSpace(`profiles`)
	if err != nil {
		if err.Error() != "Error 8777: cannot rm space: does not exist" &&
			err.Error() != "unknown hyperdex_client_returncode: cannot rm space: does not exist" {
			panic(err)
		}
	}

	err = admin.AddSpace(`space profiles
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
`)
	if err != nil {
		panic(err)
	}
}

func TestGetPutDelete(t *testing.T) {
	client, err, _ := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()
	defer func() {
		client.Del("profiles", KEY)
		client.Del("profiles", "derek")
	}()

	putSomething(client, t)

	attr, err5 := client.Get("profiles", KEY)
	if err5 != nil {
		t.Fatal(err5)
	}

	for key, value := range attr {
		t.Logf("Key: %v\n", key)
		t.Logf("Value: %v\n", value)
		// if !((attrs[key] == value) || (attrs[key] == nil)) {
		// 	t.Fatalf("%v != %v", attrs[key], value)
		// }
	}

	err2 := client.Put("profiles", "derek", Attributes{
		"name": "john",
	})

	if err2 != nil {
		t.Fatal(err2)
	}

	objCh, _ := client.Search("profiles", []Predicate{
		{
			"name", "john", EQUALS,
		},
	})

	counter := 0
	for {
		_, ok := <-objCh
		//obj, ok := <-objCh
		//if obj.Err != nil {
		//	t.Fatal(obj.Err)
		//}
		if !ok {
			break
		}
		counter++
	}

	if counter != 2 {
		t.Fatalf("Should return 2 objects.  Instead, we got %d", counter)
	}

	err3 := client.Del("profiles", KEY)

	if err3 != nil {
		t.Fatal(err3)
	}

	//obj, err := client.Get("profiles", KEY)
	_, err4 := client.Get("profiles", KEY)
	if err4 == nil {
		t.Fatal("There should be an error.")
	} else {
		if err4.Status != NOTFOUND {
			t.Fatalf("Return code should be NOT_FOUND; instead we got: %v",
				err)
		}
	}
}

func TestAtomicAddSub(t *testing.T) {
	client, err, _ := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()
	defer func() {
		client.Del("profiles", KEY)
	}()

	putSomething(client, t)
	obj, err7 := client.Get("profiles", KEY)
	if err7 != nil {
		t.Fatal(err7)
	}

	original := obj["height"].(float64)
	delta := 10.2

	err2 := client.AtomicAdd("profiles", KEY, Attributes{
		"height": delta,
	})
	if err2 != nil {
		t.Fatal(err2)
	}

	obj, err4 := client.Get("profiles", KEY)

	if err4 != nil {
		t.Fatal(err4)
	}

	if obj["height"] != original+delta {
		t.Fatal("Atomic add failed.")
	}

	err5 := client.AtomicSub("profiles", KEY, Attributes{
		"height": delta,
	})
	if err5 != nil {
		t.Fatal(err5)
	}

	obj, err6 := client.Get("profiles", KEY)

	if err6 != nil {
		t.Fatal(err6)
	}

	if obj["height"] != original {
		t.Fatal("Atomic add failed.")
	}
}

func TestCondPut(t *testing.T) {
	client, err, _ := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()
	defer func() {
		client.Del("profiles", KEY)
	}()

	putSomething(client, t)

	// this test failing, why?
	value := 214.15

	err2 := client.CondPut("profiles", KEY, []Predicate{{
		Attr:      "profile_views",
		Value:     124312141241,
		Predicate: EQUALS,
	}},
		Attributes{"height": value},
	)

	if err2.Status == SUCCESS {
		t.Fatalf("There should be a comparison failure")
		//t.Fatalf("The failure code should be C.HYPERDEX_CLIENT_COMFAIL, was: %v", err2)
	}

	obj, err3 := client.Get("profiles", KEY)
	if err3 != nil {
		t.Fatal(err3)
	}

	if obj["height"].(float64) == value {
		t.Fatal("The value shouldn't be set because the conditions did not hold")
	}

}

func TestListOps(t *testing.T) {
	client, err, _ := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()

	putSomething(client, t)

	// Test ListLPush and ListRPush
	obj, err2 := client.Get(SPACE, KEY)
	if err2 != nil {
		t.Fatal(err2)
	}

	oldSize := len(obj["ratings"].(ListFloat))

	floatToInsert := 2141.2142

	err3 := client.ListLpush(SPACE, KEY, Attributes{
		"ratings": floatToInsert,
	})
	if err3 != nil {
		t.Fatal(err3)
	}

	err4 := client.ListRpush(SPACE, KEY, Attributes{
		"ratings": floatToInsert,
	})
	if err4 != nil {
		t.Fatal(err4)
	}

	obj, err5 := client.Get(SPACE, KEY)
	if err5 != nil {
		t.Fatal(err5)
	}

	ratings := obj["ratings"].(ListFloat)
	newSize := len(ratings)
	if newSize != oldSize+2 {
		t.Fatal("New size should be old size + 2")
	}

	if ratings[0] != floatToInsert || ratings[len(ratings)-1] != floatToInsert {
		t.Fatal("The first and the last elements should both be floatToInsert.")
	}
}

func TestSetOps(t *testing.T) {
	client, err, _ := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()
	defer func() {
		client.Del("profiles", KEY)
	}()

	putSomething(client, t)

	// Test SetAdd
	obj, err2 := client.Get(SPACE, KEY)
	if err2 != nil {
		t.Fatal(err2)
	}

	oldSize := len(obj["ages"].(SetInt))

	client.SetAdd(SPACE, KEY, Attributes{
		"ages": 0, // 0 is not in the set
	})

	obj, err3 := client.Get(SPACE, KEY)
	if err3 != nil {
		t.Fatal(err)
	}

	newSize := len(obj["ages"].(SetInt))

	if newSize != oldSize+1 {
		t.Fatal("New size should be old size + 1")
	}

	found := false
	for _, elem := range obj["ages"].(SetInt) {
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

	err4 := client.SetIntersect(SPACE, KEY, Attributes{
		"ages": newSet,
	})
	if err4 != nil {
		t.Fatal(err)
	}

	obj, err5 := client.Get(SPACE, KEY)
	if err5 != nil {
		t.Fatal(err5)
	}

	set := obj["ages"].(SetInt)

	if len(set) != 2 {
		t.Fatal("The length of set should be 2.")
	}

	if !((set[0] == 0 && set[1] == 284) || (set[0] == 284 && set[1] == 0)) {
		t.Fatal("The set should contain 0 and 284.")
	}
}

func TestMapOps(t *testing.T) {
	client, err, _ := NewClient("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()
	defer func() {
		client.Del("profiles", KEY)
	}()

	putSomething(client, t)

	// Test MapAdd
	//obj, err := client.Get(SPACE, KEY)
	_, err2 := client.Get(SPACE, KEY)
	if err2 != nil {
		t.Fatal(err2)
	}

	// oldSize := obj.Attrs["upvotes"].(map[string]int64)
	// println(oldSize)
}
