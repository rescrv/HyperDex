package hypergo

import (
	"testing"
	"unsafe"
)

func TestGetAndPut(t *testing.T) {
	println(unsafe.Sizeof("abcdefg"))
	println(len([]byte("abcdefg")))
	println(bytesOf("abcdefg"))
	println("BP1")
	client, err := NewClient("127.0.0.1", 2012)
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

	obj := client.Get("phone", "jsmith")
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	println("BP4")
	t.Logf("%v\n", obj)
}
