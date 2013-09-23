package hypergo

import (
	"testing"
)

func TestGetAndPut(t *testing.T) {
	client, err := NewClient("127.0.0.1", 2012)
	if err != nil {
		t.Fatal(err)
	}

	attrs := map[string]interface{}{
		"first": "john",
		"last":  "smith",
		"phone": "6075551024",
	}

	// attrs["first"] = "john"
	// attrs["last"] = "smith"
	// attrs["phone"] = "6075551024"
	err = client.Put("phonebook", "jsmith", attrs)

	if err != nil {
		t.Fatal(err)
	}

	obj := client.Get("phone", "jsmith")
	if obj.Err != nil {
		t.Fatal(obj.Err)
	}

	t.Logf("%v\n", obj)
}
