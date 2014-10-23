package hyperdex

import "testing"

func TestDumpConfig(t *testing.T) {
	admin, err := NewAdmin("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Destroy()

	println(admin.DumpConfig())
}

func TestValidateSpace(t *testing.T) {
	admin, err := NewAdmin("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Destroy()

	validSpace := `
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
`
	if !(admin.ValidateSpace(validSpace) == true) {
		t.Fatalf("The following space schema is valid, "+
			"but ValidateSpace says otherwise: %s", validSpace)
	}

	// Note that the fourth line starts with "strings", which is invalid
	invalidSpace := `
space profiles
key username
attributes
    strings name,
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
`
	if !(admin.ValidateSpace(invalidSpace) == false) {
		t.Fatalf("The following space schema is invalid, "+
			"but ValidateSpace says otherwise: %s", validSpace)
	}
}

func TestListSpaces(t *testing.T) {
	admin, err := NewAdmin("127.0.0.1", 1982)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Destroy()
	for _, space := range admin.ListSpaces() {
		println(space)
	}
}
