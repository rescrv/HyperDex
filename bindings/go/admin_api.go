package hyperdex

// #cgo LDFLAGS: -lhyperdex-admin
// #include <hyperdex/admin.h>
// #include <hyperdex/client.h>
// #include <hyperdex/datastructures.h>
import "C"

import (
	"fmt"
	"strings"

	"github.com/rescrv/HyperDex/bindings/go/client"
)

// Admin APIs
func (admin *Admin) DumpConfig() string {
	return <-admin.AsyncDumpConfig()
}

func (admin *Admin) AsyncDumpConfig() chan string {
	return admin.AsyncDumpConfigOrListSpaces("dump_config")
}

func (admin *Admin) ListSpaces() []string {
	c := admin.AsyncListSpaces()
	strs := make([]string, 0)
	for {
		s, ok := <-c
		if ok {
			if len(s) > 0 {
				strs = append(strs, s)
			}
		} else {
			break
		}
	}

	return strs
}

func (admin *Admin) AsyncListSpaces() chan string {
	return admin.AsyncDumpConfigOrListSpaces("list_spaces")
}

func (admin *Admin) AsyncDumpConfigOrListSpaces(funcName string) chan string {
	c := make(chan string, CHANNEL_BUFFER_SIZE)
	var C_return_code C.enum_hyperdex_admin_returncode
	var C_string *C.char
	var req_id int64
	var success func()

	switch funcName {
	case "dump_config":
		req_id = int64(C.hyperdex_admin_dump_config(admin.ptr, &C_return_code, &C_string))
		success = func() {
			c <- C.GoString(C_string)
		}
	case "list_spaces":
		req_id = int64(C.hyperdex_admin_list_spaces(admin.ptr, &C_return_code, &C_string))
		success = func() {
			spaces := C.GoString(C_string)
			for _, s := range strings.Split(spaces, "\n") {
				c <- s
			}
			close(c)
		}
	}

	if req_id < 0 {
		panic(fmt.Sprintf("%s failed", funcName))
	}

	req := adminRequest{
		id:      req_id,
		status:  &C_return_code,
		success: success,
	}

	admin.requests = append(admin.requests, req)
	return c
}

func (admin *Admin) ValidateSpace(description string) bool {
	var C_return_code C.enum_hyperdex_admin_returncode

	valid := C.hyperdex_admin_validate_space(admin.ptr,
		C.CString(description), &C_return_code)

	if valid < 0 {
		return false
	} else {
		return true
	}
}

func (admin *Admin) AddSpace(description string) error {
	return <-admin.AsyncAddSpace(description)
}

func (admin *Admin) AsyncAddSpace(description string) ErrorChannel {
	return admin.AsyncAddOrRemoveSpace(description, "add")
}

func (admin *Admin) RemoveSpace(description string) error {
	return <-admin.AsyncRemoveSpace(description)
}

func (admin *Admin) AsyncRemoveSpace(description string) ErrorChannel {
	return admin.AsyncAddOrRemoveSpace(description, "remove")
}

func (admin *Admin) AsyncAddOrRemoveSpace(description string, funcName string) ErrorChannel {
	var C_return_code C.enum_hyperdex_admin_returncode
	errCh := make(chan error, CHANNEL_BUFFER_SIZE)
	var req_id int64
	switch funcName {
	case "add":
		req_id = int64(C.hyperdex_admin_add_space(admin.ptr,
			C.CString(description), &C_return_code))
	case "remove":
		req_id = int64(C.hyperdex_admin_rm_space(admin.ptr,
			C.CString(description), &C_return_code))
	}

	req := adminRequest{
		id:     req_id,
		status: &C_return_code,
		success: func() {
			errCh <- nil
		},
		failure: errChannelFailureCallbackForAdmin(errCh),
	}

	admin.requests = append(admin.requests, req)

	return errCh
}

// Utility functions
func newInternalError(status C.enum_hyperdex_client_returncode, msg string) error {
	return client.Error{Status: client.Status(status), Message: msg}
}

// Convenience function for generating a callback

func newInternalErrorForAdmin(status C.enum_hyperdex_admin_returncode, msg string) error {
	return client.Error{Status: client.Status(status), Message: msg}
}

func errChannelFailureCallbackForAdmin(errCh chan error) func(C.enum_hyperdex_admin_returncode, string) {
	return func(status C.enum_hyperdex_admin_returncode, msg string) {
		errCh <- newInternalErrorForAdmin(status, msg)
		close(errCh)
	}
}
