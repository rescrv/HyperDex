package hypergo

/*
#cgo LDFLAGS: -lhyperdex-admin
#include "hyperdex/admin.h"
*/
import "C"

// Admin APIs
func (admin *Admin) DumpConfig() string {
	return <-admin.AsyncDumpConfig()
}

func (admin *Admin) AsyncDumpConfig() chan string {
	c := make(chan string, CHANNEL_BUFFER_SIZE)
	var C_return_code C.enum_hyperdex_admin_returncode
	var C_string *C.char

	req_id := int64(C.hyperdex_admin_dump_config(admin.ptr, &C_return_code, &C_string))
	if req_id < 0 {
		panic("AsyncDumpConfig failed")
	}

	req := adminRequest{
		id:     req_id,
		status: &C_return_code,
		success: func() {
			c <- C.GoString(C_string)
		},
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
