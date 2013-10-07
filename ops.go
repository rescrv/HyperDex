package hypergo

// Functions that APIs make use of

/*
#cgo LDFLAGS: -lhyperdex-client
#include <netinet/in.h>
#include "hyperdex/client.h"
*/
import "C"

import (
	"fmt"
)

// Generic operation that returns objects
func (client *Client) objOp(funcName string, space string, key string, conds []Condition) ObjectChannel {
	objCh := make(chan Object, CHANNEL_BUFFER_SIZE)
	var status C.enum_hyperdex_client_returncode
	var C_attrs *C.struct_hyperdex_client_attribute
	var C_attrs_sz C.size_t
	var C_attr_checks *C.struct_hyperdex_client_attribute_check
	var C_attr_checks_sz C.size_t
	var err error

	if conds != nil {
		C_attr_checks, C_attr_checks_sz, err = newCAttributeCheckList(conds)
		if err != nil {
			objCh <- Object{
				Err: err,
			}
			close(objCh)
			return objCh
		}
	}

	var req_id int64
	switch funcName {
	case "get":
		req_id = int64(C.hyperdex_client_get(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			&status, &C_attrs, &C_attrs_sz))
	case "search":
		req_id = int64(C.hyperdex_client_search(client.ptr,
			C.CString(space), C_attr_checks, C_attr_checks_sz,
			&status, &C_attrs, &C_attrs_sz))
	}

	if req_id < 0 {
		objCh <- Object{Err: newInternalError(status,
			C.GoString(C.hyperdex_client_error_message(client.ptr)))}
		close(objCh)
		return objCh
	}

	req := request{
		id:     req_id,
		status: &status,
	}
	switch funcName {
	case "get":
		req.success = func() {
			attrs, err := newAttributeListFromC(C_attrs, C_attrs_sz)
			if err != nil {
				objCh <- Object{Err: err}
				close(objCh)
				return
			}
			objCh <- Object{Err: nil, Key: key, Attrs: attrs}
			close(objCh)
			if C_attrs_sz > 0 {
				C.hyperdex_client_destroy_attrs(C_attrs, C_attrs_sz)
			}
		}
		req.failure = objChannelFailureCallback(objCh)
	case "search":
		req.isIterator = true
		req.success = func() {
			// attrs, err := newAttributeListFromC(C_attrs, C_attrs_sz)
			attrs, err := newAttributeListFromC(C_attrs, C_attrs_sz)
			if err != nil {
				objCh <- Object{Err: err}
				close(objCh)
				return
			}

			if C_attrs_sz > 0 {
				C.hyperdex_client_destroy_attrs(C_attrs, C_attrs_sz)
			}
			objCh <- Object{Attrs: attrs}
		}
		req.failure = objChannelFailureCallback(objCh)
		req.complete = func() {
			close(objCh)
		}
	}

	client.requests = append(client.requests, req)
	return objCh
}

// Generic operation that returns errors
func (client *Client) errOp(funcName string, space string, key string, attrs Attributes, conds []Condition) ErrorChannel {
	errCh := make(chan error, CHANNEL_BUFFER_SIZE)
	var status C.enum_hyperdex_client_returncode
	var C_attrs *C.struct_hyperdex_client_attribute
	var C_attrs_sz C.size_t
	var C_attr_checks *C.struct_hyperdex_client_attribute_check
	var C_attr_checks_sz C.size_t
	var err error

	if conds != nil {
		C_attr_checks, C_attr_checks_sz, err = newCAttributeCheckList(conds)
		if err != nil {
			errCh <- err
			close(errCh)
			return errCh
		}
	}

	if attrs != nil {
		C_attrs, C_attrs_sz, err = newCAttributeList(attrs)
		if err != nil {
			errCh <- err
			close(errCh)
			return errCh
		}
	}

	var req_id int64
	switch funcName {
	case "put":
		req_id = int64(C.hyperdex_client_put(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_put":
		req_id = int64(C.hyperdex_client_cond_put(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "put_if_not_exist":
		req_id = int64(C.hyperdex_client_put_if_not_exist(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "del":
		req_id = int64(C.hyperdex_client_del(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)), &status))
	case "cond_del":
		req_id = int64(C.hyperdex_client_cond_del(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz, &status))
	case "atomic_add":
		req_id = int64(C.hyperdex_client_atomic_add(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_atomic_add":
		req_id = int64(C.hyperdex_client_cond_atomic_add(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "atomic_sub":
		req_id = int64(C.hyperdex_client_atomic_sub(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_atomic_sub":
		req_id = int64(C.hyperdex_client_cond_atomic_sub(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "atomic_mul":
		req_id = int64(C.hyperdex_client_atomic_mul(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_atomic_mul":
		req_id = int64(C.hyperdex_client_cond_atomic_mul(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "atomic_div":
		req_id = int64(C.hyperdex_client_atomic_div(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_atomic_div":
		req_id = int64(C.hyperdex_client_cond_atomic_div(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "atomic_mod":
		req_id = int64(C.hyperdex_client_atomic_mod(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_atomic_mod":
		req_id = int64(C.hyperdex_client_cond_atomic_mod(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "atomic_and":
		req_id = int64(C.hyperdex_client_atomic_and(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_atomic_and":
		req_id = int64(C.hyperdex_client_cond_atomic_and(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "atomic_or":
		req_id = int64(C.hyperdex_client_atomic_or(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_atomic_or":
		req_id = int64(C.hyperdex_client_cond_atomic_or(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "atomic_xor":
		req_id = int64(C.hyperdex_client_atomic_xor(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_atomic_xor":
		req_id = int64(C.hyperdex_client_cond_atomic_xor(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "string_prepend":
		req_id = int64(C.hyperdex_client_string_prepend(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_string_prepend":
		req_id = int64(C.hyperdex_client_cond_string_prepend(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "string_append":
		req_id = int64(C.hyperdex_client_string_append(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_string_append":
		req_id = int64(C.hyperdex_client_cond_string_append(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "list_lpush":
		req_id = int64(C.hyperdex_client_list_lpush(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_list_lpush":
		req_id = int64(C.hyperdex_client_cond_list_lpush(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "list_rpush":
		req_id = int64(C.hyperdex_client_list_rpush(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_list_rpush":
		req_id = int64(C.hyperdex_client_cond_list_rpush(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "set_add":
		req_id = int64(C.hyperdex_client_set_add(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_set_add":
		req_id = int64(C.hyperdex_client_cond_set_add(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "set_remove":
		req_id = int64(C.hyperdex_client_set_remove(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_set_remove":
		req_id = int64(C.hyperdex_client_cond_set_remove(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "set_intersect":
		req_id = int64(C.hyperdex_client_set_intersect(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_set_intersect":
		req_id = int64(C.hyperdex_client_cond_set_intersect(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	case "set_union":
		req_id = int64(C.hyperdex_client_set_union(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attrs, C_attrs_sz, &status))
	case "cond_set_union":
		req_id = int64(C.hyperdex_client_cond_set_union(client.ptr, C.CString(space),
			C.CString(key), C.size_t(bytesOf(key)),
			C_attr_checks, C_attr_checks_sz,
			C_attrs, C_attrs_sz, &status))
	default:
		panic(fmt.Sprintf("Unsupported function: %s", funcName))
	}

	if req_id < 0 {
		errCh <- newInternalError(status,
			C.GoString(C.hyperdex_client_error_message(client.ptr)))
		close(errCh)
		return errCh
	}

	req := request{
		id:     req_id,
		status: &status,
		success: func() {
			errCh <- nil
			close(errCh)
		},
		failure: errChannelFailureCallback(errCh),
		complete: func() {
			// TODO: The invocation of hyperdex_client_destroy_attrs causes
			// memory error.  But not destroying the C struct list might
			// cause potential memory leak.

			// println("put complete callback")
			// C_attrs := *req.bundle["C_attrs"].(**C.struct_hyperdex_client_attribute)
			// C_attrs_sz := *req.bundle["C_attrs_sz"].(*C.size_t)
			// if C_attrs_sz > 0 {
			// 	println("BP8")
			// 	C.hyperdex_client_destroy_attrs(C_attrs, C_attrs_sz)
			// 	println("BP9")
			// 	//log.Printf("hyperdex_client_destroy_attrs(%X, %d)\n", unsafe.Pointer(C_attrs), C_attrs_sz)
			// }
		},
	}

	client.requests = append(client.requests, req)
	return errCh
}
