package hypergo

// User-facing APIs

/*
#cgo LDFLAGS: -lhyperdex-client
#include <netinet/in.h>
#include "hyperdex/client.h"
*/
import "C"

// Destroy closes the connection between the Client and hyperdex. It has to be used on a client that is not used anymore.
//
// For every call to NewClient, there must be a call to Destroy.
func (client *Client) Destroy() {
	close(client.closeChan)
	C.hyperdex_client_destroy(client.ptr)
	//log.Printf("hyperdex_client_destroy(%X)\n", unsafe.Pointer(client.ptr))
}

func (client *Client) AtomicAdd(space, key string, attrs Attributes) error {
	return <-client.AsyncAtomicAdd(space, key, attrs)
}

func (client *Client) AsyncAtomicAdd(space, key string, attrs Attributes) ErrorChannel {
	return client.atomicAddSub(space, key, attrs, true)
}

func (client *Client) AtomicSub(space, key string, attrs Attributes) error {
	return <-client.AsyncAtomicSub(space, key, attrs)
}

func (client *Client) AsyncAtomicSub(space, key string, attrs Attributes) ErrorChannel {
	return client.atomicAddSub(space, key, attrs, false)
}

func (client *Client) Search(space string, sc []Condition) ObjectChannel {
	return client.objOp("search", space, "", sc)
}

func (client *Client) Put(space, key string, attrs Attributes) error {
	return <-client.AsyncPut(space, key, attrs)
}

func (client *Client) AsyncPut(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("put", space, key, attrs, nil)
}

func (client *Client) Get(space, key string) Object {
	return <-client.AsyncGet(space, key)
}

func (client *Client) AsyncGet(space, key string) ObjectChannel {
	return client.objOp("get", space, key, nil)
}

func (client *Client) Delete(space, key string) error {
	return <-client.AsyncDelete(space, key)
}

func (client *Client) AsyncDelete(space, key string) ErrorChannel {
	errCh := make(chan error, CHANNEL_BUFFER_SIZE)
	var status C.enum_hyperdex_client_returncode
	req_id := int64(C.hyperdex_client_del(client.ptr, C.CString(space), C.CString(key), C.size_t(len([]byte(key))), &status))
	//log.Printf("hyperdex_client_del(%X, \"%s\", \"%s\", %d, %X) -> %d", unsafe.Pointer(client.ptr), space, key, len([]byte(key)), unsafe.Pointer(&status), req_id)
	if req_id < 0 {
		errCh <- newInternalError(status)
		close(errCh)
		return errCh
	}
	req := request{
		id:      req_id,
		success: errChannelSuccessCallback(errCh),
		failure: errChannelFailureCallback(errCh),
	}
	client.requests = append(client.requests, req)
	return errCh
}

func (client *Client) atomicAddSub(space, key string, attrs Attributes, isAdd bool) ErrorChannel {
	errCh := make(chan error, CHANNEL_BUFFER_SIZE)
	var status C.enum_hyperdex_client_returncode

	C_attrs, C_attrs_sz, err := newCTypeAttributeList(attrs)
	if err != nil {
		errCh <- err
		close(errCh)
		return errCh
	}

	var req_id int64
	if isAdd {
		req_id = int64(C.hyperdex_client_atomic_add(client.ptr,
			C.CString(space), C.CString(key), C.size_t(len(key)),
			C_attrs, C_attrs_sz, &status))
	} else {
		req_id = int64(C.hyperdex_client_atomic_sub(client.ptr,
			C.CString(space), C.CString(key), C.size_t(len(key)),
			C_attrs, C_attrs_sz, &status))
	}

	if req_id < 0 {
		errCh <- newInternalError(status)
		close(errCh)
		return errCh
	}

	req := request{
		id:      req_id,
		success: errChannelSuccessCallback(errCh),
		failure: errChannelFailureCallback(errCh),
	}

	client.requests = append(client.requests, req)
	return errCh
}
