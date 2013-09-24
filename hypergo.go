package hypergo

/*
#cgo LDFLAGS: -lhyperdex-client
#include <netinet/in.h>
#include "hyperdex/client.h"

struct hyperdex_client_attribute* GetAttribute(struct hyperdex_client_attribute* list, int i) {
	return &list[i];
}
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	//"log"
	"runtime"
	"unsafe" // used only for C.GoBytes
)

// CHANNEL_BUFFER_SIZE is the size of all the returned channels' buffer.
// You can set it to 0 if you want unbuffered channels.
var CHANNEL_BUFFER_SIZE = 1

// Timeout in miliseconds.
// Negative timeout means no timeout.
var TIMEOUT = -1

// Datatypes
const (
	datatype_STRING  = 9217
	datatype_INT64   = 9218
	datatype_GARBAGE = 9727
)

// Return codes
const (
	returncode_SUCCESS    = 8448
	returncode_NOTFOUND   = 8449
	returncode_SEARCHDONE = 8450
	returncode_CMPFAIL    = 8451
	returncode_READONLY   = 8452

	// Error conditions
	returncode_UNKNOWNSPACE = 8512
	returncode_COORDFAIL    = 8513
	returncode_SERVERERROR  = 8514
	returncode_POLLFAILED   = 8515
	returncode_OVERFLOW     = 8516
	returncode_RECONFIGURE  = 8517
	returncode_LOGICERROR   = 8518
	returncode_TIMEOUT      = 8519
	returncode_UNKNOWNATTR  = 8520
	returncode_DUPEATTR     = 8521
	returncode_NONEPENDING  = 8523
	returncode_DONTUSEKEY   = 8524
	returncode_WRONGTYPE    = 8525
	returncode_NOMEM        = 8526
	returncode_BADCONFIG    = 8527
	returncode_DUPLICATE    = 8529
	returncode_INTERRUPTED  = 8530
	returncode_CLUSTER_JUMP = 8531
	returncode_COORD_LOGGED = 8532
	returncode_OFFLINE      = 8533

	// The following return codes indicate bugs in Hyperdex
	returncode_INTERNAL  = 8573
	returncode_EXCEPTION = 8574
	returncode_GARBAGE   = 8575
)

var internalErrorMessages map[int64]string = map[int64]string{
	returncode_SUCCESS:      "Success",
	returncode_NOTFOUND:     "Not Found",
	returncode_SEARCHDONE:   "Search Done",
	returncode_CMPFAIL:      "Conditional Operation Did Not Match Object",
	returncode_UNKNOWNSPACE: "Unknown Space",
	returncode_COORDFAIL:    "Coordinator Failure",
	returncode_SERVERERROR:  "Server Error",
	// returncode_CONNECTFAIL:  "Connection Failure",
	// returncode_DISCONNECT:   "Connection Reset",
	returncode_RECONFIGURE: "Reconfiguration",
	returncode_LOGICERROR:  "Logic Error (file a bug)",
	returncode_TIMEOUT:     "Timeout",
	returncode_UNKNOWNATTR: "Unknown attribute '%s'",
	returncode_DUPEATTR:    "Duplicate attribute '%s'",
	// returncode_SEEERRNO:    "See ERRNO",
	returncode_NONEPENDING: "None pending",
	returncode_DONTUSEKEY:  "Do not specify the key in a search predicate and do not redundantly specify the key for an insert",
	returncode_WRONGTYPE:   "Attribute '%s' has the wrong type",
	returncode_EXCEPTION:   "Internal Error (file a bug)",
}

// Client is the hyperdex client used to make requests to hyperdex.
type Client struct {
	ptr       *C.struct_hyperdex_client
	requests  []request
	closeChan chan struct{}
}

// Attributes represents a map of key-value attribute pairs.
//
// The value must be either a string or an int64-compatible integer
// (int, int8, int16, int32, int64, uint8, uint16, uint32).
// An incompatible type will NOT result in a panic but in a regular error return.
//
// Please note that there is no support for uint64 since its negative might be incorrectly evaluated.
// Support for uint has been dropped because it is unspecified whether it is 32 or 64 bits.
type Attributes map[string]interface{}

type Object struct {
	Err   error
	Key   string
	Attrs Attributes
}

type ObjectChannel <-chan Object
type ErrorChannel <-chan error

type bundle map[string]interface{}

type request struct {
	id       int64
	success  func()
	failure  func(C.enum_hyperdex_client_returncode)
	complete func()
}

// Return the number of bytes of a string
func bytesOf(str string) int {
	return len([]byte(str))
}

// NewClient initializes a hyperdex client ready to use.
//
// For every call to NewClient, there must be a call to Destroy.
//
// Panics when the internal looping goroutine receives an error from hyperdex.
//
// Example:
// 		client, err := hyperdex_client.NewClient("127.0.0.1", 1234)
// 		if err != nil {
//			//handle error
//		}
//		defer client.Destroy()
//		// use client
func NewClient(ip string, port int) (*Client, error) {
	C_client := C.hyperdex_client_create(C.CString(ip), C.uint16_t(port))
	//log.Printf("hyperdex_client_create(\"%s\", %d) -> %X\n", ip, port, unsafe.Pointer(C_client))
	if C_client == nil {
		return nil, fmt.Errorf("Could not create hyperdex_client (ip=%s, port=%d)", ip, port)
	}
	client := &Client{
		C_client,
		make([]request, 0, 8), // No reallocation within 8 concurrent requests to hyperdex_client_loop
		make(chan struct{}, 1),
	}

	go func() {
		for {
			select {
			// quit goroutine when client is destroyed
			case <-client.closeChan:
				return
			default:
				// check if there are pending requests
				// and only if there are, call hyperdex_client_loop
				if l := len(client.requests); l > 0 {
					println("processing request!")
					var status C.enum_hyperdex_client_returncode = 42
					ret := int64(C.hyperdex_client_loop(client.ptr, C.int(TIMEOUT), &status))
					//log.Printf("hyperdex_client_loop(%X, %d, %X) -> %d\n", unsafe.Pointer(client.ptr), hyperdex_client_loop_timeout, unsafe.Pointer(&status), ret)
					if ret < 0 {
						panic(newInternalError(status).Error())
					}
					// find processed request among pending requests
					for i, req := range client.requests {
						if req.id == ret {
							if status == returncode_SUCCESS {
								req.success()
							} else {
								req.failure(status)
							}
							if req.complete != nil {
								req.complete()
							}
							// remove processed request from pending requests
							client.requests = append(client.requests[:i], client.requests[i+1:]...)
							break
						}
					}
				}
				// prevent other goroutines from starving
				runtime.Gosched()
			}
		}
		panic("Should not be reached: end of infinite loop")
	}()

	return client, nil
}

// Destroy closes the connection between the Client and hyperdex. It has to be used on a client that is not used anymore.
//
// For every call to NewClient, there must be a call to Destroy.
func (client *Client) Destroy() {
	close(client.closeChan)
	C.hyperdex_client_destroy(client.ptr)
	//log.Printf("hyperdex_client_destroy(%X)\n", unsafe.Pointer(client.ptr))
}

func (client *Client) AtomicInc(space, key string, attrs Attributes) ErrorChannel {
	return client.atomicIncDec(space, key, attrs, false)
}

func (client *Client) AtomicDec(space, key string, attrs Attributes) ErrorChannel {
	return client.atomicIncDec(space, key, attrs, true)
}

func (client *Client) Put(space, key string, attrs Attributes) error {
	return <-client.AsyncPut(space, key, attrs)
}

func (client *Client) AsyncPut(space, key string, attrs Attributes) ErrorChannel {
	errCh := make(chan error, CHANNEL_BUFFER_SIZE)
	var status C.enum_hyperdex_client_returncode

	C_attrs, C_attrs_sz, err := newCTypeAttributeList(attrs, false)
	if err != nil {
		errCh <- err
		close(errCh)
		return errCh
	}

	println("BP5")

	fmt.Printf("%v\n", client.ptr)
	fmt.Printf("%v\n", space)
	fmt.Printf("%v\n", key)
	fmt.Printf("%v\n", bytesOf(key))
	fmt.Printf("%v\n", attrs)
	fmt.Printf("%v\n", C_attrs)
	fmt.Printf("%v\n", C_attrs_sz)
	fmt.Printf("%v\n", status)

	req_id := int64(C.hyperdex_client_put(client.ptr, C.CString(space),
		C.CString(key), C.size_t(bytesOf(key)),
		C_attrs, C_attrs_sz,
		&status))

	println("BP6")
	if req_id < 0 {
		errCh <- newInternalError(status)
		close(errCh)
		return errCh
	}

	req := request{
		id: req_id,
		success: func() {
			errCh <- nil
			close(errCh)
		},
		failure: errChannelFailureCallback(errCh),
		complete: func() {
			C.hyperdex_client_destroy_attrs(C_attrs, C_attrs_sz)
		},
	}

	client.requests = append(client.requests, req)
	return errCh
}

func (client *Client) Get(space, key string) Object {
	return <-client.AsyncGet(space, key)
}

func (client *Client) AsyncGet(space, key string) ObjectChannel {
	objCh := make(chan Object, CHANNEL_BUFFER_SIZE)
	var status C.enum_hyperdex_client_returncode
	var C_attrs *C.struct_hyperdex_client_attribute
	var C_attrs_sz C.size_t

	req_id := int64(C.hyperdex_client_get(client.ptr, C.CString(space), C.CString(key), C.size_t(bytesOf(key)), &status, &C_attrs, &C_attrs_sz))
	//log.Printf("hyperdex_client_get(%X, \"%s\", \"%s\", %d, %X, %X, %X) -> %d\n", unsafe.Pointer(client.ptr), space, key, len([]byte(key)), unsafe.Pointer(&status), unsafe.Pointer(&C_attrs), unsafe.Pointer(&C_attrs_sz), req_id)
	if req_id < 0 {
		objCh <- Object{Err: newInternalError(status)}
		close(objCh)
		return objCh
	}
	req := request{
		id: req_id,
		success: func() {
			attrs, err := newAttributeListFromC(C_attrs, C_attrs_sz)
			if err != nil {
				objCh <- Object{Err: err}
				close(objCh)
				return
			}
			objCh <- Object{Err: nil, Key: key, Attrs: attrs}
			close(objCh)
		},
		failure: objChannelFailureCallback(objCh),
		complete: func() {
			if C_attrs_sz > 0 {
				C.hyperdex_client_destroy_attrs(C_attrs, C_attrs_sz)
				//log.Printf("hyperdex_client_destroy_attrs(%X, %d)\n", unsafe.Pointer(C_attrs), C_attrs_sz)
			}
		},
	}
	client.requests = append(client.requests, req)
	return objCh
}

func (client *Client) Delete(space, key string) ErrorChannel {
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
		failure: errChannelFailureCallback(errCh),
	}
	client.requests = append(client.requests, req)
	return errCh
}

func (client *Client) atomicIncDec(space, key string, attrs Attributes, negative bool) ErrorChannel {
	errCh := make(chan error, CHANNEL_BUFFER_SIZE)
	var status C.enum_hyperdex_client_returncode
	C_attrs, C_attrs_sz, err := newCTypeAttributeList(attrs, negative)
	if err != nil {
		errCh <- err
		close(errCh)
		return errCh
	}
	req_id := int64(C.hyperdex_client_atomic_add(client.ptr, C.CString(space), C.CString(key), C.size_t(len(key)), C_attrs, C_attrs_sz, &status))
	if req_id < 0 {
		errCh <- newInternalError(status)
		close(errCh)
		return errCh
	}
	req := request{
		id:      req_id,
		failure: errChannelFailureCallback(errCh),
	}
	client.requests = append(client.requests, req)
	return errCh
}

func newInternalError(status C.enum_hyperdex_client_returncode, a ...interface{}) error {
	s, ok := internalErrorMessages[int64(status)]
	if ok {
		return fmt.Errorf(s, a)
	}
	return fmt.Errorf("Unknown Error %d (file a bug)", status)
}

// Convenience function for generating a callback
func errChannelFailureCallback(errCh chan error) func(C.enum_hyperdex_client_returncode) {
	return func(status C.enum_hyperdex_client_returncode) {
		errCh <- newInternalError(status)
		close(errCh)
	}
}

// Convenience function for generating a callback
func objChannelFailureCallback(objCh chan Object) func(C.enum_hyperdex_client_returncode) {
	return func(status C.enum_hyperdex_client_returncode) {
		objCh <- Object{Err: newInternalError(status)}
		close(objCh)
	}
}

func newCTypeAttributeList(attrs Attributes, negative bool) (C_attrs *C.struct_hyperdex_client_attribute, C_attrs_sz C.size_t, err error) {
	slice := make([]C.struct_hyperdex_client_attribute, 0, len(attrs))
	for key, value := range attrs {
		attr, err := newCTypeAttribute(key, value, negative)
		if err != nil {
			return nil, 0, err
		}
		slice = append(slice, *attr)
		C_attrs_sz += 1
	}
	if C_attrs_sz == 0 {
		return nil, C_attrs_sz, nil
	}
	return &slice[0], C_attrs_sz, nil
}

func newCTypeAttribute(key string, value interface{}, negative bool) (*C.struct_hyperdex_client_attribute, error) {
	var val string
	var datatype C.enum_hyperdex_client_returncode
	size := 0

	switch v := value.(type) {
	case string:
		val = v
		datatype = datatype_STRING
		size = len([]byte(val))
	case int, int8, int16, int32, int64, uint8, uint16, uint32:
		var i int64
		// Converting all int64-compatible integers to int64
		switch v := v.(type) {
		case int:
			i = int64(v)
		case int8:
			i = int64(v)
		case int16:
			i = int64(v)
		case int32:
			i = int64(v)
		case int64:
			i = v
		case uint8:
			i = int64(v)
		case uint16:
			i = int64(v)
		case uint32:
			i = int64(v)
		default:
			panic("Should not be reached: normalizing integers to int64")
		}

		if negative {
			i = -i
		}
		// Binary encoding
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, i)
		if err != nil {
			return nil, fmt.Errorf("Could not convert integer '%d' to bytes", i)
		}
		val = buf.String()
		datatype = datatype_INT64
		size = binary.Size(int64(0))
	default:
		return nil, fmt.Errorf("Attribute with key '%s' has unsupported type '%T'", key, v)
	}

	fmt.Printf("Key: %v\n", key)
	fmt.Printf("Val: %v\n", val)
	fmt.Printf("Size: %v\n", size)
	fmt.Printf("Datatype: %v\n", datatype)

	return &C.struct_hyperdex_client_attribute{
		C.CString(key),
		C.CString(val),
		C.size_t(size),
		C.enum_hyperdatatype(datatype),
		[4]byte{}, // alignment
	}, nil
}

func newAttributeListFromC(C_attrs *C.struct_hyperdex_client_attribute, C_attrs_sz C.size_t) (attrs Attributes, err error) {
	attrs = Attributes{}
	for i := 0; i < int(C_attrs_sz); i++ {
		C_attr := C.GetAttribute(C_attrs, C.int(i))
		attr := C.GoString(C_attr.attr)
		switch C_attr.datatype {
		case datatype_STRING:
			attrs[attr] = C.GoStringN(C_attr.value, C.int(C_attr.value_sz))
		case datatype_INT64:
			var value int64
			buf := bytes.NewBuffer(C.GoBytes(unsafe.Pointer(C_attr.value), C.int(C_attr.value_sz)))
			err := binary.Read(buf, binary.LittleEndian, &value)
			if err != nil {
				return nil, fmt.Errorf("Could not decode INT64 attribute `%s` (#%d)", attr, i)
			}
			attrs[attr] = value
		case datatype_GARBAGE:
			continue
		default:
			return nil, fmt.Errorf("Unknown datatype %d found for attribute `%s` (#%d)", C_attr.datatype, attr, i)
		}
	}
	return attrs, nil
}
