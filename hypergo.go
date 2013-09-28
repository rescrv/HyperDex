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
	"io"
	"log"
	"runtime"
	"unsafe" // used only for C.GoBytes
)

// CHANNEL_BUFFER_SIZE is the size of all the returned channels' buffer.
// You can set it to 0 if you want unbuffered channels.
var CHANNEL_BUFFER_SIZE = 1

// Timeout in miliseconds.
// Negative timeout means no timeout.
var TIMEOUT = -1

// Predicates
const (
	FAIL                 = C.HYPERPREDICATE_FAIL
	EQUALS               = C.HYPERPREDICATE_EQUALS
	LESS_EQUAL           = C.HYPERPREDICATE_LESS_EQUAL
	GREATER_EQUAL        = C.HYPERPREDICATE_GREATER_EQUAL
	CONTAINS_LESS_THAN   = C.HYPERPREDICATE_CONTAINS_LESS_THAN // alias of LENGTH_LESS_EQUAL
	REGEX                = C.HYPERPREDICATE_REGEX
	LENGTH_EQUALS        = C.HYPERPREDICATE_LENGTH_EQUALS
	LENGTH_LESS_EQUAL    = C.HYPERPREDICATE_LENGTH_LESS_EQUAL
	LENGTH_GREATER_EQUAL = C.HYPERPREDICATE_LENGTH_GREATER_EQUAL
	CONTAINS             = C.HYPERPREDICATE_CONTAINS
)

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

type SearchCriterion struct {
	Attr      string
	Value     string
	Predicate int
}

type request struct {
	id         int64
	isIterator bool
	success    func()
	failure    func(C.enum_hyperdex_client_returncode)
	complete   func()
}

// Set output of log
func SetLogOutput(w io.Writer) {
	log.SetOutput(w)
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
				if len(client.requests) > 0 {
					var status C.enum_hyperdex_client_returncode
					ret := int64(C.hyperdex_client_loop(client.ptr, C.int(TIMEOUT), &status))
					//log.Printf("hyperdex_client_loop(%X, %d, %X) -> %d\n", unsafe.Pointer(client.ptr), hyperdex_client_loop_timeout, unsafe.Pointer(&status), ret)
					if ret < 0 {
						panic(newInternalError(status).Error())
					}
					// find processed request among pending requests
					for i, req := range client.requests {
						if req.id == ret {
							log.Printf("Processing request %v\n", req.id)
							log.Printf("Status: %v\n", status)
							if status == C.HYPERDEX_CLIENT_SUCCESS {
								if req.success != nil {
									req.success()
								}
								if req.isIterator {
									// We want to break out at here so that the
									// request won't get removed
									break
								} else if req.complete != nil {
									// We want to break out at here so that the
									// request won't get removed
									req.complete()
								}
							} else if status == C.HYPERDEX_CLIENT_SEARCHDONE {
								req.complete()
							} else if req.failure != nil {
								req.failure(status)
							}
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

func (client *Client) Search(space string, sc []SearchCriterion) ObjectChannel {
	objCh := make(chan Object, CHANNEL_BUFFER_SIZE)
	var status C.enum_hyperdex_client_returncode
	var C_attrs *C.struct_hyperdex_client_attribute
	var C_attrs_sz C.size_t

	acList, acSize, err := newCAttributeCheckList(sc)
	if err != nil {
		objCh <- Object{
			Err: err,
		}
		close(objCh)
		return objCh
	}

	req_id := int64(C.hyperdex_client_search(client.ptr,
		C.CString(space), acList, acSize,
		&status, &C_attrs, &C_attrs_sz))

	if req_id < 0 {
		objCh <- Object{
			Err: err,
		}
		close(objCh)
		return objCh
	}

	req := request{
		id:         req_id,
		isIterator: true,
		success: func() {
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
		},
		failure: objChannelFailureCallback(objCh),
		complete: func() {
			close(objCh)
		},
	}

	client.requests = append(client.requests, req)
	return objCh
}

func newCAttributeCheckList(sc []SearchCriterion) (*C.struct_hyperdex_client_attribute_check, C.size_t, error) {
	slice := make([]C.struct_hyperdex_client_attribute_check, 0, len(sc))
	for _, s := range sc {
		ac, err := newCAttributeCheck(s)
		if err != nil {
			return nil, 0, err
		}
		slice = append(slice, *ac)
	}

	return &slice[0], C.size_t(len(sc)), nil
}

func newCAttributeCheck(sc SearchCriterion) (*C.struct_hyperdex_client_attribute_check, error) {
	val, valSize, valType, err := toHyperDexDatatype(sc.Value)
	if err != nil {
		return nil, err
	}

	return &C.struct_hyperdex_client_attribute_check{
		C.CString(sc.Attr),
		val,
		valSize,
		valType,
		C.enum_hyperpredicate(sc.Predicate),
	}, nil
}

// Return a CString representation of the given value, and return
// the datatype of the original value.
func toHyperDexDatatype(value interface{}) (*C.char, C.size_t, C.enum_hyperdatatype, error) {
	var val string
	var size int
	var datatype C.enum_hyperdatatype

	var i int64
	var buf *bytes.Buffer
	var err error

	switch v := value.(type) {
	case string:
		val = v
		datatype = C.HYPERDATATYPE_STRING
		size = len([]byte(val))
		goto RETURN
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
		return nil, 0, 0, fmt.Errorf("Unsupported type '%T'", v)
	}

	// Binary encoding
	buf = new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, i)
	if err != nil {
		return nil, 0, 0, err
	}
	val = buf.String()
	datatype = C.HYPERDATATYPE_INT64
	size = binary.Size(int64(0))

RETURN:
	return C.CString(val), C.size_t(size), datatype, nil
}

func (client *Client) Put(space, key string, attrs Attributes) error {
	return <-client.AsyncPut(space, key, attrs)
}

func (client *Client) AsyncPut(space, key string, attrs Attributes) ErrorChannel {
	errCh := make(chan error, CHANNEL_BUFFER_SIZE)
	var status C.enum_hyperdex_client_returncode

	C_attrs, C_attrs_sz, err := newCTypeAttributeList(attrs)
	if err != nil {
		errCh <- err
		close(errCh)
		return errCh
	}

	fmt.Printf("C_attrs: %v\n", C_attrs)

	req_id := int64(C.hyperdex_client_put(client.ptr, C.CString(space),
		C.CString(key), C.size_t(bytesOf(key)),
		C_attrs, C_attrs_sz,
		&status))

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
			if C_attrs_sz > 0 {
				C.hyperdex_client_destroy_attrs(C_attrs, C_attrs_sz)
			}
		},
		failure:  objChannelFailureCallback(objCh),
		complete: nil,
	}
	client.requests = append(client.requests, req)
	return objCh
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

func newInternalError(status C.enum_hyperdex_client_returncode, a ...interface{}) error {
	return fmt.Errorf("Error code: %d.  Please consult hyperdex/client.h for the meaning of the error code", status)
}

// Convenience function for generating a callback
func errChannelFailureCallback(errCh chan error) func(C.enum_hyperdex_client_returncode) {
	return func(status C.enum_hyperdex_client_returncode) {
		errCh <- newInternalError(status)
		close(errCh)
	}
}

func errChannelSuccessCallback(errCh chan error) func() {
	return func() {
		errCh <- nil
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

func newCTypeAttributeList(attrs Attributes) (C_attrs *C.struct_hyperdex_client_attribute, C_attrs_sz C.size_t, err error) {
	slice := make([]C.struct_hyperdex_client_attribute, 0, len(attrs))
	for key, value := range attrs {
		attr, err := newCTypeAttribute(key, value)
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

func newCTypeAttribute(key string, value interface{}) (*C.struct_hyperdex_client_attribute, error) {
	val, valSize, valType, err := toHyperDexDatatype(value)
	if err != nil {
		return nil, err
	}

	return &C.struct_hyperdex_client_attribute{
		C.CString(key),
		val,
		valSize,
		valType,
		[4]byte{}, // alignment
	}, nil
}

func newAttributeListFromC(C_attrs *C.struct_hyperdex_client_attribute, C_attrs_sz C.size_t) (attrs Attributes, err error) {
	attrs = Attributes{}
	for i := 0; i < int(C_attrs_sz); i++ {
		C_attr := C.GetAttribute(C_attrs, C.int(i))
		attr := C.GoString(C_attr.attr)
		switch C_attr.datatype {
		case C.HYPERDATATYPE_STRING:
			attrs[attr] = C.GoStringN(C_attr.value, C.int(C_attr.value_sz))
		case C.HYPERDATATYPE_INT64:
			var value int64
			buf := bytes.NewBuffer(C.GoBytes(unsafe.Pointer(C_attr.value), C.int(C_attr.value_sz)))
			err := binary.Read(buf, binary.LittleEndian, &value)
			if err != nil {
				return nil, fmt.Errorf("Could not decode INT64 attribute `%s` (#%d)", attr, i)
			}
			attrs[attr] = value
		case C.HYPERDATATYPE_GARBAGE:
			continue
		default:
			return nil, fmt.Errorf("Unknown datatype %d found for attribute `%s` (#%d)", C_attr.datatype, attr, i)
		}
	}
	return attrs, nil
}
