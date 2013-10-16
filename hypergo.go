// Package hypergo is a client for HyperDex.
package hypergo

/*
#cgo LDFLAGS: -lhyperdex-client
#include <netinet/in.h>
#include "hyperdex/client.h"
#include "hyperdex/datastructures.h"
*/
import "C"

import (
	"fmt"
	"io"
	"log"
	"runtime"
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
	arena     *C.struct_hyperdex_ds_arena
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

// Correspond to an array of hyperdex_client_attribute
type Attributes map[string]interface{}

type Map map[interface{}]interface{}

type List []interface{}

type Set []interface{}

// A hyperdex object.
// Err contains any error that happened when trying to retrieve
// this object.
type Object struct {
	Err   error
	Key   string
	Attrs Attributes
}

// Read-only channel of objects
type ObjectChannel <-chan Object

// Read-only channel of errors
type ErrorChannel <-chan error

// Correspond to a hyperdex_client_attribute_check
type Condition struct {
	Attr      string
	Value     interface{}
	Predicate int
}

// An outstanding request that contains three callback functions:
// success, failure, and complete.  success() is called when
// hyperdex returns HYPERDEX_CLIENT_SUCCESS, and failure is called
// in all other cases.
//
// The boolean flag isIterator signifies whether the request is
// that of an iterator, namely hyperdex_client_search and its
// variants.
//
// complete() is called after success() if the request is not
// an iterator, or after receiving HYPREDEX_SEARCH_DONE if the
// request is an iterator.
type request struct {
	id         int64
	isIterator bool
	success    func()
	failure    func(C.enum_hyperdex_client_returncode, string)
	complete   func()
	status     *C.enum_hyperdex_client_returncode
}

// A custom error type that allows for examining HyperDex error code.
type HyperError struct {
	returnCode C.enum_hyperdex_client_returncode
	msg        string
}

func (e HyperError) Error() string {
	return fmt.Sprintf("Error %d: %s", e.returnCode, e.msg)
}

// Set output of log.  Simply a wrapper around log.SetOutput.
func SetLogOutput(w io.Writer) {
	log.SetOutput(w)
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
	C_arena := C.hyperdex_ds_arena_create()

	if C_client == nil {
		return nil, fmt.Errorf("Could not create hyperdex_client (ip=%s, port=%d)", ip, port)
	}
	client := &Client{
		C_client,
		C_arena,
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
						panic(newInternalError(status,
							C.GoString(C.hyperdex_client_error_message(client.ptr))).Error())
					}
					// find processed request among pending requests
					for i, req := range client.requests {
						if req.id == ret {
							log.Printf("Processing request %v\n", req.id)
							log.Printf("Loop status: %v\n", status)
							log.Printf("Request status: %v\n", *req.status)
							if status == C.HYPERDEX_CLIENT_SUCCESS {
								switch *req.status {
								case C.HYPERDEX_CLIENT_SUCCESS:
									log.Println("Request success")
									if req.success != nil {
										req.success()
									}
									if req.isIterator {
										// We want to break out at here so that the
										// request won't get removed
										goto SKIP_DELETING_REQUEST
									} else if req.complete != nil {
										// We want to break out at here so that the
										// request won't get removed
										req.complete()
									}
								case C.HYPERDEX_CLIENT_SEARCHDONE:
									log.Println("Request search done")
									if req.complete != nil {
										req.complete()
									}
								case C.HYPERDEX_CLIENT_CMPFAIL:
									log.Println("Comparison failure")
									if req.failure != nil {
										req.failure(*req.status,
											C.GoString(C.hyperdex_client_error_message(client.ptr)))
									}
								default:
									log.Println("Request failure")
									if req.failure != nil {
										req.failure(*req.status,
											C.GoString(C.hyperdex_client_error_message(client.ptr)))
									}
								}
							} else if req.failure != nil {
								req.failure(status,
									C.GoString(C.hyperdex_client_error_message(client.ptr)))
							}
							client.requests = append(client.requests[:i], client.requests[i+1:]...)
						SKIP_DELETING_REQUEST:
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
