// Package hypergo is a client for HyperDex.
package hyperdex

/*
#cgo LDFLAGS: -lhyperdex-client -lhyperdex-admin
#include <netinet/in.h>
#include "hyperdex/client.h"
#include "hyperdex/admin.h"
#include "hyperdex/datastructures.h"
*/
import "C"

import (
	"fmt"
	"runtime"

	"github.com/rescrv/HyperDex/bindings/go/client"
)

// CHANNEL_BUFFER_SIZE is the size of all the returned channels' buffer.
// You can set it to 0 if you want unbuffered channels.
var CHANNEL_BUFFER_SIZE = 1

// Timeout in miliseconds.
// Negative timeout means no timeout.
var TimeOutMsec = -1

// Admin is a priviledged client used to do meta operations to hyperdex
type Admin struct {
	ptr       *C.struct_hyperdex_admin
	requests  []adminRequest
	closeChan chan bool
}

type adminRequest struct {
	id      int64
	success func()
	failure func(C.enum_hyperdex_admin_returncode, string)
	status  *C.enum_hyperdex_admin_returncode
}

// A hyperdex object.
// Err contains any error that happened when trying to retrieve
// this object.
type Object struct {
	Err   error
	Key   string
	Attrs client.Attributes
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

func NewAdmin(ip string, port int) (*Admin, error) {
	C_admin := C.hyperdex_admin_create(C.CString(ip), C.uint16_t(port))

	if C_admin == nil {
		return nil, fmt.Errorf("Could not create hyperdex_admin (ip=%s, port=%d)", ip, port)
	}

	admin := &Admin{
		C_admin,
		make([]adminRequest, 0, 8),
		make(chan bool, 1),
	}

	go func() {
		for {
			select {
			// quit goroutine when client is destroyed
			case <-admin.closeChan:
				return
			default:
				// check if there are pending requests
				// and only if there are, call hyperdex_client_loop
				if len(admin.requests) > 0 {
					var status C.enum_hyperdex_admin_returncode
					ret := int64(C.hyperdex_admin_loop(admin.ptr, C.int(TimeOutMsec), &status))
					//log.Printf("hyperdex_client_loop(%X, %d, %X) -> %d\n", unsafe.Pointer(client.ptr), hyperdex_client_loop_timeout, unsafe.Pointer(&status), ret)
					if ret < 0 {
						panic(newInternalError(status, "Admin error"))
					}

					if status == C.HYPERDEX_ADMIN_SUCCESS {
						switch *req.status {
						case C.HYPERDEX_ADMIN_SUCCESS:
							if req.success != nil {
								req.success()
							}
						default:
							if req.failure != nil {
								req.failure(*req.status,
									C.GoString(C.hyperdex_admin_error_message(admin.ptr)))
							}
						}
					} else if req.failure != nil {
						req.failure(status,
							C.GoString(C.hyperdex_admin_error_message(admin.ptr)))
					}
					admin.requests = append(admin.requests[:i], admin.requests[i+1:]...)
				}
			}
			// prevent other goroutines from starving
			runtime.Gosched()
		}
		panic("Should not be reached: end of infinite loop")
	}()

	return admin, nil
}

// Destroy closes the connection between the Admin and hyperdex. It has to be used on a admin that is not used anymore.
//
// For every call to NewAdmin, there must be a call to Destroy.
func (admin *Admin) Destroy() {
	close(admin.closeChan)
	C.hyperdex_admin_destroy(admin.ptr)
}
