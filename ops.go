package hypergo

// Functions that APIs make use of

/*
#cgo LDFLAGS: -lhyperdex-client
#include <netinet/in.h>
#include "hyperdex/client.h"
*/
import "C"

func (client *Client) get(space string, key string, sc []SearchCriterion, funcName string) ObjectChannel {
	objCh := make(chan Object, CHANNEL_BUFFER_SIZE)
	var status C.enum_hyperdex_client_returncode
	var C_attrs *C.struct_hyperdex_client_attribute
	var C_attrs_sz C.size_t
	var C_attr_checks *C.struct_hyperdex_client_attribute_check
	var C_attr_checks_sz C.size_t
	var err error

	if sc != nil {
		C_attr_checks, C_attr_checks_sz, err = newCAttributeCheckList(sc)
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
		objCh <- Object{Err: newInternalError(status)}
		close(objCh)
		return objCh
	}

	var req request
	switch funcName {
	case "get":
		req = request{
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
	case "search":
		req = request{
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
	}

	client.requests = append(client.requests, req)
	return objCh
}
