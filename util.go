package hypergo

// Utility functions

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
	"unsafe"
)

// Return the number of bytes of a string
func bytesOf(str string) int {
	return len([]byte(str))
}

// Utility functions
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
	// case float32:
	// 	i = float64(v)
	// case float64:
	// 	i = v
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
