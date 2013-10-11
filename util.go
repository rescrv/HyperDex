package hypergo

// Utility functions

/*
#cgo LDFLAGS: -lhyperdex-client
#include <netinet/in.h>
#include "hyperdex/client.h"
#include "hyperdex/datastructures.h"

struct hyperdex_client_attribute* GetAttribute(struct hyperdex_client_attribute* list, int i) {
	return &list[i];
}
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"
)

// Return the number of bytes of a string
func bytesOf(str string) int {
	return len([]byte(str))
}

// Concatenate two byte slices to form a new one
func joinByteSlices(old1, old2 []byte) []byte {
	newslice := make([]byte, len(old1)+len(old2))
	copy(newslice, old1)
	copy(newslice[len(old1):], old2)
	return newslice
}

// Utility functions
func newInternalError(status C.enum_hyperdex_client_returncode, msg string) error {
	return HyperError{returnCode: status, msg: msg}
}

// Convenience function for generating a callback
func errChannelFailureCallback(errCh chan error) func(C.enum_hyperdex_client_returncode, string) {
	return func(status C.enum_hyperdex_client_returncode, msg string) {
		errCh <- newInternalError(status, msg)
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
func objChannelFailureCallback(objCh chan Object) func(C.enum_hyperdex_client_returncode, string) {
	return func(status C.enum_hyperdex_client_returncode, msg string) {
		objCh <- Object{Err: newInternalError(status, msg)}
		close(objCh)
	}
}

func (client *Client) newCAttributeList(attrs Attributes) (*C.struct_hyperdex_client_attribute, C.size_t, error) {
	if len(attrs) == 0 {
		return nil, 0, nil
	}

	slice := make([]C.struct_hyperdex_client_attribute, 0, len(attrs))
	for key, value := range attrs {
		attr, err := client.newCAttribute(key, value)
		if err != nil {
			return nil, 0, err
		}
		slice = append(slice, *attr)
	}

	return &slice[0], C.size_t(len(attrs)), nil
}

func (client *Client) newCAttribute(key string, value interface{}) (*C.struct_hyperdex_client_attribute, error) {
	val, valSize, valType, err := client.toHyperDexDatatype(value)
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

func (client *Client) newAttributeListFromC(C_attrs *C.struct_hyperdex_client_attribute, C_attrs_sz C.size_t) (attrs Attributes, err error) {
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
			if buf.Len() == 0 {
				// TODO: I'm not sure what's the sensible value to put here.
				// When buf.Len() = 0, that means the user hasn't specified
				// a value for this attribute, so it's essentially null.
				// But does it make sense to return nil?
				attrs[attr] = nil
			} else {
				err := binary.Read(buf, binary.LittleEndian, &value)
				if err != nil {
					return nil, fmt.Errorf("Could not decode INT64 attribute `%s` (#%d)", attr, i)
				}
				attrs[attr] = value
			}

		case C.HYPERDATATYPE_FLOAT:
			var value float64
			buf := bytes.NewBuffer(C.GoBytes(unsafe.Pointer(C_attr.value), C.int(C_attr.value_sz)))
			if buf.Len() == 0 {
				attrs[attr] = nil
			} else {
				err := binary.Read(buf, binary.LittleEndian, &value)
				if err != nil {
					return nil, fmt.Errorf("Could not decode FLOAT64 attribute `%s` (#%d)", attr, i)
				}
				attrs[attr] = value
			}

		case C.HYPERDATATYPE_LIST_STRING, C.HYPERDATATYPE_SET_STRING:
			// vals := make([]string, 100)
			// buf := bytes.NewBuffer(C.GoBytes(unsafe.Pointer(C_attr.value), C.int(C_attr.value_sz)))
			bs := C.GoBytes(unsafe.Pointer(C_attr.value), C.int(C_attr.value_sz))
			pos := 0
			rem := int(C_attr.value_sz)
			lst := make([]string, 1)

			var buf *bytes.Buffer
			var size int
			for rem >= 4 {
				buf = bytes.NewBuffer(bs[pos : pos+4])
				err := binary.Read(buf, binary.LittleEndian, &size)
				if err != nil {
					return nil, err
				}
				lst = append(lst, string(bs[pos+4:pos+4+size]))
				pos += 4 + size
				rem -= 4 + size
			}

			if rem != 0 {
				return nil, fmt.Errorf("list(string) is improperly structured (file a bug)")
			}

			attrs[attr] = lst

		case C.HYPERDATATYPE_MAP_STRING_STRING, C.HYPERDATATYPE_MAP_STRING_INT64,
			C.HYPERDATATYPE_MAP_STRING_FLOAT, C.HYPERDATATYPE_MAP_INT64_STRING,
			C.HYPERDATATYPE_MAP_INT64_INT64, C.HYPERDATATYPE_MAP_INT64_FLOAT,
			C.HYPERDATATYPE_MAP_FLOAT_STRING, C.HYPERDATATYPE_MAP_FLOAT_INT64,
			C.HYPERDATATYPE_MAP_FLOAT_FLOAT:
			bs := C.GoBytes(unsafe.Pointer(C_attr.value), C.int(C_attr.value_sz))
			pos := 0
			rem := int(C_attr.value_sz)
			m := Map{}

			var buf *bytes.Buffer
			var size int
			for rem >= 4 {
				var key, val interface{}
				var err error
				switch C_attr.datatype {
				// Cases where keys are strings
				case C.HYPERDATATYPE_MAP_STRING_STRING, C.HYPERDATATYPE_MAP_STRING_INT64,
					C.HYPERDATATYPE_MAP_STRING_FLOAT:
					buf = bytes.NewBuffer(bs[pos : pos+4])
					err = binary.Read(buf, binary.LittleEndian, &size)
					if err != nil {
						return nil, err
					}
					key = string(bs[pos+4 : pos+4+size])
					pos += 4 + size
					rem -= 4 + size
				// Cases where keys are integers
				case C.HYPERDATATYPE_MAP_INT64_STRING, C.HYPERDATATYPE_MAP_INT64_INT64,
					C.HYPERDATATYPE_MAP_INT64_FLOAT:
					var intKey int64
					buf = bytes.NewBuffer(bs[pos : pos+8])
					err = binary.Read(buf, binary.LittleEndian, &intKey)
					if err != nil {
						return nil, err
					}
					key = intKey
					pos += 8
					rem -= 8
				// Cases where keys are integers
				case C.HYPERDATATYPE_MAP_FLOAT_STRING, C.HYPERDATATYPE_MAP_FLOAT_INT64,
					C.HYPERDATATYPE_MAP_FLOAT_FLOAT:
					var floatKey float64
					buf = bytes.NewBuffer(bs[pos : pos+8])
					err = binary.Read(buf, binary.LittleEndian, &floatKey)
					if err != nil {
						return nil, err
					}
					key = floatKey
					pos += 8
					rem -= 8
				}

				switch C_attr.datatype {
				// Cases where values are strings
				case C.HYPERDATATYPE_MAP_STRING_STRING, C.HYPERDATATYPE_MAP_INT64_STRING,
					C.HYPERDATATYPE_MAP_FLOAT_STRING:
					buf = bytes.NewBuffer(bs[pos : pos+4])
					err = binary.Read(buf, binary.LittleEndian, &size)
					if err != nil {
						return nil, err
					}
					val = string(bs[pos+4 : pos+4+size])
					pos += 4 + size
					rem -= 4 + size
				// Cases where values are integers
				case C.HYPERDATATYPE_MAP_STRING_INT64, C.HYPERDATATYPE_MAP_INT64_INT64,
					C.HYPERDATATYPE_MAP_FLOAT_INT64:
					var intVal int64
					buf = bytes.NewBuffer(bs[pos : pos+8])
					err = binary.Read(buf, binary.LittleEndian, &intVal)
					if err != nil {
						return nil, err
					}
					val = intVal
					pos += 8
					rem -= 8
				// Cases where values are integers
				case C.HYPERDATATYPE_MAP_STRING_FLOAT, C.HYPERDATATYPE_MAP_INT64_FLOAT,
					C.HYPERDATATYPE_MAP_FLOAT_FLOAT:
					var floatVal float64
					buf = bytes.NewBuffer(bs[pos : pos+8])
					err = binary.Read(buf, binary.LittleEndian, &floatVal)
					if err != nil {
						return nil, err
					}
					key = floatVal
					pos += 8
					rem -= 8
				}
				m[key] = val
			}
			if rem != 0 {
				return nil, fmt.Errorf("list(string) is improperly structured (file a bug)")
			}
			attrs[attr] = m

		case C.HYPERDATATYPE_GARBAGE:
			continue

		default:
			return nil, fmt.Errorf("Unknown datatype %d found for attribute `%s` (#%d)", C_attr.datatype, attr, i)
		}
	}
	return attrs, nil
}

func (client *Client) newCAttributeCheckList(sc []Condition) (*C.struct_hyperdex_client_attribute_check, C.size_t, error) {
	slice := make([]C.struct_hyperdex_client_attribute_check, 0, len(sc))
	for _, s := range sc {
		ac, err := client.newCAttributeCheck(s)
		if err != nil {
			return nil, 0, err
		}
		slice = append(slice, *ac)
	}

	return &slice[0], C.size_t(len(sc)), nil
}

func (client *Client) newCMapAttributeList(attr string, mapAttr Map) (*C.struct_hyperdex_client_map_attribute, C.size_t, error) {
	C_size_t := C.size_t(len(mapAttr))
	if C_size_t == 0 {
		return nil, 0, nil
	}

	slice := make([]C.struct_hyperdex_client_map_attribute, 0, C_size_t)
	for key, item := range mapAttr {
		C_map_attr, err := client.newCMapAttribute(attr, key, item)
		if err != nil {
			return nil, 0, err
		}
		slice = append(slice, *C_map_attr)
	}

	return &slice[0], C_size_t, nil
}

func (client *Client) newCMapAttribute(attrKey string, itemKey interface{}, itemValue interface{}) (*C.struct_hyperdex_client_map_attribute, error) {
	key, keySize, keyType, err := client.toHyperDexDatatype(itemKey)
	if err != nil {
		return nil, err
	}

	val, valSize, valType, err := client.toHyperDexDatatype(itemValue)
	if err != nil {
		return nil, err
	}

	return &C.struct_hyperdex_client_map_attribute{
		C.CString(attrKey),
		key,
		keySize,
		keyType,
		[4]byte{}, // alignment
		val,
		valSize,
		valType,
		[4]byte{},
	}, nil
}

func (client *Client) newCAttributeCheck(sc Condition) (*C.struct_hyperdex_client_attribute_check, error) {
	val, valSize, valType, err := client.toHyperDexDatatype(sc.Value)
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
func (client *Client) toHyperDexDatatype(value interface{}) (*C.char, C.size_t, C.enum_hyperdatatype, error) {
	buf := new(bytes.Buffer)

	processInt64 := func(val int64) (*C.char, C.size_t, C.enum_hyperdatatype, error) {
		err := binary.Write(buf, binary.LittleEndian, val)
		if err != nil {
			return nil, 0, 0, err
		}
		valStr := buf.String()
		return C.CString(valStr), C.size_t(bytesOf(valStr)), C.HYPERDATATYPE_INT64, nil
	}

	processFloat64 := func(val float64) (*C.char, C.size_t, C.enum_hyperdatatype, error) {
		err := binary.Write(buf, binary.LittleEndian, val)
		if err != nil {
			return nil, 0, 0, err
		}
		valStr := buf.String()
		return C.CString(valStr), C.size_t(bytesOf(valStr)), C.HYPERDATATYPE_FLOAT, nil
	}

	processString := func(val string) (*C.char, C.size_t, C.enum_hyperdatatype, error) {
		return C.CString(val), C.size_t(bytesOf(val)), C.HYPERDATATYPE_STRING, nil
	}

	processList := func(vals List) (*C.char, C.size_t, C.enum_hyperdatatype, error) {
		C_ds_list := C.hyperdex_ds_allocate_list(client.arena)
		var C_ds_status C.enum_hyperdex_ds_returncode
		for _, val := range vals {
			t := reflect.TypeOf(val)
			v := reflect.ValueOf(val)
			switch t.Kind() {
			case reflect.String:
				C_string := C.CString(v.String())
				C_size_t := C.size_t(v.Len())
				C.hyperdex_ds_list_append_string(C_ds_list,
					C_string, C_size_t, &C_ds_status)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
				reflect.Int64:
				C.hyperdex_ds_list_append_int(C_ds_list,
					C.int64_t(v.Int()), &C_ds_status)
			case reflect.Float32, reflect.Float64:
				C.hyperdex_ds_list_append_float(C_ds_list,
					C.double(v.Float()), &C_ds_status)
			default:
				return nil, 0, 0, fmt.Errorf("Unsupported type within list %s", t.String())
			}
			if C_ds_status != C.HYPERDEX_DS_SUCCESS {
				return nil, 0, 0, fmt.Errorf("DS error: %d", C_ds_status)
			}
		}
		var C_string *C.char
		var C_size_t C.size_t
		var C_datatype C.enum_hyperdatatype
		C.hyperdex_ds_list_finalize(C_ds_list, &C_ds_status,
			&C_string, &C_size_t, &C_datatype)
		return C_string, C_size_t, C_datatype, nil
	}

	t := reflect.TypeOf(value)
	v := reflect.ValueOf(value)
	switch t.Kind() {
	case reflect.String:
		return processString(v.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Int64:
		return processInt64(v.Int())
	case reflect.Float32, reflect.Float64:
		return processFloat64(v.Float())
	case reflect.Slice:
		lst := value.(List)
		return processList(lst)
	default:
		return nil, 0, 0, fmt.Errorf("Unsupported type %s", t.String())
	}
}
