package client

// #cgo pkg-config: hyperdex-client
// #include <stdlib.h>
// #include <string.h>
// #include <poll.h>
// #include <hyperdex/client.h>
// #include <hyperdex/datastructures.h>
import "C"
import "unsafe"
import "reflect"
import "fmt"
import "runtime"
import "sync"

const (
	SUCCESS      = C.HYPERDEX_CLIENT_SUCCESS
	NOTFOUND     = C.HYPERDEX_CLIENT_NOTFOUND
	SEARCHDONE   = C.HYPERDEX_CLIENT_SEARCHDONE
	CMPFAIL      = C.HYPERDEX_CLIENT_CMPFAIL
	READONLY     = C.HYPERDEX_CLIENT_READONLY
	UNKNOWNSPACE = C.HYPERDEX_CLIENT_UNKNOWNSPACE
	COORDFAIL    = C.HYPERDEX_CLIENT_COORDFAIL
	SERVERERROR  = C.HYPERDEX_CLIENT_SERVERERROR
	POLLFAILED   = C.HYPERDEX_CLIENT_POLLFAILED
	OVERFLOW     = C.HYPERDEX_CLIENT_OVERFLOW
	RECONFIGURE  = C.HYPERDEX_CLIENT_RECONFIGURE
	TIMEOUT      = C.HYPERDEX_CLIENT_TIMEOUT
	UNKNOWNATTR  = C.HYPERDEX_CLIENT_UNKNOWNATTR
	DUPEATTR     = C.HYPERDEX_CLIENT_DUPEATTR
	NONEPENDING  = C.HYPERDEX_CLIENT_NONEPENDING
	DONTUSEKEY   = C.HYPERDEX_CLIENT_DONTUSEKEY
	WRONGTYPE    = C.HYPERDEX_CLIENT_WRONGTYPE
	NOMEM        = C.HYPERDEX_CLIENT_NOMEM
	INTERRUPTED  = C.HYPERDEX_CLIENT_INTERRUPTED
	CLUSTER_JUMP = C.HYPERDEX_CLIENT_CLUSTER_JUMP
	OFFLINE      = C.HYPERDEX_CLIENT_OFFLINE
	INTERNAL     = C.HYPERDEX_CLIENT_INTERNAL
	EXCEPTION    = C.HYPERDEX_CLIENT_EXCEPTION
	GARBAGE      = C.HYPERDEX_CLIENT_GARBAGE
)

const (
	FAIL                 = C.HYPERPREDICATE_FAIL
	EQUALS               = C.HYPERPREDICATE_EQUALS
	LESS_THAN            = C.HYPERPREDICATE_LESS_THAN
	LESS_EQUAL           = C.HYPERPREDICATE_LESS_EQUAL
	GREATER_EQUAL        = C.HYPERPREDICATE_GREATER_EQUAL
	GREATER_THAN         = C.HYPERPREDICATE_GREATER_THAN
	REGEX                = C.HYPERPREDICATE_REGEX
	LENGTH_EQUALS        = C.HYPERPREDICATE_LENGTH_EQUALS
	LENGTH_LESS_EQUAL    = C.HYPERPREDICATE_LENGTH_LESS_EQUAL
	LENGTH_GREATER_EQUAL = C.HYPERPREDICATE_LENGTH_GREATER_EQUAL
	CONTAINS             = C.HYPERPREDICATE_CONTAINS
)

type Status int

type Error struct {
	Status   Status
	Message  string
	Location string
}

type Value interface{}

type Set []Value
type SetString []string
type SetInt []int64
type SetFloat []float64

type List []Value
type ListString []string
type ListInt []int64
type ListFloat []float64

type Map map[Value]Value
type MapStringString map[string]string
type MapStringInt map[string]int64
type MapStringFloat map[string]float64
type MapIntString map[int64]string
type MapIntInt map[int64]int64
type MapIntFloat map[int64]float64
type MapFloatString map[float64]string
type MapFloatInt map[float64]int64
type MapFloatFloat map[float64]float64

type Predicate struct {
	Attr      string
	Value     Value
	Predicate int
}

type Attributes map[string]Value
type MapAttributes map[string]Map
type AttributeNames []string

func (s Status) String() string {
	return C.GoString(C.hyperdex_client_returncode_to_string(C.enum_hyperdex_client_returncode(s)))
}

func (e Error) Error() string {
	return C.GoString(C.hyperdex_client_returncode_to_string(C.enum_hyperdex_client_returncode(e.Status))) + ": " + e.Message
}

func (e Error) String() string {
	return e.Error()
}

type cIterator struct {
	status   C.enum_hyperdex_client_returncode
	attrs    *C.struct_hyperdex_client_attribute
	attrs_sz C.size_t
	attrChan chan Attributes
	errChan  chan Error
}

type Client struct {
	counter uint64
	mutex   sync.Mutex
	clients []*innerClient
	errChan chan Error
}

func (client *Client) convertCString(arena *C.struct_hyperdex_ds_arena, s string, cs **C.char) {
	x := C.CString(s)
	defer C.free(unsafe.Pointer(x))
	var e C.enum_hyperdex_ds_returncode
	var throwaway C.size_t
	if C.hyperdex_ds_copy_string(arena, x, C.strlen(x)+1, &e, cs, &throwaway) < 0 {
		panic("failed to allocate memory")
	}
}

func (client *Client) convertSpacename(arena *C.struct_hyperdex_ds_arena, spacename string, space **C.char) error {
	client.convertCString(arena, spacename, space)
	return nil
}

// Return the number of bytes of a string
func bytesOf(str string) C.size_t {
	return C.size_t(len([]byte(str)))
}

func (client *Client) convertByteString(arena *C.struct_hyperdex_ds_arena, val []byte) (k *C.char, k_sz C.size_t, dt C.enum_hyperdatatype, err error) {
	return client.convertString(arena, string(val[:]))
}

func (client *Client) convertString(arena *C.struct_hyperdex_ds_arena, val string) (k *C.char, k_sz C.size_t, dt C.enum_hyperdatatype, err error) {
	x := C.CString(val)
	defer C.free(unsafe.Pointer(x))
	var e C.enum_hyperdex_ds_returncode
	if C.hyperdex_ds_copy_string(arena, x, bytesOf(val), &e, &k, &k_sz) < 0 {
		panic("failed to allocate memory")
	}
	dt = C.HYPERDATATYPE_STRING
	return
}

func (client *Client) convertInt(arena *C.struct_hyperdex_ds_arena, val int64) (k *C.char, k_sz C.size_t, dt C.enum_hyperdatatype, err error) {
	var e C.enum_hyperdex_ds_returncode
	if C.hyperdex_ds_copy_int(arena, C.int64_t(val), &e, &k, &k_sz) < 0 {
		panic("failed to allocate memory")
	}
	dt = C.HYPERDATATYPE_INT64
	return
}

func (client *Client) convertFloat(arena *C.struct_hyperdex_ds_arena, val float64) (k *C.char, k_sz C.size_t, dt C.enum_hyperdatatype, err error) {
	var e C.enum_hyperdex_ds_returncode
	if C.hyperdex_ds_copy_float(arena, C.double(val), &e, &k, &k_sz) < 0 {
		panic("failed to allocate memory")
	}
	dt = C.HYPERDATATYPE_FLOAT
	return
}

func (client *Client) convertList(arena *C.struct_hyperdex_ds_arena, lst interface{}) (k *C.char, k_sz C.size_t, dt C.enum_hyperdatatype, err error) {
	C_ds_list := C.hyperdex_ds_allocate_list(arena)
	if C_ds_list == nil {
		panic("failed to allocate memory")
	}
	var C_ds_status C.enum_hyperdex_ds_returncode

	switch vals := lst.(type) {
	case List:
		for _, val := range vals {
			t := reflect.TypeOf(val)
			v := reflect.ValueOf(val)
			var rc int
			switch t.Kind() {
			case reflect.String:
				C_string := C.CString(v.String())
				C_size_t := C.size_t(v.Len())
				rc = int(C.hyperdex_ds_list_append_string(C_ds_list,
					C_string, C_size_t, &C_ds_status))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
				reflect.Int64:
				rc = int(C.hyperdex_ds_list_append_int(C_ds_list,
					C.int64_t(v.Int()), &C_ds_status))
			case reflect.Float32, reflect.Float64:
				rc = int(C.hyperdex_ds_list_append_float(C_ds_list,
					C.double(v.Float()), &C_ds_status))
			default:
				return nil, 0, 0, fmt.Errorf("Unsupported type within list %s", t.String())
			}
			if rc < 0 && C_ds_status != C.HYPERDEX_DS_SUCCESS {
				return nil, 0, 0, fmt.Errorf("DS error: %d", C_ds_status)
			}
		}

	case ListString:
		for _, val := range vals {
			C_string := C.CString(val)
			C_size_t := C.size_t(bytesOf(val))
			if C.hyperdex_ds_list_append_string(C_ds_list, C_string, C_size_t, &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case ListInt:
		for _, val := range vals {
			if C.hyperdex_ds_list_append_int(C_ds_list, C.int64_t(val), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case ListFloat:
		for _, val := range vals {
			if C.hyperdex_ds_list_append_float(C_ds_list, C.double(val), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}
	}

	var C_string *C.char
	var C_size_t C.size_t
	var C_datatype C.enum_hyperdatatype
	if C.hyperdex_ds_list_finalize(C_ds_list, &C_ds_status, &C_string, &C_size_t, &C_datatype) < 0 {
		panic("failed to allocate memory")
	}
	return C_string, C_size_t, C_datatype, nil
}

func (client *Client) convertSet(arena *C.struct_hyperdex_ds_arena, set interface{}) (k *C.char, k_sz C.size_t, dt C.enum_hyperdatatype, err error) {
	C_ds_set := C.hyperdex_ds_allocate_set(arena)
	if C_ds_set == nil {
		panic("failed to allocate memory")
	}
	var C_ds_status C.enum_hyperdex_ds_returncode

	switch vals := set.(type) {
	case Set:
		for _, val := range vals {
			t := reflect.TypeOf(val)
			v := reflect.ValueOf(val)
			var rc int
			switch t.Kind() {
			case reflect.String:
				C_string := C.CString(v.String())
				C_size_t := C.size_t(v.Len())
				rc = int(C.hyperdex_ds_set_insert_string(C_ds_set,
					C_string, C_size_t, &C_ds_status))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
				reflect.Int64:
				rc = int(C.hyperdex_ds_set_insert_int(C_ds_set,
					C.int64_t(v.Int()), &C_ds_status))
			case reflect.Float32, reflect.Float64:
				rc = int(C.hyperdex_ds_set_insert_float(C_ds_set,
					C.double(v.Float()), &C_ds_status))
			default:
				return nil, 0, 0, fmt.Errorf("Unsupported type within set %s", t.String())
			}
			if rc < 0 && C_ds_status != C.HYPERDEX_DS_SUCCESS {
				return nil, 0, 0, fmt.Errorf("DS error: %d", C_ds_status)
			}
		}

	case SetString:
		for _, val := range vals {
			C_string := C.CString(val)
			C_size_t := C.size_t(bytesOf(val))
			if C.hyperdex_ds_set_insert_string(C_ds_set, C_string, C_size_t, &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case SetInt:
		for _, val := range vals {
			if C.hyperdex_ds_set_insert_int(C_ds_set, C.int64_t(val), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case SetFloat:
		for _, val := range vals {
			if C.hyperdex_ds_set_insert_float(C_ds_set, C.double(val), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}
	}

	var C_string *C.char
	var C_size_t C.size_t
	var C_datatype C.enum_hyperdatatype
	if C.hyperdex_ds_set_finalize(C_ds_set, &C_ds_status, &C_string, &C_size_t, &C_datatype) < 0 {
		panic("failed to allocate memory")
	}
	return C_string, C_size_t, C_datatype, nil
}

func (client *Client) convertMap(arena *C.struct_hyperdex_ds_arena, v interface{}) (k *C.char, k_sz C.size_t, dt C.enum_hyperdatatype, err error) {
	C_ds_map := C.hyperdex_ds_allocate_map(arena)
	var C_ds_status C.enum_hyperdex_ds_returncode

	switch m := v.(type) {
	case Map:
		for key, value := range m {
			t := reflect.TypeOf(key)
			v := reflect.ValueOf(key)
			switch t.Kind() {
			case reflect.String:
				C_string := C.CString(v.String())
				C_size_t := C.size_t(v.Len())
				C.hyperdex_ds_map_insert_key_string(C_ds_map,
					C_string, C_size_t, &C_ds_status)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
				reflect.Int64:
				C.hyperdex_ds_map_insert_key_int(C_ds_map,
					C.int64_t(v.Int()), &C_ds_status)
			case reflect.Float32, reflect.Float64:
				C.hyperdex_ds_map_insert_key_float(C_ds_map,
					C.double(v.Float()), &C_ds_status)
			default:
				return nil, 0, 0, fmt.Errorf("Unsupported key type within map %s", t.String())
			}
			if C_ds_status != C.HYPERDEX_DS_SUCCESS {
				return nil, 0, 0, fmt.Errorf("DS error: %d", C_ds_status)
			}

			t = reflect.TypeOf(value)
			v = reflect.ValueOf(value)
			switch t.Kind() {
			case reflect.String:
				C_string := C.CString(v.String())
				C_size_t := C.size_t(v.Len())
				C.hyperdex_ds_map_insert_val_string(C_ds_map,
					C_string, C_size_t, &C_ds_status)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
				reflect.Int64:
				C.hyperdex_ds_map_insert_val_int(C_ds_map,
					C.int64_t(v.Int()), &C_ds_status)
			case reflect.Float32, reflect.Float64:
				C.hyperdex_ds_map_insert_val_float(C_ds_map,
					C.double(v.Float()), &C_ds_status)
			default:
				return nil, 0, 0, fmt.Errorf("Unsupported value type within map %s", t.String())
			}
			if C_ds_status != C.HYPERDEX_DS_SUCCESS {
				return nil, 0, 0, fmt.Errorf("DS error: %d", C_ds_status)
			}
		}

	case MapStringString:
		for key, value := range m {
			C_string := C.CString(key)
			C_size_t := C.size_t(bytesOf(key))
			if C.hyperdex_ds_map_insert_key_string(C_ds_map, C_string, C_size_t, &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
			C_string = C.CString(value)
			C_size_t = C.size_t(bytesOf(value))
			if C.hyperdex_ds_map_insert_val_string(C_ds_map, C_string, C_size_t, &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case MapStringInt:
		for key, value := range m {
			C_string := C.CString(key)
			C_size_t := C.size_t(bytesOf(key))
			if C.hyperdex_ds_map_insert_key_string(C_ds_map, C_string, C_size_t, &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
			if C.hyperdex_ds_map_insert_val_int(C_ds_map, C.int64_t(value), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case MapStringFloat:
		for key, value := range m {
			C_string := C.CString(key)
			C_size_t := C.size_t(bytesOf(key))
			if C.hyperdex_ds_map_insert_key_string(C_ds_map, C_string, C_size_t, &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
			if C.hyperdex_ds_map_insert_val_float(C_ds_map, C.double(value), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case MapIntString:
		for key, value := range m {
			if C.hyperdex_ds_map_insert_key_int(C_ds_map, C.int64_t(key), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
			C_string := C.CString(value)
			C_size_t := C.size_t(bytesOf(value))
			if C.hyperdex_ds_map_insert_val_string(C_ds_map, C_string, C_size_t, &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case MapIntInt:
		for key, value := range m {
			if C.hyperdex_ds_map_insert_key_int(C_ds_map, C.int64_t(key), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
			if C.hyperdex_ds_map_insert_val_int(C_ds_map, C.int64_t(value), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case MapIntFloat:
		for key, value := range m {
			if C.hyperdex_ds_map_insert_key_int(C_ds_map, C.int64_t(key), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
			if C.hyperdex_ds_map_insert_val_float(C_ds_map, C.double(value), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case MapFloatString:
		for key, value := range m {
			if C.hyperdex_ds_map_insert_key_float(C_ds_map, C.double(key), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
			C_string := C.CString(value)
			C_size_t := C.size_t(bytesOf(value))
			if C.hyperdex_ds_map_insert_val_string(C_ds_map, C_string, C_size_t, &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case MapFloatInt:
		for key, value := range m {
			if C.hyperdex_ds_map_insert_key_float(C_ds_map, C.double(key), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
			if C.hyperdex_ds_map_insert_val_int(C_ds_map, C.int64_t(value), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}

	case MapFloatFloat:
		for key, value := range m {
			if C.hyperdex_ds_map_insert_key_float(C_ds_map, C.double(key), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
			if C.hyperdex_ds_map_insert_val_float(C_ds_map, C.double(value), &C_ds_status) < 0 {
				panic("failed to allocate memory")
			}
		}
	}

	var C_string *C.char
	var C_size_t C.size_t
	var C_datatype C.enum_hyperdatatype
	if C.hyperdex_ds_map_finalize(C_ds_map, &C_ds_status, &C_string, &C_size_t, &C_datatype) < 0 {
		panic("failed to allocate memory")
	}
	return C_string, C_size_t, C_datatype, nil
}

func (client *Client) convertType(arena *C.struct_hyperdex_ds_arena, val Value) (k *C.char, k_sz C.size_t, dt C.enum_hyperdatatype, err error) {
	t := reflect.TypeOf(val)
	v := reflect.ValueOf(val)
	switch t.Kind() {
	case reflect.String:
		k, k_sz, dt, err = client.convertString(arena, v.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:

		k, k_sz, dt, err = client.convertInt(arena, v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		k, k_sz, dt, err = client.convertInt(arena, int64(v.Uint()))
	case reflect.Float32, reflect.Float64:
		k, k_sz, dt, err = client.convertFloat(arena, v.Float())
	case reflect.Slice:
		switch val.(type) {
		case []byte:
			k, k_sz, dt, err = client.convertByteString(arena, val.([]byte))
		case Set, SetString, SetInt, SetFloat:
			k, k_sz, dt, err = client.convertSet(arena, val)
		case List, ListString, ListInt, ListFloat:
			k, k_sz, dt, err = client.convertList(arena, val)
		default:
			err = fmt.Errorf("Unsupported type %s", t.String())
		}
	case reflect.Map:
		k, k_sz, dt, err = client.convertMap(arena, val)
	default:
		err = fmt.Errorf("Unsupported type %s", t.String())
	}
	return
}

func (client *Client) convertKey(arena *C.struct_hyperdex_ds_arena, key Value, k **C.char, k_sz *C.size_t) (err error) {
	var er error
	*k, *k_sz, _, err = client.convertType(arena, key)
	if er != nil {
		err = fmt.Errorf("could not convert key to HyperDex type: %s", err.Error())
	}
	return
}

func (client *Client) convertAttributenames(arena *C.struct_hyperdex_ds_arena, names AttributeNames, ns ***C.char, ns_sz *C.size_t) (err error) {
	*ns = nil
	*ns_sz = 0
	if len(names) == 0 {
		return
	}
	slice := make([]*C.char, 0, len(names))
	for _, name := range names {
		var n *C.char
		client.convertCString(arena, name, &n)
		slice = append(slice, n)
	}
	*ns = &slice[0]
	*ns_sz = C.size_t(len(names))
	return
}

func (client *Client) convertPredicates(arena *C.struct_hyperdex_ds_arena, predicates []Predicate, preds **C.struct_hyperdex_client_attribute_check, preds_sz *C.size_t) (err error) {
	*preds = nil
	*preds_sz = 0
	if len(predicates) == 0 {
		return
	}
	slice := make([]C.struct_hyperdex_client_attribute_check, 0, len(predicates))
	for _, predicate := range predicates {
		var n *C.char
		client.convertCString(arena, predicate.Attr, &n)
		var v *C.char
		var v_sz C.size_t
		var dt C.enum_hyperdatatype
		v, v_sz, dt, err = client.convertType(arena, predicate.Value)
		if err != nil {
			return
		}
		slice = append(slice, C.struct_hyperdex_client_attribute_check{n, v, v_sz, dt, C.enum_hyperpredicate(predicate.Predicate)})
	}
	*preds = &slice[0]
	*preds_sz = C.size_t(len(predicates))
	return
}

func (client *Client) convertAttributes(arena *C.struct_hyperdex_ds_arena, attributes Attributes, attrs **C.struct_hyperdex_client_attribute, attrs_sz *C.size_t) (err error) {
	*attrs = nil
	*attrs_sz = 0
	if len(attributes) == 0 {
		return
	}
	slice := make([]C.struct_hyperdex_client_attribute, 0, len(attributes))
	for key, value := range attributes {
		var k *C.char
		var v *C.char
		var v_sz C.size_t
		var dt C.enum_hyperdatatype
		client.convertCString(arena, key, &k)
		v, v_sz, dt, err = client.convertType(arena, value)
		if err != nil {
			return
		}
		slice = append(slice, C.struct_hyperdex_client_attribute{k, v, v_sz, dt, [4]byte{}})
	}
	*attrs = &slice[0]
	*attrs_sz = C.size_t(len(attributes))
	return
}

func (client *Client) convertMapattributes(arena *C.struct_hyperdex_ds_arena, mapattributes MapAttributes, mapattrs **C.struct_hyperdex_client_map_attribute, mapattrs_sz *C.size_t) (err error) {
	*mapattrs = nil
	*mapattrs_sz = 0
	if len(mapattributes) == 0 {
		return
	}
	slice := make([]C.struct_hyperdex_client_map_attribute, 0)
	size := 0
	for attr, mapval := range mapattributes {
		var a *C.char
		client.convertCString(arena, attr, &a)
		/*if mapAttr, ok := mapval.(Map); ok {*/
		if len(mapval) == 0 {
			continue
		}
		for key, value := range mapval {
			var k *C.char
			var k_sz C.size_t
			var k_dt C.enum_hyperdatatype
			var v *C.char
			var v_sz C.size_t
			var v_dt C.enum_hyperdatatype
			k, k_sz, k_dt, err = client.convertType(arena, key)
			if err != nil {
				return err
			}
			v, v_sz, v_dt, err = client.convertType(arena, value)
			if err != nil {
				return err
			}
			slice = append(slice, C.struct_hyperdex_client_map_attribute{a, k, k_sz, k_dt, [4]byte{}, v, v_sz, v_dt, [4]byte{}})
			size++
		}
		/*} else {
		    return fmt.Errorf("%s is not a map.", attr)
		}*/
	}

	*mapattrs = &slice[0]
	*mapattrs_sz = C.size_t(size)
	return
}

func (client *Client) convertSortby(arena *C.struct_hyperdex_ds_arena, attr string, out **C.char) error {
	client.convertCString(arena, attr, out)
	return nil
}

func (client *Client) convertLimit(arena *C.struct_hyperdex_ds_arena, limit uint32, out *C.uint64_t) error {
	*out = C.uint64_t(limit)
	return nil
}

func (client *Client) convertMaxmin(arena *C.struct_hyperdex_ds_arena, maxmin string, out *C.int) error {
	if maxmin == "max" || maxmin == "maximum" {
		*out = 1
	} else {
		*out = 0
	}
	return nil
}

func (client *innerClient) buildAttributes(_attrs *C.struct_hyperdex_client_attribute, _attrs_sz C.size_t) (attributes Attributes, err error) {
	var attrs []C.struct_hyperdex_client_attribute
	h := (*reflect.SliceHeader)(unsafe.Pointer(&attrs))
	h.Data = uintptr(unsafe.Pointer(_attrs))
	h.Len = int(_attrs_sz)
	h.Cap = int(_attrs_sz)
	attributes = Attributes{}
	for i, attr := range attrs {
		var C_iter C.struct_hyperdex_ds_iterator
		name := C.GoString(attr.attr)
		switch attr.datatype {
		case C.HYPERDATATYPE_STRING:
			attributes[name] = C.GoStringN(attr.value, C.int(attr.value_sz))

		case C.HYPERDATATYPE_INT64:
			var C_int C.int64_t
			if C.hyperdex_ds_unpack_int(attr.value, attr.value_sz, &C_int) < 0 {
				return nil, fmt.Errorf("Server sent a malformed int")
			}
			attributes[name] = int64(C_int)

		case C.HYPERDATATYPE_FLOAT:
			var C_double C.double
			if C.hyperdex_ds_unpack_float(attr.value, attr.value_sz, &C_double) < 0 {
				return nil, fmt.Errorf("Server sent a malformed float")
			}
			attributes[name] = float64(C_double)

		case C.HYPERDATATYPE_LIST_STRING:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_LIST_STRING,
				attr.value, attr.value_sz)
			lst := make(ListString, 0)

			for {
				var C_string *C.char
				var C_size_t C.size_t
				status := C.hyperdex_ds_iterate_list_string_next(&C_iter,
					&C_string, &C_size_t)
				if status > 0 {
					lst = append(lst, C.GoStringN(C_string, C.int(C_size_t)))
				} else if status < 0 {
					return nil, fmt.Errorf("Server sent a corrupted list of strings")
				} else {
					break
				}
			}

			attributes[name] = lst

		case C.HYPERDATATYPE_LIST_INT64:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_LIST_INT64,
				attr.value, attr.value_sz)
			lst := make(ListInt, 0)

			for {
				var num C.int64_t
				status := C.hyperdex_ds_iterate_list_int_next(&C_iter, &num)
				if status > 0 {
					lst = append(lst, int64(num))
				} else if status < 0 {
					return nil, fmt.Errorf("Server sent a orrupted list of integers")
				} else {
					break
				}
			}

			attributes[name] = lst

		case C.HYPERDATATYPE_LIST_FLOAT:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_LIST_FLOAT,
				attr.value, attr.value_sz)
			lst := make(ListFloat, 0)

			for {
				var num C.double
				status := C.hyperdex_ds_iterate_list_float_next(&C_iter, &num)
				if status > 0 {
					lst = append(lst, float64(num))
				} else if status < 0 {
					return nil, fmt.Errorf("Server sent a corrupted list of floats")
				} else {
					break
				}
			}

			attributes[name] = lst

		case C.HYPERDATATYPE_SET_STRING:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_SET_STRING,
				attr.value, attr.value_sz)
			lst := make(SetString, 0)

			for {
				var C_string *C.char
				var C_size_t C.size_t
				status := C.hyperdex_ds_iterate_set_string_next(&C_iter,
					&C_string, &C_size_t)
				if status > 0 {
					lst = append(lst, C.GoStringN(C_string, C.int(C_size_t)))
				} else if status < 0 {
					return nil, fmt.Errorf("Server sent a corrupted set of strings")
				} else {
					break
				}
			}

			attributes[name] = lst

		case C.HYPERDATATYPE_SET_INT64:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_SET_INT64,
				attr.value, attr.value_sz)
			lst := make(SetInt, 0)

			for {
				var num C.int64_t
				status := C.hyperdex_ds_iterate_set_int_next(&C_iter, &num)
				if status > 0 {
					lst = append(lst, int64(num))
				} else if status < 0 {
					return nil, fmt.Errorf("Sever sent a corrupted set of integers")
				} else {
					break
				}
			}

			attributes[name] = lst

		case C.HYPERDATATYPE_SET_FLOAT:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_SET_FLOAT,
				attr.value, attr.value_sz)
			lst := make(SetFloat, 0)

			for {
				var num C.double
				status := C.hyperdex_ds_iterate_set_float_next(&C_iter, &num)
				if status > 0 {
					lst = append(lst, float64(num))
				} else if status < 0 {
					return nil, fmt.Errorf("Sever sent a corrupted set of floats")
				} else {
					break
				}
			}

			attributes[name] = lst

		case C.HYPERDATATYPE_MAP_STRING_STRING:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_MAP_STRING_STRING,
				attr.value, attr.value_sz)
			m := make(MapStringString)

			for {
				var C_key_string *C.char
				var C_key_size_t C.size_t
				var C_value_string *C.char
				var C_value_size_t C.size_t

				status := C.hyperdex_ds_iterate_map_string_string_next(&C_iter,
					&C_key_string, &C_key_size_t, &C_value_string, &C_value_size_t)
				if status > 0 {
					m[C.GoStringN(C_key_string, C.int(C_key_size_t))] = C.GoStringN(C_value_string, C.int(C_value_size_t))
				} else if status < 0 {
					return nil, fmt.Errorf("Sever sent a corrupted map of strings to strings")
				} else {
					break
				}
			}

			attributes[name] = m

		case C.HYPERDATATYPE_MAP_STRING_INT64:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_MAP_STRING_INT64,
				attr.value, attr.value_sz)
			m := make(MapStringInt)

			for {
				var C_key_string *C.char
				var C_key_size_t C.size_t
				var C_value C.int64_t

				status := C.hyperdex_ds_iterate_map_string_int_next(&C_iter,
					&C_key_string, &C_key_size_t, &C_value)
				if status > 0 {
					m[C.GoStringN(C_key_string, C.int(C_key_size_t))] = int64(C_value)
				} else if status < 0 {
					return nil, fmt.Errorf("Sever sent a corrupted map of strings to integers")
				} else {
					break
				}
			}

			attributes[name] = m

		case C.HYPERDATATYPE_MAP_STRING_FLOAT:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_MAP_STRING_FLOAT,
				attr.value, attr.value_sz)
			m := make(MapStringFloat)

			for {
				var C_key_string *C.char
				var C_key_size_t C.size_t
				var C_value C.double

				status := C.hyperdex_ds_iterate_map_string_float_next(&C_iter,
					&C_key_string, &C_key_size_t, &C_value)
				if status > 0 {
					m[C.GoStringN(C_key_string, C.int(C_key_size_t))] = float64(C_value)
				} else if status < 0 {
					return nil, fmt.Errorf("Sever sent a corrupted map of strings to floats")
				} else {
					break
				}
			}

			attributes[name] = m

		case C.HYPERDATATYPE_MAP_INT64_STRING:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_MAP_INT64_STRING,
				attr.value, attr.value_sz)
			m := make(MapIntString)

			for {
				var C_key C.int64_t
				var C_val_string *C.char
				var C_val_size_t C.size_t

				status := C.hyperdex_ds_iterate_map_int_string_next(&C_iter,
					&C_key, &C_val_string, &C_val_size_t)
				if status > 0 {
					m[int64(C_key)] = C.GoStringN(C_val_string, C.int(C_val_size_t))
				} else if status < 0 {
					return nil, fmt.Errorf("Sever sent a corrupted map of integers to strings")
				} else {
					break
				}
			}

			attributes[name] = m

		case C.HYPERDATATYPE_MAP_INT64_INT64:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_MAP_INT64_INT64,
				attr.value, attr.value_sz)
			m := make(MapIntInt)

			for {
				var C_key C.int64_t
				var C_val C.int64_t

				status := C.hyperdex_ds_iterate_map_int_int_next(&C_iter,
					&C_key, &C_val)
				if status > 0 {
					m[int64(C_key)] = int64(C_val)
				} else if status < 0 {
					return nil, fmt.Errorf("Sever sent a corrupted map of integers to integers")
				} else {
					break
				}
			}

			attributes[name] = m

		case C.HYPERDATATYPE_MAP_INT64_FLOAT:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_MAP_INT64_FLOAT,
				attr.value, attr.value_sz)
			m := make(MapIntFloat)

			for {
				var C_key C.int64_t
				var C_val C.double

				status := C.hyperdex_ds_iterate_map_int_float_next(&C_iter,
					&C_key, &C_val)
				if status > 0 {
					m[int64(C_key)] = float64(C_val)
				} else if status < 0 {
					return nil, fmt.Errorf("Sever sent a corrupted map of integers to floats")
				} else {
					break
				}
			}

			attributes[name] = m

		case C.HYPERDATATYPE_MAP_FLOAT_STRING:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_MAP_FLOAT_STRING,
				attr.value, attr.value_sz)
			m := make(MapFloatString)

			for {
				var C_key C.double
				var C_val_string *C.char
				var C_val_size_t C.size_t

				status := C.hyperdex_ds_iterate_map_float_string_next(&C_iter,
					&C_key, &C_val_string, &C_val_size_t)
				if status > 0 {
					m[float64(C_key)] = C.GoStringN(C_val_string, C.int(C_val_size_t))
				} else if status < 0 {
					return nil, fmt.Errorf("Sever sent a corrupted map of floats to strings")
				} else {
					break
				}
			}

			attributes[name] = m

		case C.HYPERDATATYPE_MAP_FLOAT_INT64:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_MAP_FLOAT_INT64,
				attr.value, attr.value_sz)
			m := make(MapFloatInt)

			for {
				var C_key C.double
				var C_val C.int64_t

				status := C.hyperdex_ds_iterate_map_float_int_next(&C_iter,
					&C_key, &C_val)
				if status > 0 {
					m[float64(C_key)] = int64(C_val)
				} else if status < 0 {
					return nil, fmt.Errorf("Sever sent a corrupted map of floats to integers")
				} else {
					break
				}
			}

			attributes[name] = m

		case C.HYPERDATATYPE_MAP_FLOAT_FLOAT:
			C.hyperdex_ds_iterator_init(&C_iter, C.HYPERDATATYPE_MAP_FLOAT_FLOAT,
				attr.value, attr.value_sz)
			m := make(MapFloatFloat)

			for {
				var C_key C.double
				var C_val C.double

				status := C.hyperdex_ds_iterate_map_float_float_next(&C_iter,
					&C_key, &C_val)
				if status > 0 {
					m[float64(C_key)] = float64(C_val)
				} else if status < 0 {
					return nil, fmt.Errorf("Sever sent a corrupted map of floats to floats")
				} else {
					break
				}
			}

			attributes[name] = m

		default:
			return nil, fmt.Errorf("Unknown datatype %d found for attribute `%s` (#%d)", attr.datatype, attr, i)
		}
	}
	return
}

type innerClient struct {
	ptr       *C.struct_hyperdex_client
	mutex     sync.Mutex
	ops       map[int64]chan Error
	searches  map[int64]*cIterator
	closeChan chan bool
}

func innerClientFinalizer(c *innerClient) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	C.hyperdex_client_destroy(c.ptr)
	for _, val := range c.ops {
		close(val)
	}
	for _, val := range c.searches {
		close(val.attrChan)
		close(val.errChan)
	}
}

func (client *innerClient) runForever(errChan chan Error) {
	for {
		select {
		case <-client.closeChan:
			return
		default:
			C.hyperdex_client_block(client.ptr, 250)
			var loop_status C.enum_hyperdex_client_returncode
			client.mutex.Lock()
			reqid := int64(C.hyperdex_client_loop(client.ptr, 0, &loop_status))
			if reqid < 0 && loop_status == TIMEOUT {
				// pass
			} else if reqid < 0 && loop_status == NONEPENDING {
				// pass
			} else if reqid < 0 {
				e := Error{Status(loop_status),
					C.GoString(C.hyperdex_client_error_message(client.ptr)),
					C.GoString(C.hyperdex_client_error_location(client.ptr))}
				errChan <- e
			} else if c, ok := (client.ops)[reqid]; ok {
				e := Error{Status(loop_status),
					C.GoString(C.hyperdex_client_error_message(client.ptr)),
					C.GoString(C.hyperdex_client_error_location(client.ptr))}
				c <- e
				delete(client.ops, reqid)
			} else if cIter, ok := client.searches[reqid]; ok {
				if cIter.status == C.HYPERDEX_CLIENT_SUCCESS {
					attrs, er := client.buildAttributes(cIter.attrs, cIter.attrs_sz)
					if er != nil {
						e := Error{Status(SERVERERROR), er.Error(), ""}
						cIter.errChan <- e
					} else {
						cIter.attrChan <- attrs
					}
					C.hyperdex_client_destroy_attrs(cIter.attrs, cIter.attrs_sz)
				} else if cIter.status == C.HYPERDEX_CLIENT_SEARCHDONE {
					close(cIter.attrChan)
					close(cIter.errChan)
					delete(client.searches, reqid)
				} else {
					e := Error{Status(cIter.status),
						C.GoString(C.hyperdex_client_error_message(client.ptr)),
						C.GoString(C.hyperdex_client_error_location(client.ptr))}
					cIter.errChan <- e
				}
			}
			client.mutex.Unlock()
		}
	}
	panic("Should not be reached: end of infinite loop")
}

func NewClient(host string, port int) (*Client, error, chan Error) {
	numClients := runtime.NumCPU()
	clients := make([]*innerClient, 0, numClients)
	for i := 0; i < numClients; i++ {
		C_client := C.hyperdex_client_create(C.CString(host), C.uint16_t(port))
		if C_client == nil {
			return nil, fmt.Errorf("Could not create HyperDex client (host=%s, port=%d)", host, port), nil
		}
		client := &innerClient{C_client, sync.Mutex{}, map[int64]chan Error{}, map[int64]*cIterator{}, make(chan bool, 1)}
		runtime.SetFinalizer(client, innerClientFinalizer)
		clients = append(clients, client)
	}
	errChan := make(chan Error, 16)
	client := &Client{0, sync.Mutex{}, clients, errChan}
	for i := 0; i < len(clients); i++ {
		go clients[i].runForever(errChan)
	}
	return client, nil, errChan
}

// For every call to NewClient, there must be a call to Destroy.
func (client *Client) Destroy() {
	for i := 0; i < len(client.clients); i++ {
		close(client.clients[i].closeChan)
	}
	close(client.errChan)
}

// Begin Automatically Generated Code
func (client *Client) AsynccallSpacenameKeyStatusAttributes(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_key *C.char, c_key_sz C.size_t, c_status *C.enum_hyperdex_client_returncode, c_attrs **C.struct_hyperdex_client_attribute, c_attrs_sz *C.size_t) int64, spacename string, key Value) (attrs Attributes, err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_key *C.char
	var c_key_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertKey(arena, key, &c_key, &c_key_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	var c_attrs *C.struct_hyperdex_client_attribute
	var c_attrs_sz C.size_t
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_key, c_key_sz, &c_status, &c_attrs, &c_attrs_sz)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	if c_status == SUCCESS {
		var er error
		attrs, er = inner.buildAttributes(c_attrs, c_attrs_sz)
		if er != nil {
			err = &Error{Status(SERVERERROR), er.Error(), ""}
		}
		C.hyperdex_client_destroy_attrs(c_attrs, c_attrs_sz)
	}
	return
}

func (client *Client) AsynccallSpacenameKeyAttributenamesStatusAttributes(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_key *C.char, c_key_sz C.size_t, c_attrnames **C.char, c_attrnames_sz C.size_t, c_status *C.enum_hyperdex_client_returncode, c_attrs **C.struct_hyperdex_client_attribute, c_attrs_sz *C.size_t) int64, spacename string, key Value, attributenames AttributeNames) (attrs Attributes, err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_key *C.char
	var c_key_sz C.size_t
	var c_attrnames **C.char
	var c_attrnames_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertKey(arena, key, &c_key, &c_key_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertAttributenames(arena, attributenames, &c_attrnames, &c_attrnames_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	var c_attrs *C.struct_hyperdex_client_attribute
	var c_attrs_sz C.size_t
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_key, c_key_sz, c_attrnames, c_attrnames_sz, &c_status, &c_attrs, &c_attrs_sz)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	if c_status == SUCCESS {
		var er error
		attrs, er = inner.buildAttributes(c_attrs, c_attrs_sz)
		if er != nil {
			err = &Error{Status(SERVERERROR), er.Error(), ""}
		}
		C.hyperdex_client_destroy_attrs(c_attrs, c_attrs_sz)
	}
	return
}

func (client *Client) AsynccallSpacenameKeyAttributesStatus(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_key *C.char, c_key_sz C.size_t, c_attrs *C.struct_hyperdex_client_attribute, c_attrs_sz C.size_t, c_status *C.enum_hyperdex_client_returncode) int64, spacename string, key Value, attributes Attributes) (err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_key *C.char
	var c_key_sz C.size_t
	var c_attrs *C.struct_hyperdex_client_attribute
	var c_attrs_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertKey(arena, key, &c_key, &c_key_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertAttributes(arena, attributes, &c_attrs, &c_attrs_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_key, c_key_sz, c_attrs, c_attrs_sz, &c_status)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	return
}

func (client *Client) AsynccallSpacenameKeyPredicatesAttributesStatus(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_key *C.char, c_key_sz C.size_t, c_checks *C.struct_hyperdex_client_attribute_check, c_checks_sz C.size_t, c_attrs *C.struct_hyperdex_client_attribute, c_attrs_sz C.size_t, c_status *C.enum_hyperdex_client_returncode) int64, spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_key *C.char
	var c_key_sz C.size_t
	var c_checks *C.struct_hyperdex_client_attribute_check
	var c_checks_sz C.size_t
	var c_attrs *C.struct_hyperdex_client_attribute
	var c_attrs_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertKey(arena, key, &c_key, &c_key_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertPredicates(arena, predicates, &c_checks, &c_checks_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertAttributes(arena, attributes, &c_attrs, &c_attrs_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_key, c_key_sz, c_checks, c_checks_sz, c_attrs, c_attrs_sz, &c_status)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	return
}

func (client *Client) AsynccallSpacenamePredicatesAttributesStatusCount(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_checks *C.struct_hyperdex_client_attribute_check, c_checks_sz C.size_t, c_attrs *C.struct_hyperdex_client_attribute, c_attrs_sz C.size_t, c_status *C.enum_hyperdex_client_returncode, c_count *C.uint64_t) int64, spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_checks *C.struct_hyperdex_client_attribute_check
	var c_checks_sz C.size_t
	var c_attrs *C.struct_hyperdex_client_attribute
	var c_attrs_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertPredicates(arena, predicates, &c_checks, &c_checks_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertAttributes(arena, attributes, &c_attrs, &c_attrs_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	var c_count C.uint64_t
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_checks, c_checks_sz, c_attrs, c_attrs_sz, &c_status, &c_count)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	if c_status == SUCCESS {
		count = uint64(c_count)
	}
	return
}

func (client *Client) AsynccallSpacenameKeyStatus(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_key *C.char, c_key_sz C.size_t, c_status *C.enum_hyperdex_client_returncode) int64, spacename string, key Value) (err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_key *C.char
	var c_key_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertKey(arena, key, &c_key, &c_key_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_key, c_key_sz, &c_status)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	return
}

func (client *Client) AsynccallSpacenameKeyPredicatesStatus(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_key *C.char, c_key_sz C.size_t, c_checks *C.struct_hyperdex_client_attribute_check, c_checks_sz C.size_t, c_status *C.enum_hyperdex_client_returncode) int64, spacename string, key Value, predicates []Predicate) (err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_key *C.char
	var c_key_sz C.size_t
	var c_checks *C.struct_hyperdex_client_attribute_check
	var c_checks_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertKey(arena, key, &c_key, &c_key_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertPredicates(arena, predicates, &c_checks, &c_checks_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_key, c_key_sz, c_checks, c_checks_sz, &c_status)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	return
}

func (client *Client) AsynccallSpacenamePredicatesStatusCount(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_checks *C.struct_hyperdex_client_attribute_check, c_checks_sz C.size_t, c_status *C.enum_hyperdex_client_returncode, c_count *C.uint64_t) int64, spacename string, predicates []Predicate) (count uint64, err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_checks *C.struct_hyperdex_client_attribute_check
	var c_checks_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertPredicates(arena, predicates, &c_checks, &c_checks_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	var c_count C.uint64_t
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_checks, c_checks_sz, &c_status, &c_count)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	if c_status == SUCCESS {
		count = uint64(c_count)
	}
	return
}

func (client *Client) AsynccallSpacenameKeyMapattributesStatus(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_key *C.char, c_key_sz C.size_t, c_mapattrs *C.struct_hyperdex_client_map_attribute, c_mapattrs_sz C.size_t, c_status *C.enum_hyperdex_client_returncode) int64, spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_key *C.char
	var c_key_sz C.size_t
	var c_mapattrs *C.struct_hyperdex_client_map_attribute
	var c_mapattrs_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertKey(arena, key, &c_key, &c_key_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertMapattributes(arena, mapattributes, &c_mapattrs, &c_mapattrs_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_key, c_key_sz, c_mapattrs, c_mapattrs_sz, &c_status)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	return
}

func (client *Client) AsynccallSpacenameKeyPredicatesMapattributesStatus(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_key *C.char, c_key_sz C.size_t, c_checks *C.struct_hyperdex_client_attribute_check, c_checks_sz C.size_t, c_mapattrs *C.struct_hyperdex_client_map_attribute, c_mapattrs_sz C.size_t, c_status *C.enum_hyperdex_client_returncode) int64, spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_key *C.char
	var c_key_sz C.size_t
	var c_checks *C.struct_hyperdex_client_attribute_check
	var c_checks_sz C.size_t
	var c_mapattrs *C.struct_hyperdex_client_map_attribute
	var c_mapattrs_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertKey(arena, key, &c_key, &c_key_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertPredicates(arena, predicates, &c_checks, &c_checks_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertMapattributes(arena, mapattributes, &c_mapattrs, &c_mapattrs_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_key, c_key_sz, c_checks, c_checks_sz, c_mapattrs, c_mapattrs_sz, &c_status)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	return
}

func (client *Client) AsynccallSpacenamePredicatesMapattributesStatusCount(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_checks *C.struct_hyperdex_client_attribute_check, c_checks_sz C.size_t, c_mapattrs *C.struct_hyperdex_client_map_attribute, c_mapattrs_sz C.size_t, c_status *C.enum_hyperdex_client_returncode, c_count *C.uint64_t) int64, spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_checks *C.struct_hyperdex_client_attribute_check
	var c_checks_sz C.size_t
	var c_mapattrs *C.struct_hyperdex_client_map_attribute
	var c_mapattrs_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertPredicates(arena, predicates, &c_checks, &c_checks_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertMapattributes(arena, mapattributes, &c_mapattrs, &c_mapattrs_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	var c_count C.uint64_t
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_checks, c_checks_sz, c_mapattrs, c_mapattrs_sz, &c_status, &c_count)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	if c_status == SUCCESS {
		count = uint64(c_count)
	}
	return
}

func (client *Client) IteratorSpacenamePredicatesStatusAttributes(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_checks *C.struct_hyperdex_client_attribute_check, c_checks_sz C.size_t, c_status *C.enum_hyperdex_client_returncode, c_attrs **C.struct_hyperdex_client_attribute, c_attrs_sz *C.size_t) int64, spacename string, predicates []Predicate) (attrs chan Attributes, errs chan Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_checks *C.struct_hyperdex_client_attribute_check
	var c_checks_sz C.size_t
	var er error
	var c_iter cIterator
	c_iter = cIterator{C.HYPERDEX_CLIENT_GARBAGE, nil, 0, make(chan Attributes, 10), make(chan Error, 10)}
	attrs = c_iter.attrChan
	errs = c_iter.errChan
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err := Error{Status(WRONGTYPE), er.Error(), ""}
		errs<-err
		close(attrs)
		close(errs)
		return
	}
	er = client.convertPredicates(arena, predicates, &c_checks, &c_checks_sz)
	if er != nil {
		err := Error{Status(WRONGTYPE), er.Error(), ""}
		errs<-err
		close(attrs)
		close(errs)
		return
	}
	var err Error
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_checks, c_checks_sz, &c_iter.status, &c_iter.attrs, &c_iter.attrs_sz)
	if reqid >= 0 {
		inner.searches[reqid] = &c_iter
	} else {
		err = Error{Status(c_iter.status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}
	}
	inner.mutex.Unlock()
	if reqid < 0 {
		errs<-err
		close(attrs)
		close(errs)
	}
	return
}

func (client *Client) AsynccallSpacenamePredicatesStatusDescription(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_checks *C.struct_hyperdex_client_attribute_check, c_checks_sz C.size_t, c_status *C.enum_hyperdex_client_returncode, c_description **C.char) int64, spacename string, predicates []Predicate) (desc string, err *Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_checks *C.struct_hyperdex_client_attribute_check
	var c_checks_sz C.size_t
	var er error
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	er = client.convertPredicates(arena, predicates, &c_checks, &c_checks_sz)
	if er != nil {
		err = &Error{Status(WRONGTYPE), er.Error(), ""}
		return
	}
	var c_status C.enum_hyperdex_client_returncode
	var c_description *C.char
	done := make(chan Error)
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_checks, c_checks_sz, &c_status, &c_description)
	if reqid >= 0 {
		inner.ops[reqid] = done
	} else {
		if c_status != SUCCESS {
		err = &Error{Status(c_status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}}
	}
	inner.mutex.Unlock()
	if reqid >= 0 {
		 rz := <-done
		 if c_status != SUCCESS {
		    err = &rz
		    err.Status = Status(c_status)
	}
	}
	if c_status == SUCCESS {
		desc = C.GoString(c_description)
	}
	return
}

func (client *Client) IteratorSpacenamePredicatesSortbyLimitMaxminStatusAttributes(stub func(client *C.struct_hyperdex_client, c_space *C.char, c_checks *C.struct_hyperdex_client_attribute_check, c_checks_sz C.size_t, c_sort_by *C.char, c_limit C.uint64_t, c_maxmin C.int, c_status *C.enum_hyperdex_client_returncode, c_attrs **C.struct_hyperdex_client_attribute, c_attrs_sz *C.size_t) int64, spacename string, predicates []Predicate, sortby string, limit uint32, maxmin string) (attrs chan Attributes, errs chan Error) {
	arena := C.hyperdex_ds_arena_create()
	defer C.hyperdex_ds_arena_destroy(arena)
	var c_space *C.char
	var c_checks *C.struct_hyperdex_client_attribute_check
	var c_checks_sz C.size_t
	var c_sort_by *C.char
	var c_limit C.uint64_t
	var c_maxmin C.int
	var er error
	var c_iter cIterator
	c_iter = cIterator{C.HYPERDEX_CLIENT_GARBAGE, nil, 0, make(chan Attributes, 10), make(chan Error, 10)}
	attrs = c_iter.attrChan
	errs = c_iter.errChan
	er = client.convertSpacename(arena, spacename, &c_space)
	if er != nil {
		err := Error{Status(WRONGTYPE), er.Error(), ""}
		errs<-err
		close(attrs)
		close(errs)
		return
	}
	er = client.convertPredicates(arena, predicates, &c_checks, &c_checks_sz)
	if er != nil {
		err := Error{Status(WRONGTYPE), er.Error(), ""}
		errs<-err
		close(attrs)
		close(errs)
		return
	}
	er = client.convertSortby(arena, sortby, &c_sort_by)
	if er != nil {
		err := Error{Status(WRONGTYPE), er.Error(), ""}
		errs<-err
		close(attrs)
		close(errs)
		return
	}
	er = client.convertLimit(arena, limit, &c_limit)
	if er != nil {
		err := Error{Status(WRONGTYPE), er.Error(), ""}
		errs<-err
		close(attrs)
		close(errs)
		return
	}
	er = client.convertMaxmin(arena, maxmin, &c_maxmin)
	if er != nil {
		err := Error{Status(WRONGTYPE), er.Error(), ""}
		errs<-err
		close(attrs)
		close(errs)
		return
	}
	var err Error
	client.mutex.Lock()
	inner := client.clients[client.counter%uint64(len(client.clients))]
	client.counter++
	client.mutex.Unlock()
	inner.mutex.Lock()
	reqid := stub(inner.ptr, c_space, c_checks, c_checks_sz, c_sort_by, c_limit, c_maxmin, &c_iter.status, &c_iter.attrs, &c_iter.attrs_sz)
	if reqid >= 0 {
		inner.searches[reqid] = &c_iter
	} else {
		err = Error{Status(c_iter.status),
		            C.GoString(C.hyperdex_client_error_message(inner.ptr)),
		            C.GoString(C.hyperdex_client_error_location(inner.ptr))}
	}
	inner.mutex.Unlock()
	if reqid < 0 {
		errs<-err
		close(attrs)
		close(errs)
	}
	return
}

func stub_get(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, status *C.enum_hyperdex_client_returncode, attrs **C.struct_hyperdex_client_attribute, attrs_sz *C.size_t) int64 {
	return int64(C.hyperdex_client_get(client, space, key, key_sz, status, attrs, attrs_sz))
}
func (client *Client) Get(spacename string, key Value) (attrs Attributes, err *Error) {
	return client.AsynccallSpacenameKeyStatusAttributes(stub_get, spacename, key)
}

func stub_get_partial(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrnames **C.char, attrnames_sz C.size_t, status *C.enum_hyperdex_client_returncode, attrs **C.struct_hyperdex_client_attribute, attrs_sz *C.size_t) int64 {
	return int64(C.hyperdex_client_get_partial(client, space, key, key_sz, attrnames, attrnames_sz, status, attrs, attrs_sz))
}
func (client *Client) GetPartial(spacename string, key Value, attributenames AttributeNames) (attrs Attributes, err *Error) {
	return client.AsynccallSpacenameKeyAttributenamesStatusAttributes(stub_get_partial, spacename, key, attributenames)
}

func stub_put(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_put(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) Put(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_put, spacename, key, attributes)
}

func stub_cond_put(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_put(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondPut(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_put, spacename, key, predicates, attributes)
}

func stub_cond_put_or_create(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_put_or_create(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondPutOrCreate(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_put_or_create, spacename, key, predicates, attributes)
}

func stub_group_put(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_put(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupPut(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_put, spacename, predicates, attributes)
}

func stub_put_if_not_exist(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_put_if_not_exist(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) PutIfNotExist(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_put_if_not_exist, spacename, key, attributes)
}

func stub_del(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_del(client, space, key, key_sz, status))
}
func (client *Client) Del(spacename string, key Value) (err *Error) {
	return client.AsynccallSpacenameKeyStatus(stub_del, spacename, key)
}

func stub_cond_del(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_del(client, space, key, key_sz, checks, checks_sz, status))
}
func (client *Client) CondDel(spacename string, key Value, predicates []Predicate) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesStatus(stub_cond_del, spacename, key, predicates)
}

func stub_group_del(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_del(client, space, checks, checks_sz, status, count))
}
func (client *Client) GroupDel(spacename string, predicates []Predicate) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesStatusCount(stub_group_del, spacename, predicates)
}

func stub_atomic_add(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_atomic_add(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) AtomicAdd(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_atomic_add, spacename, key, attributes)
}

func stub_cond_atomic_add(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_atomic_add(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondAtomicAdd(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_atomic_add, spacename, key, predicates, attributes)
}

func stub_group_atomic_add(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_atomic_add(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupAtomicAdd(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_atomic_add, spacename, predicates, attributes)
}

func stub_atomic_sub(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_atomic_sub(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) AtomicSub(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_atomic_sub, spacename, key, attributes)
}

func stub_cond_atomic_sub(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_atomic_sub(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondAtomicSub(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_atomic_sub, spacename, key, predicates, attributes)
}

func stub_group_atomic_sub(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_atomic_sub(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupAtomicSub(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_atomic_sub, spacename, predicates, attributes)
}

func stub_atomic_mul(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_atomic_mul(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) AtomicMul(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_atomic_mul, spacename, key, attributes)
}

func stub_cond_atomic_mul(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_atomic_mul(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondAtomicMul(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_atomic_mul, spacename, key, predicates, attributes)
}

func stub_group_atomic_mul(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_atomic_mul(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupAtomicMul(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_atomic_mul, spacename, predicates, attributes)
}

func stub_atomic_div(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_atomic_div(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) AtomicDiv(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_atomic_div, spacename, key, attributes)
}

func stub_cond_atomic_div(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_atomic_div(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondAtomicDiv(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_atomic_div, spacename, key, predicates, attributes)
}

func stub_group_atomic_div(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_atomic_div(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupAtomicDiv(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_atomic_div, spacename, predicates, attributes)
}

func stub_atomic_mod(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_atomic_mod(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) AtomicMod(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_atomic_mod, spacename, key, attributes)
}

func stub_cond_atomic_mod(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_atomic_mod(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondAtomicMod(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_atomic_mod, spacename, key, predicates, attributes)
}

func stub_group_atomic_mod(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_atomic_mod(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupAtomicMod(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_atomic_mod, spacename, predicates, attributes)
}

func stub_atomic_and(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_atomic_and(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) AtomicAnd(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_atomic_and, spacename, key, attributes)
}

func stub_cond_atomic_and(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_atomic_and(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondAtomicAnd(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_atomic_and, spacename, key, predicates, attributes)
}

func stub_group_atomic_and(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_atomic_and(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupAtomicAnd(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_atomic_and, spacename, predicates, attributes)
}

func stub_atomic_or(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_atomic_or(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) AtomicOr(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_atomic_or, spacename, key, attributes)
}

func stub_cond_atomic_or(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_atomic_or(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondAtomicOr(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_atomic_or, spacename, key, predicates, attributes)
}

func stub_group_atomic_or(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_atomic_or(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupAtomicOr(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_atomic_or, spacename, predicates, attributes)
}

func stub_atomic_xor(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_atomic_xor(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) AtomicXor(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_atomic_xor, spacename, key, attributes)
}

func stub_cond_atomic_xor(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_atomic_xor(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondAtomicXor(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_atomic_xor, spacename, key, predicates, attributes)
}

func stub_group_atomic_xor(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_atomic_xor(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupAtomicXor(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_atomic_xor, spacename, predicates, attributes)
}

func stub_atomic_min(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_atomic_min(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) AtomicMin(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_atomic_min, spacename, key, attributes)
}

func stub_cond_atomic_min(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_atomic_min(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondAtomicMin(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_atomic_min, spacename, key, predicates, attributes)
}

func stub_group_atomic_min(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_atomic_min(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupAtomicMin(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_atomic_min, spacename, predicates, attributes)
}

func stub_atomic_max(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_atomic_max(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) AtomicMax(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_atomic_max, spacename, key, attributes)
}

func stub_cond_atomic_max(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_atomic_max(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondAtomicMax(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_atomic_max, spacename, key, predicates, attributes)
}

func stub_group_atomic_max(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_atomic_max(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupAtomicMax(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_atomic_max, spacename, predicates, attributes)
}

func stub_string_prepend(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_string_prepend(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) StringPrepend(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_string_prepend, spacename, key, attributes)
}

func stub_cond_string_prepend(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_string_prepend(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondStringPrepend(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_string_prepend, spacename, key, predicates, attributes)
}

func stub_group_string_prepend(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_string_prepend(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupStringPrepend(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_string_prepend, spacename, predicates, attributes)
}

func stub_string_append(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_string_append(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) StringAppend(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_string_append, spacename, key, attributes)
}

func stub_cond_string_append(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_string_append(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondStringAppend(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_string_append, spacename, key, predicates, attributes)
}

func stub_group_string_append(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_string_append(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupStringAppend(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_string_append, spacename, predicates, attributes)
}

func stub_list_lpush(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_list_lpush(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) ListLpush(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_list_lpush, spacename, key, attributes)
}

func stub_cond_list_lpush(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_list_lpush(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondListLpush(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_list_lpush, spacename, key, predicates, attributes)
}

func stub_group_list_lpush(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_list_lpush(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupListLpush(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_list_lpush, spacename, predicates, attributes)
}

func stub_list_rpush(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_list_rpush(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) ListRpush(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_list_rpush, spacename, key, attributes)
}

func stub_cond_list_rpush(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_list_rpush(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondListRpush(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_list_rpush, spacename, key, predicates, attributes)
}

func stub_group_list_rpush(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_list_rpush(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupListRpush(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_list_rpush, spacename, predicates, attributes)
}

func stub_set_add(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_set_add(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) SetAdd(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_set_add, spacename, key, attributes)
}

func stub_cond_set_add(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_set_add(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondSetAdd(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_set_add, spacename, key, predicates, attributes)
}

func stub_group_set_add(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_set_add(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupSetAdd(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_set_add, spacename, predicates, attributes)
}

func stub_set_remove(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_set_remove(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) SetRemove(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_set_remove, spacename, key, attributes)
}

func stub_cond_set_remove(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_set_remove(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondSetRemove(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_set_remove, spacename, key, predicates, attributes)
}

func stub_group_set_remove(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_set_remove(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupSetRemove(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_set_remove, spacename, predicates, attributes)
}

func stub_set_intersect(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_set_intersect(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) SetIntersect(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_set_intersect, spacename, key, attributes)
}

func stub_cond_set_intersect(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_set_intersect(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondSetIntersect(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_set_intersect, spacename, key, predicates, attributes)
}

func stub_group_set_intersect(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_set_intersect(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupSetIntersect(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_set_intersect, spacename, predicates, attributes)
}

func stub_set_union(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_set_union(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) SetUnion(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_set_union, spacename, key, attributes)
}

func stub_cond_set_union(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_set_union(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondSetUnion(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_set_union, spacename, key, predicates, attributes)
}

func stub_group_set_union(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_set_union(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupSetUnion(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_set_union, spacename, predicates, attributes)
}

func stub_document_rename(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_document_rename(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) DocumentRename(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_document_rename, spacename, key, attributes)
}

func stub_cond_document_rename(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_document_rename(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondDocumentRename(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_document_rename, spacename, key, predicates, attributes)
}

func stub_group_document_rename(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_document_rename(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupDocumentRename(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_document_rename, spacename, predicates, attributes)
}

func stub_document_unset(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_document_unset(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) DocumentUnset(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_document_unset, spacename, key, attributes)
}

func stub_cond_document_unset(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_document_unset(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondDocumentUnset(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_document_unset, spacename, key, predicates, attributes)
}

func stub_group_document_unset(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_document_unset(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupDocumentUnset(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_document_unset, spacename, predicates, attributes)
}

func stub_map_add(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_add(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapAdd(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_add, spacename, key, mapattributes)
}

func stub_cond_map_add(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_add(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapAdd(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_add, spacename, key, predicates, mapattributes)
}

func stub_group_map_add(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_add(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapAdd(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_add, spacename, predicates, mapattributes)
}

func stub_map_remove(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_remove(client, space, key, key_sz, attrs, attrs_sz, status))
}
func (client *Client) MapRemove(spacename string, key Value, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyAttributesStatus(stub_map_remove, spacename, key, attributes)
}

func stub_cond_map_remove(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_remove(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status))
}
func (client *Client) CondMapRemove(spacename string, key Value, predicates []Predicate, attributes Attributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesAttributesStatus(stub_cond_map_remove, spacename, key, predicates, attributes)
}

func stub_group_map_remove(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, attrs *C.struct_hyperdex_client_attribute, attrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_remove(client, space, checks, checks_sz, attrs, attrs_sz, status, count))
}
func (client *Client) GroupMapRemove(spacename string, predicates []Predicate, attributes Attributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesAttributesStatusCount(stub_group_map_remove, spacename, predicates, attributes)
}

func stub_map_atomic_add(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_atomic_add(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapAtomicAdd(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_atomic_add, spacename, key, mapattributes)
}

func stub_cond_map_atomic_add(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_atomic_add(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapAtomicAdd(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_atomic_add, spacename, key, predicates, mapattributes)
}

func stub_group_map_atomic_add(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_atomic_add(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapAtomicAdd(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_atomic_add, spacename, predicates, mapattributes)
}

func stub_map_atomic_sub(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_atomic_sub(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapAtomicSub(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_atomic_sub, spacename, key, mapattributes)
}

func stub_cond_map_atomic_sub(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_atomic_sub(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapAtomicSub(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_atomic_sub, spacename, key, predicates, mapattributes)
}

func stub_group_map_atomic_sub(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_atomic_sub(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapAtomicSub(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_atomic_sub, spacename, predicates, mapattributes)
}

func stub_map_atomic_mul(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_atomic_mul(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapAtomicMul(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_atomic_mul, spacename, key, mapattributes)
}

func stub_cond_map_atomic_mul(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_atomic_mul(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapAtomicMul(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_atomic_mul, spacename, key, predicates, mapattributes)
}

func stub_group_map_atomic_mul(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_atomic_mul(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapAtomicMul(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_atomic_mul, spacename, predicates, mapattributes)
}

func stub_map_atomic_div(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_atomic_div(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapAtomicDiv(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_atomic_div, spacename, key, mapattributes)
}

func stub_cond_map_atomic_div(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_atomic_div(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapAtomicDiv(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_atomic_div, spacename, key, predicates, mapattributes)
}

func stub_group_map_atomic_div(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_atomic_div(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapAtomicDiv(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_atomic_div, spacename, predicates, mapattributes)
}

func stub_map_atomic_mod(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_atomic_mod(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapAtomicMod(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_atomic_mod, spacename, key, mapattributes)
}

func stub_cond_map_atomic_mod(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_atomic_mod(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapAtomicMod(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_atomic_mod, spacename, key, predicates, mapattributes)
}

func stub_group_map_atomic_mod(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_atomic_mod(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapAtomicMod(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_atomic_mod, spacename, predicates, mapattributes)
}

func stub_map_atomic_and(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_atomic_and(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapAtomicAnd(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_atomic_and, spacename, key, mapattributes)
}

func stub_cond_map_atomic_and(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_atomic_and(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapAtomicAnd(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_atomic_and, spacename, key, predicates, mapattributes)
}

func stub_group_map_atomic_and(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_atomic_and(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapAtomicAnd(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_atomic_and, spacename, predicates, mapattributes)
}

func stub_map_atomic_or(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_atomic_or(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapAtomicOr(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_atomic_or, spacename, key, mapattributes)
}

func stub_cond_map_atomic_or(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_atomic_or(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapAtomicOr(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_atomic_or, spacename, key, predicates, mapattributes)
}

func stub_group_map_atomic_or(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_atomic_or(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapAtomicOr(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_atomic_or, spacename, predicates, mapattributes)
}

func stub_map_atomic_xor(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_atomic_xor(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapAtomicXor(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_atomic_xor, spacename, key, mapattributes)
}

func stub_cond_map_atomic_xor(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_atomic_xor(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapAtomicXor(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_atomic_xor, spacename, key, predicates, mapattributes)
}

func stub_group_map_atomic_xor(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_atomic_xor(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapAtomicXor(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_atomic_xor, spacename, predicates, mapattributes)
}

func stub_map_string_prepend(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_string_prepend(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapStringPrepend(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_string_prepend, spacename, key, mapattributes)
}

func stub_cond_map_string_prepend(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_string_prepend(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapStringPrepend(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_string_prepend, spacename, key, predicates, mapattributes)
}

func stub_group_map_string_prepend(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_string_prepend(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapStringPrepend(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_string_prepend, spacename, predicates, mapattributes)
}

func stub_map_string_append(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_string_append(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapStringAppend(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_string_append, spacename, key, mapattributes)
}

func stub_cond_map_string_append(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_string_append(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapStringAppend(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_string_append, spacename, key, predicates, mapattributes)
}

func stub_group_map_string_append(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_string_append(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapStringAppend(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_string_append, spacename, predicates, mapattributes)
}

func stub_map_atomic_min(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_atomic_min(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapAtomicMin(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_atomic_min, spacename, key, mapattributes)
}

func stub_cond_map_atomic_min(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_atomic_min(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapAtomicMin(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_atomic_min, spacename, key, predicates, mapattributes)
}

func stub_group_map_atomic_min(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_atomic_min(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapAtomicMin(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_atomic_min, spacename, predicates, mapattributes)
}

func stub_map_atomic_max(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_map_atomic_max(client, space, key, key_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) MapAtomicMax(spacename string, key Value, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyMapattributesStatus(stub_map_atomic_max, spacename, key, mapattributes)
}

func stub_cond_map_atomic_max(client *C.struct_hyperdex_client, space *C.char, key *C.char, key_sz C.size_t, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode) int64 {
	return int64(C.hyperdex_client_cond_map_atomic_max(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status))
}
func (client *Client) CondMapAtomicMax(spacename string, key Value, predicates []Predicate, mapattributes MapAttributes) (err *Error) {
	return client.AsynccallSpacenameKeyPredicatesMapattributesStatus(stub_cond_map_atomic_max, spacename, key, predicates, mapattributes)
}

func stub_group_map_atomic_max(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, mapattrs *C.struct_hyperdex_client_map_attribute, mapattrs_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_group_map_atomic_max(client, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count))
}
func (client *Client) GroupMapAtomicMax(spacename string, predicates []Predicate, mapattributes MapAttributes) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesMapattributesStatusCount(stub_group_map_atomic_max, spacename, predicates, mapattributes)
}

func stub_search(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, status *C.enum_hyperdex_client_returncode, attrs **C.struct_hyperdex_client_attribute, attrs_sz *C.size_t) int64 {
	return int64(C.hyperdex_client_search(client, space, checks, checks_sz, status, attrs, attrs_sz))
}
func (client *Client) Search(spacename string, predicates []Predicate) (attrs chan Attributes, errs chan Error) {
	return client.IteratorSpacenamePredicatesStatusAttributes(stub_search, spacename, predicates)
}

func stub_search_describe(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, status *C.enum_hyperdex_client_returncode, description **C.char) int64 {
	return int64(C.hyperdex_client_search_describe(client, space, checks, checks_sz, status, description))
}
func (client *Client) SearchDescribe(spacename string, predicates []Predicate) (desc string, err *Error) {
	return client.AsynccallSpacenamePredicatesStatusDescription(stub_search_describe, spacename, predicates)
}

func stub_sorted_search(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, sort_by *C.char, limit C.uint64_t, maxmin C.int, status *C.enum_hyperdex_client_returncode, attrs **C.struct_hyperdex_client_attribute, attrs_sz *C.size_t) int64 {
	return int64(C.hyperdex_client_sorted_search(client, space, checks, checks_sz, sort_by, limit, maxmin, status, attrs, attrs_sz))
}
func (client *Client) SortedSearch(spacename string, predicates []Predicate, sortby string, limit uint32, maxmin string) (attrs chan Attributes, errs chan Error) {
	return client.IteratorSpacenamePredicatesSortbyLimitMaxminStatusAttributes(stub_sorted_search, spacename, predicates, sortby, limit, maxmin)
}

func stub_count(client *C.struct_hyperdex_client, space *C.char, checks *C.struct_hyperdex_client_attribute_check, checks_sz C.size_t, status *C.enum_hyperdex_client_returncode, count *C.uint64_t) int64 {
	return int64(C.hyperdex_client_count(client, space, checks, checks_sz, status, count))
}
func (client *Client) Count(spacename string, predicates []Predicate) (count uint64, err *Error) {
	return client.AsynccallSpacenamePredicatesStatusCount(stub_count, spacename, predicates)
}

// End Automatically Generated Code
