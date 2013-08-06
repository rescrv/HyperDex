// Copyright (c) 2012, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of HyperDex nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

// C
#include <cassert>

// ruby
extern "C"
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow"
#include <ruby.h>
#pragma GCC diagnostic pop
}

// STL
#include <algorithm>

// e
#include <e/endian.h>
#include <e/slice.h>

// HyperDex
#include "common/macros.h"
#include "datatypes/compare.h"
#include "datatypes/step.h"
#include "client/ruby/type_conversion.h"

extern "C"
{

void
rhc_free_checks(struct hyperclient_attribute_check* chks)
{
    delete[] chks;
}

void
rhc_free_attrs(struct hyperclient_attribute* attrs)
{
    delete[] attrs;
}

void
rhc_free_map_attrs(struct hyperclient_map_attribute* attrs)
{
    delete[] attrs;
}

static void
_elem_to_hyperdex(VALUE obj, enum hyperdatatype* type, VALUE* backing)
{
    const char* tmp_cstr;
    uint32_t sz;
    char buf_s[sizeof(uint32_t)];
    int64_t val_i;
    char buf_i[sizeof(int64_t)];
    double val_d;
    char buf_d[sizeof(double)];

    switch (TYPE(obj))
    {
        case T_SYMBOL:
            tmp_cstr = rb_id2name(SYM2ID(obj));
            sz = strlen(tmp_cstr);
            e::pack32le(sz, buf_s);
            *backing = rb_str_new(buf_s, sizeof(uint32_t));
            rb_str_cat(*backing, tmp_cstr, sz);
            *type = HYPERDATATYPE_STRING;
            break;
        case T_STRING:
            sz = RSTRING_LEN(obj);
            e::pack32le(sz, buf_s);
            *backing = rb_str_new(buf_s, sizeof(uint32_t));
            rb_str_append(*backing, obj);
            *type = HYPERDATATYPE_STRING;
            break;
        case T_FIXNUM:
        case T_BIGNUM:
            val_i = NUM2LL(obj);
            e::pack64le(val_i, buf_i);
            *type = HYPERDATATYPE_INT64;
            *backing = rb_str_new(buf_i, sizeof(int64_t));
            break;
        case T_FLOAT:
            val_d = NUM2DBL(obj);
            e::packdoublele(val_d, buf_d);
            *type = HYPERDATATYPE_FLOAT;
            *backing = rb_str_new(buf_d, sizeof(double));
            break;
        default:
            rb_raise(rb_eTypeError, "Only primitive types can be inside a container");
            break;
    }
}

static void
_list_to_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    VALUE elem = Qnil;
    VALUE elem_backing = Qnil;
    enum hyperdatatype elem_datatype = HYPERDATATYPE_GENERIC;
    *datatype = HYPERDATATYPE_GENERIC;
    *backing = rb_str_new("", 0);

    for (ssize_t i = 0; i < RARRAY_LEN(obj); ++i)
    {
        elem = rb_ary_entry(obj, i);
        _elem_to_hyperdex(elem, &elem_datatype, &elem_backing);

        if (!IS_PRIMITIVE(elem_datatype))
        {
            rb_raise(rb_eTypeError, "Lists may only contain primitive types");
        }

        if (*datatype == HYPERDATATYPE_GENERIC)
        {
            *datatype = elem_datatype;
        }

        if (*datatype != elem_datatype)
        {
            rb_raise(rb_eTypeError, "Lists must contain elements of the same type");
        }

        *datatype = elem_datatype;
        rb_str_append(*backing, elem_backing);
    }

    assert(*datatype == HYPERDATATYPE_GENERIC || IS_PRIMITIVE(*datatype));
    *datatype = CREATE_CONTAINER(HYPERDATATYPE_LIST_GENERIC, *datatype);
}

static bool
_cmp_map_string(const std::pair<VALUE, VALUE>& lhs,
                const std::pair<VALUE, VALUE>& rhs)
{
    VALUE lv = lhs.first;
    VALUE rv = rhs.first;
    e::slice l(StringValuePtr(lv), RSTRING_LEN(lv));
    e::slice r(StringValuePtr(rv), RSTRING_LEN(rv));
    return compare_string(l, r) < 0;
}

static bool
_cmp_map_int64(const std::pair<VALUE, VALUE>& lhs,
               const std::pair<VALUE, VALUE>& rhs)
{
    VALUE lv = lhs.first;
    VALUE rv = rhs.first;
    e::slice l(StringValuePtr(lv), RSTRING_LEN(lv));
    e::slice r(StringValuePtr(rv), RSTRING_LEN(rv));
    return compare_int64(l, r) < 0;
}

static bool
_cmp_map_float(const std::pair<VALUE, VALUE>& lhs,
               const std::pair<VALUE, VALUE>& rhs)
{
    VALUE lv = lhs.first;
    VALUE rv = rhs.first;
    e::slice l(StringValuePtr(lv), RSTRING_LEN(lv));
    e::slice r(StringValuePtr(rv), RSTRING_LEN(rv));
    return compare_float(l, r) < 0;
}

static void
_hash_to_backing(VALUE obj, enum hyperdatatype* datatype, VALUE* backing)
{
    VALUE hash_pairs = Qnil;
    VALUE hash_pair = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    VALUE key_backing = Qnil;
    VALUE val_backing = Qnil;
    enum hyperdatatype key_datatype = HYPERDATATYPE_GENERIC;
    enum hyperdatatype val_datatype = HYPERDATATYPE_GENERIC;
    hash_pairs = rb_funcall(obj, rb_intern("to_a"), 0);
    *datatype = HYPERDATATYPE_MAP_GENERIC;
    *backing = rb_str_new("", 0);
    std::vector<std::pair<VALUE, VALUE> > pairs;

    for (ssize_t i = 0; i < RARRAY_LEN(hash_pairs); ++i)
    {
        hash_pair = rb_ary_entry(hash_pairs, i);
        key = rb_ary_entry(hash_pair, 0);
        val = rb_ary_entry(hash_pair, 1);
        _elem_to_hyperdex(key, &key_datatype, &key_backing);
        _elem_to_hyperdex(val, &val_datatype, &val_backing);

        if (!IS_PRIMITIVE(key_datatype) || !IS_PRIMITIVE(val_datatype))
        {
            rb_raise(rb_eTypeError, "Maps may only contain primitive types");
        }

        if (*datatype == HYPERDATATYPE_MAP_GENERIC)
        {
            *datatype = CREATE_CONTAINER2(HYPERDATATYPE_MAP_GENERIC, key_datatype, val_datatype);
        }

        if (*datatype != CREATE_CONTAINER2(HYPERDATATYPE_MAP_GENERIC, key_datatype, val_datatype))
        {
            rb_raise(rb_eTypeError, "Maps must contain keys/values of the same type");
        }

        pairs.push_back(std::make_pair(key_backing, val_backing));
    }

    if (key_datatype == HYPERDATATYPE_STRING)
    {
        std::sort(pairs.begin(), pairs.end(), _cmp_map_string);
    }
    else if (key_datatype == HYPERDATATYPE_INT64)
    {
        std::sort(pairs.begin(), pairs.end(), _cmp_map_int64);
    }
    else if (key_datatype == HYPERDATATYPE_FLOAT)
    {
        std::sort(pairs.begin(), pairs.end(), _cmp_map_float);
    }
    else
    {
        abort();
    }

    for (size_t i = 0; i < pairs.size(); ++i)
    {
        rb_str_append(*backing, pairs[i].first);
        rb_str_append(*backing, pairs[i].second);
    }
}

void
rhc_ruby_to_hyperdex(VALUE obj, const char** attr, size_t* attr_sz, enum hyperdatatype* type, VALUE* backings)
{
    VALUE backing = Qnil;
    const char* tmp_cstr;
    int64_t val_i;
    char buf_i[sizeof(int64_t)];
    double val_d;
    char buf_d[sizeof(double)];

    switch (TYPE(obj))
    {
        case T_SYMBOL:
            tmp_cstr = rb_id2name(SYM2ID(obj));
            *type = HYPERDATATYPE_STRING;
            backing = rb_str_new2(tmp_cstr);
            break;
        case T_STRING:
            *type = HYPERDATATYPE_STRING;
            backing = obj;
            break;
        case T_FIXNUM:
        case T_BIGNUM:
            val_i = NUM2LL(obj);
            e::pack64le(val_i, buf_i);
            *type = HYPERDATATYPE_INT64;
            backing = rb_str_new(buf_i, sizeof(int64_t));
            break;
        case T_FLOAT:
            val_d = NUM2DBL(obj);
            e::packdoublele(val_d, buf_d);
            *type = HYPERDATATYPE_FLOAT;
            backing = rb_str_new(buf_d, sizeof(double));
            break;
        case T_ARRAY:
            _list_to_backing(obj, type, &backing);
            break;
        case T_HASH:
            _hash_to_backing(obj, type, &backing);
            break;
        default:
            rb_raise(rb_eTypeError, "Cannot convert object to a HyperDex type");
            break;
    }

    if (attr)
    {
        *attr = StringValuePtr(backing);
        *attr_sz = RSTRING_LEN(backing);
    }

    rb_ary_push(*backings, backing);
}

void
rhc_hash_to_checks(VALUE obj, struct hyperclient_attribute_check** chks, size_t* chks_sz, VALUE* backings)
{
    VALUE hash_pairs = Qnil;
    VALUE hash_pair = Qnil;
    VALUE chk_name = Qnil;
    VALUE value = Qnil;

    hash_pairs = rb_funcall(obj, rb_intern("to_a"), 0);
    *chks_sz = RARRAY_LEN(hash_pairs);
    *chks = new (std::nothrow) hyperclient_attribute_check[*chks_sz];

    for (size_t i = 0; i < *chks_sz; ++i)
    {
        hash_pair = rb_ary_entry(hash_pairs, i);
        chk_name = rb_ary_entry(hash_pair, 0);
        value = rb_ary_entry(hash_pair, 1);
        rb_ary_push(*backings, chk_name);
        (*chks)[i].attr = StringValueCStr(chk_name);
        rhc_ruby_to_hyperdex(value, &(*chks)[i].value, &(*chks)[i].value_sz, &(*chks)[i].datatype, backings);
        (*chks)[i].predicate = HYPERPREDICATE_EQUALS;
    }
}

void
rhc_hash_to_attrs(VALUE obj, struct hyperclient_attribute** attrs, size_t* attrs_sz, VALUE* backings)
{
    VALUE hash_pairs = Qnil;
    VALUE hash_pair = Qnil;
    VALUE attr_name = Qnil;
    VALUE value = Qnil;

    hash_pairs = rb_funcall(obj, rb_intern("to_a"), 0);
    *attrs_sz = RARRAY_LEN(hash_pairs);
    *attrs = new (std::nothrow) hyperclient_attribute[*attrs_sz];

    for (size_t i = 0; i < *attrs_sz; ++i)
    {
        hash_pair = rb_ary_entry(hash_pairs, i);
        attr_name = rb_ary_entry(hash_pair, 0);
        value = rb_ary_entry(hash_pair, 1);
        rb_ary_push(*backings, attr_name);
        (*attrs)[i].attr = StringValueCStr(attr_name);
        rhc_ruby_to_hyperdex(value, &(*attrs)[i].value, &(*attrs)[i].value_sz, &(*attrs)[i].datatype, backings);
    }
}

void
rhc_hash_to_map_attrs(VALUE obj, struct hyperclient_map_attribute** attrs, size_t* attrs_sz, VALUE* backings)
{
    VALUE hash_pairs = Qnil;
    VALUE hash_pair = Qnil;
    VALUE attr_name = Qnil;
    VALUE map = Qnil;
    VALUE map_pairs = Qnil;
    VALUE map_pair = Qnil;
    VALUE map_key = Qnil;
    VALUE map_val = Qnil;

    hash_pairs = rb_funcall(obj, rb_intern("to_a"), 0);
    *attrs_sz = 0;

    for (ssize_t i = 0; i < RARRAY_LEN(hash_pairs); ++i)
    {
        hash_pair = rb_ary_entry(hash_pairs, i);
        attr_name = rb_ary_entry(hash_pair, 0);
        map = rb_ary_entry(hash_pair, 1);
        map_pairs = rb_funcall(map, rb_intern("to_a"), 0);

        for (ssize_t j = 0; j < RARRAY_LEN(map_pairs); ++j)
        {
            map_pair = rb_ary_entry(map_pairs, j);
            map_key = rb_ary_entry(map_pair, 0);
            map_val = rb_ary_entry(map_pair, 1);
            ++(*attrs_sz);
        }
    }

    *attrs = new (std::nothrow) hyperclient_map_attribute[*attrs_sz];
    size_t elem = 0;

    for (ssize_t i = 0; i < RARRAY_LEN(hash_pairs); ++i)
    {
        hash_pair = rb_ary_entry(hash_pairs, i);
        attr_name = rb_ary_entry(hash_pair, 0);
        map = rb_ary_entry(hash_pair, 1);
        map_pairs = rb_funcall(map, rb_intern("to_a"), 0);
        rb_ary_push(*backings, attr_name);

        for (ssize_t j = 0; j < RARRAY_LEN(map_pairs); ++j)
        {
            map_pair = rb_ary_entry(map_pairs, j);
            map_key = rb_ary_entry(map_pair, 0);
            map_val = rb_ary_entry(map_pair, 1);
            (*attrs)[i].attr = StringValueCStr(attr_name);
            rhc_ruby_to_hyperdex(map_key, &(*attrs)[i].map_key, &(*attrs)[i].map_key_sz, &(*attrs)[i].map_key_datatype, backings);
            rhc_ruby_to_hyperdex(map_val, &(*attrs)[i].value, &(*attrs)[i].value_sz, &(*attrs)[i].value_datatype, backings);
            ++elem;
        }
    }

    assert(elem == *attrs_sz);
}

static VALUE
_to_string(const e::slice& value)
{
    return rb_str_new(reinterpret_cast<const char*>(value.data()), value.size());
}

static VALUE
_to_int64(const e::slice& value)
{
    int64_t ret = 0;
    char buf[sizeof(int64_t)];

    if (value.size() == sizeof(int64_t))
    {
        e::unpack64le(value.data(), &ret);
    }
    else
    {
        memset(buf, 0, sizeof(int64_t));
        memmove(buf, value.data(), value.size());
        e::unpack64le(buf, &ret);
    }

    return LL2NUM(ret);
}

static VALUE
_to_float(const e::slice& value)
{
    double ret = 0;
    char buf[sizeof(double)];

    if (value.size() >= sizeof(double))
    {
        e::unpackdoublele(value.data(), &ret);
    }
    else
    {
        memset(buf, 0, sizeof(double));
        memmove(buf, value.data(), value.size());
        e::unpackdoublele(buf, &ret);
    }

    return rb_float_new(ret);
}

static bool
_elem_string(const char** ptr, const char* end, VALUE* obj)
{
    e::slice elem;

    if (!step_string(reinterpret_cast<const uint8_t**>(ptr),
                     reinterpret_cast<const uint8_t*>(end), &elem))
    {
        return false;
    }

    *obj = _to_string(elem);
    return true;
}

static bool
_elem_int64(const char** ptr, const char* end, VALUE* obj)
{
    e::slice elem;

    if (!step_int64(reinterpret_cast<const uint8_t**>(ptr),
                    reinterpret_cast<const uint8_t*>(end), &elem))
    {
        return false;
    }

    *obj = _to_int64(elem);
    return true;
}

static bool
_elem_float(const char** ptr, const char* end, VALUE* obj)
{
    e::slice elem;

    if (!step_float(reinterpret_cast<const uint8_t**>(ptr),
                    reinterpret_cast<const uint8_t*>(end), &elem))
    {
        return false;
    }

    *obj = _to_float(elem);
    return true;
}

static VALUE
_attr_to_list(struct hyperclient_attribute* attr,
              bool (*elem)(const char** ptr, const char* end, VALUE* obj))
{
    const char* ptr = attr->value;
    const char* end = attr->value + attr->value_sz;
    VALUE list = Qnil;
    VALUE obj = Qnil;

    while (ptr < end)
    {
        if (!elem(&ptr, end, &obj))
        {
            rb_raise(rb_eRuntimeError, "list is improperly structured (file a bug)");
        }

        rb_ary_push(list, obj);
    }

    if (ptr != end)
    {
        rb_raise(rb_eRuntimeError, "list contains excess data (file a bug)");
    }

    return list;
}

static VALUE
_attr_to_set(struct hyperclient_attribute* attr,
             bool (*elem)(const char** ptr, const char* end, VALUE* obj))
{
    const char* ptr = attr->value;
    const char* end = attr->value + attr->value_sz;
    VALUE set = rb_class_new_instance(0, NULL, rb_path2class("Set"));
    VALUE obj = Qnil;

    while (ptr < end)
    {
        if (!elem(&ptr, end, &obj))
        {
            rb_raise(rb_eRuntimeError, "set is improperly structured (file a bug)");
        }

        rb_funcall(set, rb_intern("add"), 1, obj);
    }

    if (ptr != end)
    {
        rb_raise(rb_eRuntimeError, "set contains excess data (file a bug)");
    }

    return set;
}

static VALUE
_attr_to_map(struct hyperclient_attribute* attr,
             bool (*elem_key)(const char** ptr, const char* end, VALUE* obj),
             bool (*elem_val)(const char** ptr, const char* end, VALUE* obj))
{
    const char* ptr = attr->value;
    const char* end = attr->value + attr->value_sz;
    VALUE map = rb_hash_new();
    VALUE key = Qnil;
    VALUE val = Qnil;

    while (ptr < end)
    {
        if (!elem_key(&ptr, end, &key))
        {
            rb_raise(rb_eRuntimeError, "map is improperly structured (file a bug)");
        }

        if (!elem_val(&ptr, end, &val))
        {
            rb_raise(rb_eRuntimeError, "map is improperly structured (file a bug)");
        }

        rb_hash_aset(map, key, val);
    }

    if (ptr != end)
    {
        rb_raise(rb_eRuntimeError, "map contains excess data (file a bug)");
    }

    return map;
}

VALUE
rhc_attrs_to_hash(struct hyperclient_attribute* attrs, size_t attrs_sz)
{
    VALUE ret = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;

    ret = rb_hash_new();

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        key = rb_str_new2(attrs[i].attr);

        switch (attrs[i].datatype)
        {
            case HYPERDATATYPE_STRING:
                val = _to_string(e::slice(attrs[i].value, attrs[i].value_sz));
                break;
            case HYPERDATATYPE_INT64:
                val = _to_int64(e::slice(attrs[i].value, attrs[i].value_sz));
                break;
            case HYPERDATATYPE_FLOAT:
                val = _to_float(e::slice(attrs[i].value, attrs[i].value_sz));
                break;
            case HYPERDATATYPE_LIST_STRING:
                val = _attr_to_list(attrs + i, _elem_string);
                break;
            case HYPERDATATYPE_LIST_INT64:
                val = _attr_to_list(attrs + i, _elem_int64);
                break;
            case HYPERDATATYPE_LIST_FLOAT:
                val = _attr_to_list(attrs + i, _elem_float);
                break;
            case HYPERDATATYPE_SET_STRING:
                val = _attr_to_set(attrs + i, _elem_string);
                break;
            case HYPERDATATYPE_SET_INT64:
                val = _attr_to_set(attrs + i, _elem_int64);
                break;
            case HYPERDATATYPE_SET_FLOAT:
                val = _attr_to_set(attrs + i, _elem_float);
                break;
            case HYPERDATATYPE_MAP_STRING_STRING:
                val = _attr_to_map(attrs + i, _elem_string, _elem_string);
                break;
            case HYPERDATATYPE_MAP_STRING_INT64:
                val = _attr_to_map(attrs + i, _elem_string, _elem_int64);
                break;
            case HYPERDATATYPE_MAP_STRING_FLOAT:
                val = _attr_to_map(attrs + i, _elem_string, _elem_float);
                break;
            case HYPERDATATYPE_MAP_INT64_STRING:
                val = _attr_to_map(attrs + i, _elem_int64, _elem_string);
                break;
            case HYPERDATATYPE_MAP_INT64_INT64:
                val = _attr_to_map(attrs + i, _elem_int64, _elem_int64);
                break;
            case HYPERDATATYPE_MAP_INT64_FLOAT:
                val = _attr_to_map(attrs + i, _elem_int64, _elem_float);
                break;
            case HYPERDATATYPE_MAP_FLOAT_STRING:
                val = _attr_to_map(attrs + i, _elem_float, _elem_string);
                break;
            case HYPERDATATYPE_MAP_FLOAT_INT64:
                val = _attr_to_map(attrs + i, _elem_float, _elem_int64);
                break;
            case HYPERDATATYPE_MAP_FLOAT_FLOAT:
                val = _attr_to_map(attrs + i, _elem_float, _elem_float);
                break;
            case HYPERDATATYPE_GENERIC:
            case HYPERDATATYPE_LIST_GENERIC:
            case HYPERDATATYPE_SET_GENERIC:
            case HYPERDATATYPE_MAP_GENERIC:
            case HYPERDATATYPE_MAP_STRING_KEYONLY:
            case HYPERDATATYPE_MAP_INT64_KEYONLY:
            case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
            case HYPERDATATYPE_GARBAGE:
            default:
                rb_raise(rb_eRuntimeError, "trying to deserialize invalid type (file a bug)");
        }

        rb_hash_aset(ret, key, val);
    }

    return ret;
}

}
