/* Copyright (c) 2013, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of HyperDex nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* C */
#include <assert.h>

/* Ruby */
#include <ruby.h>
#include <intern.h>
#include <st.h>

/* HyperDex */
#include "hyperdex/client.h"
#include "hyperdex/datastructures.h"

extern VALUE mod_hyperdex;
static VALUE mod_hyperdex_client;
static VALUE class_exception;
static VALUE Set;
static VALUE class_deferred;
static VALUE class_iterator;
static VALUE class_client;
static VALUE class_predicate;
static VALUE class_equals;
static VALUE class_lessequal;
static VALUE class_greaterequal;
static VALUE class_lessthan;
static VALUE class_greaterthan;
static VALUE class_range;
static VALUE class_regex;
static VALUE class_lengthequals;
static VALUE class_lengthlessequal;
static VALUE class_lengthgreaterequal;
static VALUE class_contains;

/******************************* Error Handling *******************************/

void
hyperdex_ruby_out_of_memory()
{
    rb_raise(rb_eNoMemError, "failed to allocate memory");
}

static VALUE
hyperdex_ruby_client_create_exception(enum hyperdex_client_returncode status,
                                      const char* error_message)
{
    const char* symbol = hyperdex_client_returncode_to_string(status);
    VALUE exception;
    exception = rb_exc_new2(class_exception, error_message);
    rb_iv_set(exception, "@status", rb_uint_new(status));
    rb_iv_set(exception, "@symbol", rb_str_new2(symbol));
    return exception;
}

void
hyperdex_ruby_client_throw_exception(enum hyperdex_client_returncode status,
                                     const char* error_message)
{
    VALUE exception = hyperdex_ruby_client_create_exception(status, error_message);
    rb_exc_raise(exception);
    abort(); /* unreachable? */
}

/******************************* Predicate class ******************************/

struct hyperdex_ruby_client_predicate_inner
{
    VALUE v;
    enum hyperpredicate predicate;
};

struct hyperdex_ruby_client_predicate
{
    size_t num_checks;
    struct hyperdex_ruby_client_predicate_inner checks[1];
};

/********************************** Ruby -> C *********************************/

static const char*
hyperdex_ruby_client_convert_cstring(VALUE x, const char* error_message)
{
    if (TYPE(x) == T_SYMBOL)
    {
        return rb_id2name(SYM2ID(x));
    }
    else if (TYPE(x) == T_STRING)
    {
        return StringValueCStr(x);
    }
    else
    {
        rb_exc_raise(rb_exc_new2(rb_eTypeError, error_message));
        abort(); /* unreachable? */
    }
}

typedef int (*elem_string_fptr)(void*, const char*, size_t, enum hyperdex_ds_returncode*);
typedef int (*elem_int_fptr)(void*, int64_t, enum hyperdex_ds_returncode*);
typedef int (*elem_float_fptr)(void*, double, enum hyperdex_ds_returncode*);

#define HDRB_HANDLE_ELEM_ERROR(X, TYPE) \
    switch (X) \
    { \
        case HYPERDEX_DS_NOMEM: \
            hyperdex_ruby_out_of_memory(); \
        case HYPERDEX_DS_MIXED_TYPES: \
            rb_raise(rb_eTypeError, "Cannot add " TYPE " to a heterogenous container"); \
        case HYPERDEX_DS_SUCCESS: \
        case HYPERDEX_DS_STRING_TOO_LONG: \
        case HYPERDEX_DS_WRONG_STATE: \
        default: \
            rb_raise(rb_eTypeError, "Cannot convert " TYPE " to a HyperDex type"); \
    }

static void
hyperdex_ruby_client_convert_elem(VALUE e,
                                  void* x,
                                  elem_string_fptr f_string,
                                  elem_int_fptr f_int,
                                  elem_float_fptr f_float)
{
    enum hyperdex_ds_returncode error;
    const char* tmp_str;
    size_t tmp_str_sz;
    int64_t tmp_i;
    double tmp_d;

    switch (TYPE(e))
    {
        case T_SYMBOL:
            tmp_str = rb_id2name(SYM2ID(e));
            tmp_str_sz = strlen(tmp_str);
            if (f_string(x, tmp_str, tmp_str_sz, &error) < 0)
            {
                HDRB_HANDLE_ELEM_ERROR(error, "string");
            }
            break;
        case T_STRING:
            tmp_str = StringValuePtr(e);
            tmp_str_sz = RSTRING_LEN(e);
            if (f_string(x, tmp_str, tmp_str_sz, &error) < 0)
            {
                HDRB_HANDLE_ELEM_ERROR(error, "string");
            }
            break;
        case T_FIXNUM:
        case T_BIGNUM:
            tmp_i = NUM2LL(e);
            if (f_int(x, tmp_i, &error) < 0)
            {
                HDRB_HANDLE_ELEM_ERROR(error, "integer");
            }
            break;
        case T_FLOAT:
            tmp_d = NUM2DBL(e);
            if (f_float(x, tmp_d, &error) < 0)
            {
                HDRB_HANDLE_ELEM_ERROR(error, "float");
            }
            break;
        default:
            rb_raise(rb_eTypeError, "Cannot convert object to a HyperDex container element");
            break;
    }
}

static void
hyperdex_ruby_client_convert_list(struct hyperdex_ds_arena* arena,
                                  VALUE x,
                                  const char** value,
                                  size_t* value_sz,
                                  enum hyperdatatype* datatype)
{
    struct hyperdex_ds_list* list;
    enum hyperdex_ds_returncode error;
    VALUE entry;
    ssize_t i = 0;

    list = hyperdex_ds_allocate_list(arena);

    if (!list)
    {
        hyperdex_ruby_out_of_memory();
    }

    for (i = 0; i < RARRAY_LEN(x); ++i)
    {
        entry = rb_ary_entry(x, i);
        hyperdex_ruby_client_convert_elem(entry, list,
                (elem_string_fptr) hyperdex_ds_list_append_string,
                (elem_int_fptr) hyperdex_ds_list_append_int,
                (elem_float_fptr) hyperdex_ds_list_append_float);
    }

    if (hyperdex_ds_list_finalize(list, &error, value, value_sz, datatype) < 0)
    {
        hyperdex_ruby_out_of_memory();
    }
}

static void
hyperdex_ruby_client_convert_set(struct hyperdex_ds_arena* arena,
                                 VALUE _x,
                                 const char** value,
                                 size_t* value_sz,
                                 enum hyperdatatype* datatype)
{
    struct hyperdex_ds_set* set;
    enum hyperdex_ds_returncode error;
    VALUE entry;
    ssize_t i = 0;
    VALUE x = rb_funcall(_x, rb_intern("to_a"), 0);

    set = hyperdex_ds_allocate_set(arena);

    if (!set)
    {
        hyperdex_ruby_out_of_memory();
    }

    for (i = 0; i < RARRAY_LEN(x); ++i)
    {
        entry = rb_ary_entry(x, i);
        hyperdex_ruby_client_convert_elem(entry, set,
                (elem_string_fptr) hyperdex_ds_set_insert_string,
                (elem_int_fptr) hyperdex_ds_set_insert_int,
                (elem_float_fptr) hyperdex_ds_set_insert_float);
    }

    if (hyperdex_ds_set_finalize(set, &error, value, value_sz, datatype) < 0)
    {
        hyperdex_ruby_out_of_memory();
    }
}

static void
hyperdex_ruby_client_convert_map(struct hyperdex_ds_arena* arena,
                                 VALUE _x,
                                 const char** value,
                                 size_t* value_sz,
                                 enum hyperdatatype* datatype)
{
    struct hyperdex_ds_map* map;
    enum hyperdex_ds_returncode error;
    VALUE entry;
    VALUE key;
    VALUE val;
    ssize_t i = 0;
    VALUE x = rb_funcall(_x, rb_intern("to_a"), 0);

    map = hyperdex_ds_allocate_map(arena);

    if (!map)
    {
        hyperdex_ruby_out_of_memory();
    }

    for (i = 0; i < RARRAY_LEN(x); ++i)
    {
        entry = rb_ary_entry(x, i);
        key = rb_ary_entry(entry, 0);
        val = rb_ary_entry(entry, 1);
        hyperdex_ruby_client_convert_elem(key, map,
                (elem_string_fptr) hyperdex_ds_map_insert_key_string,
                (elem_int_fptr) hyperdex_ds_map_insert_key_int,
                (elem_float_fptr) hyperdex_ds_map_insert_key_float);
        hyperdex_ruby_client_convert_elem(val, map,
                (elem_string_fptr) hyperdex_ds_map_insert_val_string,
                (elem_int_fptr) hyperdex_ds_map_insert_val_int,
                (elem_float_fptr) hyperdex_ds_map_insert_val_float);
    }

    if (hyperdex_ds_map_finalize(map, &error, value, value_sz, datatype) < 0)
    {
        hyperdex_ruby_out_of_memory();
    }
}

static void
hyperdex_ruby_client_convert_type(struct hyperdex_ds_arena* arena,
                                  VALUE x,
                                  const char** value,
                                  size_t* value_sz,
                                  enum hyperdatatype* datatype)
{
    enum hyperdex_ds_returncode error;
    const char* tmp_str;
    size_t tmp_str_sz;
    int64_t tmp_i;
    double tmp_d;

    switch (TYPE(x))
    {
        case T_SYMBOL:
            tmp_str = rb_id2name(SYM2ID(x));
            tmp_str_sz = strlen(tmp_str);

            if (hyperdex_ds_copy_string(arena, tmp_str, tmp_str_sz,
                                        &error, value, value_sz) < 0)
            {
                hyperdex_ruby_out_of_memory();
            }

            *datatype = HYPERDATATYPE_STRING;
            break;
        case T_STRING:
            tmp_str = StringValuePtr(x);
            tmp_str_sz = RSTRING_LEN(x);

            if (hyperdex_ds_copy_string(arena, tmp_str, tmp_str_sz,
                                        &error, value, value_sz) < 0)
            {
                hyperdex_ruby_out_of_memory();
            }

            *datatype = HYPERDATATYPE_STRING;
            break;
        case T_FIXNUM:
        case T_BIGNUM:
            tmp_i = NUM2LL(x);

            if (hyperdex_ds_copy_int(arena, tmp_i,
                                     &error, value, value_sz) < 0)
            {
                hyperdex_ruby_out_of_memory();
            }

            *datatype = HYPERDATATYPE_INT64;
            break;
        case T_FLOAT:
            tmp_d = NUM2DBL(x);

            if (hyperdex_ds_copy_float(arena, tmp_d,
                                       &error, value, value_sz) < 0)
            {
                hyperdex_ruby_out_of_memory();
            }

            *datatype = HYPERDATATYPE_FLOAT;
            break;
        case T_ARRAY:
            hyperdex_ruby_client_convert_list(arena, x, value, value_sz, datatype);
            break;
        case T_HASH:
            hyperdex_ruby_client_convert_map(arena, x, value, value_sz, datatype);
            break;
        case T_OBJECT:
            if (!rb_obj_is_kind_of(x, Set) == Qtrue)
            {
                rb_raise(rb_eTypeError, "Cannot convert object to a HyperDex type");
                break;
            }

            hyperdex_ruby_client_convert_set(arena, x, value, value_sz, datatype);
            break;
        default:
            rb_raise(rb_eTypeError, "Cannot convert object to a HyperDex type");
            break;
    }
}

static void
hyperdex_ruby_client_convert_attributes(struct hyperdex_ds_arena* arena,
                                        VALUE x,
                                        const struct hyperdex_client_attribute** _attrs,
                                        size_t* _attrs_sz)
{
    VALUE hash_pairs = Qnil;
    VALUE hash_pair = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    struct hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    ssize_t i;

    if (TYPE(x) != T_HASH)
    {
        rb_exc_raise(rb_exc_new2(rb_eTypeError, "Attributes must be specified as a hash"));
        abort(); /* unreachable? */
    }

    hash_pairs = rb_funcall(x, rb_intern("to_a"), 0);
    attrs_sz = RARRAY_LEN(hash_pairs);
    attrs = hyperdex_ds_allocate_attribute(arena, attrs_sz);

    if (!attrs)
    {
        hyperdex_ruby_out_of_memory();
    }

    *_attrs = attrs;
    *_attrs_sz = attrs_sz;

    for (i = 0; i < RARRAY_LEN(hash_pairs); ++i)
    {
        hash_pair = rb_ary_entry(hash_pairs, i);
        key = rb_ary_entry(hash_pair, 0);
        val = rb_ary_entry(hash_pair, 1);

        attrs[i].attr = hyperdex_ruby_client_convert_cstring(key, "Attribute name must be a string or symbol");
        hyperdex_ruby_client_convert_type(arena, val, &attrs[i].value, &attrs[i].value_sz, &attrs[i].datatype);
    }
}

static void
hyperdex_ruby_client_convert_key(struct hyperdex_ds_arena* arena,
                                 VALUE x,
                                 const char** key,
                                 size_t* key_sz)
{
    enum hyperdatatype datatype;
    hyperdex_ruby_client_convert_type(arena, x, key, key_sz, &datatype);
}

static void
hyperdex_ruby_client_convert_limit(struct hyperdex_ds_arena* arena,
                                   VALUE x,
                                   uint64_t* limit)
{
    *limit = rb_num2ulong(x);
    (void) arena;
}

static void
hyperdex_ruby_client_convert_mapattributes(struct hyperdex_ds_arena* arena,
                                           VALUE x,
                                           const struct hyperdex_client_map_attribute** _mapattrs,
                                           size_t* _mapattrs_sz)
{
    VALUE outer_pairs = Qnil;
    VALUE outer_pair = Qnil;
    VALUE inner_pairs = Qnil;
    VALUE inner_pair = Qnil;
    VALUE attr = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    struct hyperdex_client_map_attribute* mapattrs = NULL;
    size_t mapattrs_sz = 0;
    size_t mapattrs_idx = 0;
    ssize_t i = 0;
    size_t j = 0;

    if (TYPE(x) != T_HASH)
    {
        rb_exc_raise(rb_exc_new2(rb_eTypeError, "Map attributes must be specified as a hash"));
        abort(); /* unreachable? */
    }

    outer_pairs = rb_funcall(x, rb_intern("to_a"), 0);

    for (i = 0; i < RARRAY_LEN(outer_pairs); ++i)
    {
        outer_pair = rb_ary_entry(outer_pairs, i);
        inner_pairs = rb_ary_entry(outer_pair, 1);

        if (TYPE(inner_pairs) != T_HASH)
        {
            rb_exc_raise(rb_exc_new2(rb_eTypeError, "Map attributes must be specified as a hash"));
            abort(); /* unreachable? */
        }

        mapattrs_sz += RHASH_SIZE(inner_pairs);
    }

    mapattrs = hyperdex_ds_allocate_map_attribute(arena, mapattrs_sz);

    if (!mapattrs)
    {
        hyperdex_ruby_out_of_memory();
    }

    *_mapattrs = mapattrs;
    *_mapattrs_sz = mapattrs_sz;

    for (i = 0; i < RARRAY_LEN(outer_pairs); ++i)
    {
        outer_pair = rb_ary_entry(outer_pairs, i);
        attr = rb_ary_entry(outer_pair, 0);
        inner_pairs = rb_ary_entry(outer_pair, 1);
        inner_pairs = rb_funcall(inner_pairs, rb_intern("to_a"), 0);

        for (j = 0; RARRAY_LEN(inner_pairs); ++j)
        {
            inner_pair = rb_ary_entry(inner_pairs, j);
            key = rb_ary_entry(inner_pair, 0);
            val = rb_ary_entry(inner_pair, 1);
            mapattrs[mapattrs_idx].attr = hyperdex_ruby_client_convert_cstring(attr, "Attribute name must be a string or symbol");
            hyperdex_ruby_client_convert_type(arena, key,
                                              &mapattrs[mapattrs_idx].map_key,
                                              &mapattrs[mapattrs_idx].map_key_sz,
                                              &mapattrs[mapattrs_idx].map_key_datatype);
            hyperdex_ruby_client_convert_type(arena, val,
                                              &mapattrs[mapattrs_idx].value,
                                              &mapattrs[mapattrs_idx].value_sz,
                                              &mapattrs[mapattrs_idx].value_datatype);
            ++mapattrs_idx;
        }
    }
}

static void
hyperdex_ruby_client_convert_maxmin(struct hyperdex_ds_arena* arena,
                                    VALUE x,
                                    int* maxmin)
{
    *maxmin = x == Qtrue ? 1: 0;
    (void) arena;
}

static size_t
hyperdex_ruby_client_estimate_predicate_size(VALUE x)
{
    VALUE pred = Qnil;
    struct hyperdex_ruby_client_predicate* p = NULL;
    ssize_t i = 0;
    size_t sum = 0;

    if (TYPE(x) == T_DATA &&
        rb_obj_is_kind_of(x, class_predicate) == Qtrue)
    {
        Data_Get_Struct(x, struct hyperdex_ruby_client_predicate, p);
        return p->num_checks;
    }
    else if (TYPE(x) == T_ARRAY &&
             RARRAY_LEN(x) > 0 &&
             rb_obj_is_kind_of(rb_ary_entry(x, 0), class_predicate) == Qtrue)
    {
        for (i = 0; i < RARRAY_LEN(x); ++i)
        {
            pred = rb_ary_entry(x, i);

            if (TYPE(pred) != T_DATA ||
                rb_obj_is_kind_of(pred, class_predicate) != Qtrue)
            {
                rb_raise(rb_eTypeError, "Cannot convert predicate to a HyperDex type");
                return 0;
            }

            Data_Get_Struct(pred, struct hyperdex_ruby_client_predicate, p);
            sum += p->num_checks;
        }

        return sum;
    }
    else
    {
        return 1;
    }
}

static size_t
hyperdex_ruby_client_convert_predicate(struct hyperdex_ds_arena* arena,
                                       const char* attr,
                                       VALUE x,
                                       struct hyperdex_client_attribute_check* checks,
                                       size_t checks_idx)
{
    VALUE pred = Qnil;
    struct hyperdex_ruby_client_predicate* p = NULL;
    size_t i = 0;

    if (TYPE(x) == T_DATA &&
        rb_obj_is_kind_of(x, class_predicate) == Qtrue)
    {
        Data_Get_Struct(x, struct hyperdex_ruby_client_predicate, p);

        for (i = 0; i < p->num_checks; ++i)
        {
            checks[checks_idx + i].attr = attr;
            checks[checks_idx + i].predicate = p->checks[i].predicate;
            hyperdex_ruby_client_convert_type(arena, p->checks[i].v,
                                              &checks[checks_idx + i].value,
                                              &checks[checks_idx + i].value_sz,
                                              &checks[checks_idx + i].datatype);
        }

        return checks_idx + p->num_checks;
    }
    else if (TYPE(x) == T_ARRAY &&
             RARRAY_LEN(x) > 0 &&
             rb_obj_is_kind_of(rb_ary_entry(x, 0), class_predicate) == Qtrue)
    {
        for (i = 0; i < (size_t)RARRAY_LEN(x); ++i)
        {
            pred = rb_ary_entry(x, i);
            assert(TYPE(pred) == T_DATA);
            assert(rb_obj_is_kind_of(pred, class_predicate) == Qtrue);
            checks_idx = hyperdex_ruby_client_convert_predicate(arena, attr, pred, checks, checks_idx);
        }

        return checks_idx;
    }
    else
    {
        checks[checks_idx].attr = attr;
        checks[checks_idx].predicate = HYPERPREDICATE_EQUALS,
        hyperdex_ruby_client_convert_type(arena, x,
                                          &checks[checks_idx].value,
                                          &checks[checks_idx].value_sz,
                                          &checks[checks_idx].datatype);
        return checks_idx + 1;
    }
}

static void
hyperdex_ruby_client_convert_predicates(struct hyperdex_ds_arena* arena,
                                        VALUE x,
                                        const struct hyperdex_client_attribute_check** _checks,
                                        size_t* _checks_sz)
{
    VALUE hash_pairs = Qnil;
    VALUE hash_pair = Qnil;
    VALUE key = Qnil;
    VALUE val = Qnil;
    struct hyperdex_client_attribute_check* checks = NULL;
    size_t checks_sz = 0;
    size_t checks_idx = 0;
    ssize_t i = 0;
    const char* attr;

    if (TYPE(x) != T_HASH)
    {
        rb_exc_raise(rb_exc_new2(rb_eTypeError, "Predicates must be specified as a hash"));
        abort(); /* unreachable? */
    }

    hash_pairs = rb_funcall(x, rb_intern("to_a"), 0);

    /* figure out how many checks to allocate */
    for (i = 0; i < RARRAY_LEN(hash_pairs); ++i)
    {
        hash_pair = rb_ary_entry(hash_pairs, i);
        val = rb_ary_entry(hash_pair, 1);
        checks_sz += hyperdex_ruby_client_estimate_predicate_size(val);
    }

    checks = hyperdex_ds_allocate_attribute_check(arena, checks_sz);

    if (!checks)
    {
        hyperdex_ruby_out_of_memory();
    }

    *_checks = checks;
    *_checks_sz = checks_sz;
    checks_idx = 0;

    /* turn the predicate into checks */
    for (i = 0; i < RARRAY_LEN(hash_pairs); ++i)
    {
        hash_pair = rb_ary_entry(hash_pairs, i);
        key = rb_ary_entry(hash_pair, 0);
        val = rb_ary_entry(hash_pair, 1);

        attr = hyperdex_ruby_client_convert_cstring(key, "Attribute name must be a string or symbol");
        checks_idx = hyperdex_ruby_client_convert_predicate(arena, attr, val, checks, checks_idx);
    }
}

static void
hyperdex_ruby_client_convert_attributenames(struct hyperdex_ds_arena* arena,
                                            VALUE x,
                                            const char*** names, size_t* names_sz)
{
    size_t i = 0;
    *names_sz = RARRAY_LEN(x);
    *names = hyperdex_ds_malloc(arena, sizeof(char*) * (*names_sz));

    if (!(*names))
    {
        hyperdex_ruby_out_of_memory();
    }

    for (i = 0; i < *names_sz; ++i)
    {
        (*names)[i] = hyperdex_ruby_client_convert_cstring(rb_ary_entry(x, i),
                "Attribute name must be a string or symbol");
    }
}

static void
hyperdex_ruby_client_convert_sortby(struct hyperdex_ds_arena* arena,
                                    VALUE x,
                                    const char** sortby)
{
    *sortby = hyperdex_ruby_client_convert_cstring(x, "sortby must be a string or symbol");
    (void) arena;
}

static void
hyperdex_ruby_client_convert_spacename(struct hyperdex_ds_arena* arena,
                                       VALUE x,
                                       const char** spacename)
{
    *spacename = hyperdex_ruby_client_convert_cstring(x, "spacename must be a string or symbol");
    (void) arena;
}

/********************************** C -> Ruby *********************************/

static VALUE
hyperdex_ruby_client_build_attribute(const struct hyperdex_client_attribute* attrs)
{
    struct hyperdex_ds_iterator iter;
    const char* tmp_str = NULL;
    size_t tmp_str_sz = 0;
    const char* tmp_str2 = NULL;
    size_t tmp_str2_sz = 0;
    int64_t tmp_i = 0;
    int64_t tmp_i2 = 0;
    double tmp_d = 0;
    double tmp_d2 = 0;
    int result = 0;
    VALUE ret = Qnil;
    VALUE tmp = Qnil;
    VALUE tmp2 = Qnil;

    switch (attrs->datatype)
    {
        case HYPERDATATYPE_STRING:
            return rb_str_new(attrs->value, attrs->value_sz);
        case HYPERDATATYPE_INT64:
            if (hyperdex_ds_unpack_int(attrs->value, attrs->value_sz, &tmp_i) < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed int");
            }

            return LL2NUM(tmp_i);
        case HYPERDATATYPE_FLOAT:
            if (hyperdex_ds_unpack_float(attrs->value, attrs->value_sz, &tmp_d) < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed float");
            }

            return rb_float_new(tmp_d);
        case HYPERDATATYPE_DOCUMENT:
            hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_WRONGTYPE, "Ruby bindings do not support JSON objects");
        case HYPERDATATYPE_LIST_STRING:
            ret = rb_ary_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_list_string_next(&iter, &tmp_str, &tmp_str_sz)) > 0)
            {
                tmp = rb_str_new(tmp_str, tmp_str_sz);
                rb_ary_push(ret, tmp);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed list(string)");
            }

            return ret;
        case HYPERDATATYPE_LIST_INT64:
            ret = rb_ary_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_list_int_next(&iter, &tmp_i)) > 0)
            {
                tmp = LL2NUM(tmp_i);
                rb_ary_push(ret, tmp);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed list(int)");
            }

            return ret;
        case HYPERDATATYPE_LIST_FLOAT:
            ret = rb_ary_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_list_float_next(&iter, &tmp_d)) > 0)
            {
                tmp = rb_float_new(tmp_d);
                rb_ary_push(ret, tmp);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed list(float)");
            }

            return ret;
        case HYPERDATATYPE_SET_STRING:
            ret = rb_ary_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_set_string_next(&iter, &tmp_str, &tmp_str_sz)) > 0)
            {
                tmp = rb_str_new(tmp_str, tmp_str_sz);
                rb_ary_push(ret, tmp);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed set(string)");
            }

            return rb_class_new_instance(1, &ret, Set);
        case HYPERDATATYPE_SET_INT64:
            ret = rb_ary_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_set_int_next(&iter, &tmp_i)) > 0)
            {
                tmp = LL2NUM(tmp_i);
                rb_ary_push(ret, tmp);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed set(int)");
            }

            return rb_class_new_instance(1, &ret, Set);
        case HYPERDATATYPE_SET_FLOAT:
            ret = rb_ary_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_set_float_next(&iter, &tmp_d)) > 0)
            {
                tmp = rb_float_new(tmp_d);
                rb_ary_push(ret, tmp);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed set(float)");
            }

            return rb_class_new_instance(1, &ret, Set);
        case HYPERDATATYPE_MAP_STRING_STRING:
            ret = rb_hash_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_map_string_string_next(&iter, &tmp_str, &tmp_str_sz, &tmp_str2, &tmp_str2_sz)) > 0)
            {
                tmp = rb_str_new(tmp_str, tmp_str_sz);
                tmp2 = rb_str_new(tmp_str2, tmp_str2_sz);
                rb_hash_aset(ret, tmp, tmp2);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(string, string)");
            }

            return ret;
        case HYPERDATATYPE_MAP_STRING_INT64:
            ret = rb_hash_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_map_string_int_next(&iter, &tmp_str, &tmp_str_sz, &tmp_i)) > 0)
            {
                tmp = rb_str_new(tmp_str, tmp_str_sz);
                tmp2 = LL2NUM(tmp_i);
                rb_hash_aset(ret, tmp, tmp2);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(string, int)");
            }

            return ret;
        case HYPERDATATYPE_MAP_STRING_FLOAT:
            ret = rb_hash_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_map_string_float_next(&iter, &tmp_str, &tmp_str_sz, &tmp_d)) > 0)
            {
                tmp = rb_str_new(tmp_str, tmp_str_sz);
                tmp2 = rb_float_new(tmp_d);
                rb_hash_aset(ret, tmp, tmp2);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(string, float)");
            }

            return ret;
        case HYPERDATATYPE_MAP_INT64_STRING:
            ret = rb_hash_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_map_int_string_next(&iter, &tmp_i, &tmp_str, &tmp_str_sz)) > 0)
            {
                tmp = LL2NUM(tmp_i);
                tmp2 = rb_str_new(tmp_str, tmp_str_sz);
                rb_hash_aset(ret, tmp, tmp2);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(int, string)");
            }

            return ret;
        case HYPERDATATYPE_MAP_INT64_INT64:
            ret = rb_hash_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_map_int_int_next(&iter, &tmp_i, &tmp_i2)) > 0)
            {
                tmp = LL2NUM(tmp_i);
                tmp2 = LL2NUM(tmp_i2);
                rb_hash_aset(ret, tmp, tmp2);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(int, int)");
            }

            return ret;
        case HYPERDATATYPE_MAP_INT64_FLOAT:
            ret = rb_hash_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_map_int_float_next(&iter, &tmp_i, &tmp_d)) > 0)
            {
                tmp = LL2NUM(tmp_i);
                tmp2 = rb_float_new(tmp_d);
                rb_hash_aset(ret, tmp, tmp2);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(int, float)");
            }

            return ret;
        case HYPERDATATYPE_MAP_FLOAT_STRING:
            ret = rb_hash_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_map_float_string_next(&iter, &tmp_d, &tmp_str, &tmp_str_sz)) > 0)
            {
                tmp = rb_float_new(tmp_d);
                tmp2 = rb_str_new(tmp_str, tmp_str_sz);
                rb_hash_aset(ret, tmp, tmp2);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(float, string)");
            }

            return ret;
        case HYPERDATATYPE_MAP_FLOAT_INT64:
            ret = rb_hash_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_map_float_int_next(&iter, &tmp_d, &tmp_i)) > 0)
            {
                tmp = rb_float_new(tmp_d);
                tmp2 = LL2NUM(tmp_i);
                rb_hash_aset(ret, tmp, tmp2);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(float, int)");
            }

            return ret;
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
            ret = rb_hash_new();
            hyperdex_ds_iterator_init(&iter, attrs->datatype, attrs->value, attrs->value_sz);

            while ((result = hyperdex_ds_iterate_map_float_float_next(&iter, &tmp_d, &tmp_d2)) > 0)
            {
                tmp = rb_float_new(tmp_d);
                tmp2 = rb_float_new(tmp_d2);
                rb_hash_aset(ret, tmp, tmp2);
            }

            if (result < 0)
            {
                hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(float, float)");
            }

            return ret;
        case HYPERDATATYPE_GENERIC:
        case HYPERDATATYPE_LIST_GENERIC:
        case HYPERDATATYPE_SET_GENERIC:
        case HYPERDATATYPE_MAP_GENERIC:
        case HYPERDATATYPE_MAP_STRING_KEYONLY:
        case HYPERDATATYPE_MAP_INT64_KEYONLY:
        case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
        case HYPERDATATYPE_GARBAGE:
        default:
            hyperdex_ruby_client_throw_exception(HYPERDEX_CLIENT_SERVERERROR, "server sent malformed attributes");
            return Qnil;
    }
}

static VALUE
hyperdex_ruby_client_build_attributes(const struct hyperdex_client_attribute* attrs,
                                      size_t attrs_sz)
{
    VALUE ret = rb_hash_new();
    VALUE val = Qnil;
    size_t i = 0;

    for (i = 0; i < attrs_sz; ++i)
    {
        val = hyperdex_ruby_client_build_attribute(attrs + i);
        rb_hash_aset(ret, ID2SYM(rb_intern(attrs[i].attr)), val);
    }

    return ret;
}

/******************************* Deferred Class *******************************/

struct hyperdex_ruby_client_deferred
{
    VALUE client;
    struct hyperdex_ds_arena* arena;
    int64_t reqid;
    enum hyperdex_client_returncode status;
    const struct hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    const char* description;
    uint64_t count;
    int finished;
    VALUE (*encode_return)(struct hyperdex_ruby_client_deferred* d);
};

void
hyperdex_ruby_client_deferred_mark(struct hyperdex_ruby_client_deferred* dfrd)
{
    if (dfrd)
    {
        rb_gc_mark(dfrd->client);
    }
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
void
hyperdex_ruby_client_deferred_free(struct hyperdex_ruby_client_deferred* dfrd)
{
    if (dfrd)
    {
        if (dfrd->arena)
        {
            hyperdex_ds_arena_destroy(dfrd->arena);
        }

        if (dfrd->attrs)
        {
            hyperdex_client_destroy_attrs(dfrd->attrs, dfrd->attrs_sz);
        }

        if (dfrd->description)
        {
            free((void*)dfrd->description);
        }

        free(dfrd);
    }
}
#pragma GCC diagnostic pop

static VALUE
hyperdex_ruby_client_deferred_alloc(VALUE class)
{
    struct hyperdex_ruby_client_deferred* dfrd;
    dfrd = malloc(sizeof(struct hyperdex_ruby_client_deferred));

    if (!dfrd)
    {
        hyperdex_ruby_out_of_memory();
        return Qnil;
    }

    memset(dfrd, 0, sizeof(struct hyperdex_ruby_client_deferred));
    dfrd->client = Qnil;
    dfrd->arena = hyperdex_ds_arena_create();

    if (!dfrd->arena)
    {
        hyperdex_ruby_out_of_memory();
        return Qnil;
    }

    dfrd->reqid = -1;
    dfrd->status = HYPERDEX_CLIENT_GARBAGE;
    dfrd->attrs = NULL;
    dfrd->attrs_sz = 0;
    dfrd->description = NULL;
    dfrd->count = 0;
    dfrd->finished = 0;
    dfrd->encode_return = NULL;
    return Data_Wrap_Struct(class, hyperdex_ruby_client_deferred_mark, hyperdex_ruby_client_deferred_free, dfrd);
}

static VALUE
hyperdex_ruby_client_deferred_init(VALUE self, VALUE client)
{
    struct hyperdex_ruby_client_deferred* dfrd = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_deferred, dfrd);
    dfrd->client = client;
    return self;
}

static VALUE
hyperdex_ruby_client_deferred_callback(VALUE self)
{
    VALUE ops;
    struct hyperdex_ruby_client_deferred* dfrd = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_deferred, dfrd);
    dfrd->finished = 1;
    ops = rb_iv_get(dfrd->client, "ops");
    rb_hash_delete(ops, INT2NUM(dfrd->reqid));
    return Qnil;
}

static VALUE
hyperdex_ruby_client_deferred_wait(VALUE self)
{
    struct hyperdex_ruby_client_deferred* dfrd = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_deferred, dfrd);

    while (!dfrd->finished && dfrd->reqid >= 0)
    {
        rb_funcall(dfrd->client, rb_intern("loop"), 0);
    }

    dfrd->finished = 1;
    assert(dfrd->encode_return);
    return dfrd->encode_return(dfrd);
}

static VALUE
hyperdex_ruby_client_deferred_encode_status(struct hyperdex_ruby_client_deferred* d)
{
    struct hyperdex_client* client = NULL;

    if (d->status == HYPERDEX_CLIENT_SUCCESS)
    {
        return Qtrue;
    }
    else if (d->status == HYPERDEX_CLIENT_NOTFOUND)
    {
        return Qnil;
    }
    else if (d->status == HYPERDEX_CLIENT_CMPFAIL)
    {
        return Qfalse;
    }
    else
    {
        Data_Get_Struct(d->client, struct hyperdex_client, client);
        hyperdex_ruby_client_throw_exception(d->status, hyperdex_client_error_message(client));
        return Qnil;
    }
}

static VALUE
hyperdex_ruby_client_deferred_encode_status_attributes(struct hyperdex_ruby_client_deferred* d)
{
    struct hyperdex_client* client = NULL;

    if (d->status == HYPERDEX_CLIENT_SUCCESS)
    {
        return hyperdex_ruby_client_build_attributes(d->attrs, d->attrs_sz);
    }
    else if (d->status == HYPERDEX_CLIENT_NOTFOUND)
    {
        return Qnil;
    }
    else if (d->status == HYPERDEX_CLIENT_CMPFAIL)
    {
        return Qfalse;
    }
    else
    {
        Data_Get_Struct(d->client, struct hyperdex_client, client);
        hyperdex_ruby_client_throw_exception(d->status, hyperdex_client_error_message(client));
        return Qnil;
    }
}

static VALUE
hyperdex_ruby_client_deferred_encode_status_count(struct hyperdex_ruby_client_deferred* d)
{
    struct hyperdex_client* client = NULL;

    if (d->status == HYPERDEX_CLIENT_SUCCESS)
    {
        return INT2NUM(d->count);
    }
    else if (d->status == HYPERDEX_CLIENT_NOTFOUND)
    {
        return Qnil;
    }
    else if (d->status == HYPERDEX_CLIENT_CMPFAIL)
    {
        return Qfalse;
    }
    else
    {
        Data_Get_Struct(d->client, struct hyperdex_client, client);
        hyperdex_ruby_client_throw_exception(d->status, hyperdex_client_error_message(client));
        return Qnil;
    }
}

static VALUE
hyperdex_ruby_client_deferred_encode_status_description(struct hyperdex_ruby_client_deferred* d)
{
    struct hyperdex_client* client = NULL;

    if (d->status == HYPERDEX_CLIENT_SUCCESS)
    {
        return rb_str_new2(d->description);
    }
    else if (d->status == HYPERDEX_CLIENT_NOTFOUND)
    {
        return Qnil;
    }
    else if (d->status == HYPERDEX_CLIENT_CMPFAIL)
    {
        return Qfalse;
    }
    else
    {
        Data_Get_Struct(d->client, struct hyperdex_client, client);
        hyperdex_ruby_client_throw_exception(d->status, hyperdex_client_error_message(client));
        return Qnil;
    }
}

/******************************* Iterator Class *******************************/

struct hyperdex_ruby_client_iterator
{
    VALUE client;
    VALUE backlogged;
    struct hyperdex_ds_arena* arena;
    int64_t reqid;
    enum hyperdex_client_returncode status;
    const struct hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    int finished;
    VALUE (*encode_return)(struct hyperdex_ruby_client_iterator* d);
};

void
hyperdex_ruby_client_iterator_mark(struct hyperdex_ruby_client_iterator* iter)
{
    if (iter)
    {
        rb_gc_mark(iter->client);
    }
}

void
hyperdex_ruby_client_iterator_free(struct hyperdex_ruby_client_iterator* iter)
{
    if (iter)
    {
        if (iter->arena)
        {
            hyperdex_ds_arena_destroy(iter->arena);
        }

        if (iter->attrs)
        {
            hyperdex_client_destroy_attrs(iter->attrs, iter->attrs_sz);
        }

        free(iter);
    }
}

static VALUE
hyperdex_ruby_client_iterator_alloc(VALUE class)
{
    struct hyperdex_ruby_client_iterator* iter;
    iter = malloc(sizeof(struct hyperdex_ruby_client_iterator));

    if (!iter)
    {
        hyperdex_ruby_out_of_memory();
        return Qnil;
    }

    memset(iter, 0, sizeof(struct hyperdex_ruby_client_iterator));
    iter->client = Qnil;
    iter->backlogged = rb_ary_new();
    iter->arena = hyperdex_ds_arena_create();

    if (!iter->arena)
    {
        hyperdex_ruby_out_of_memory();
        return Qnil;
    }

    iter->reqid = -1;
    iter->status = HYPERDEX_CLIENT_GARBAGE;
    iter->attrs = NULL;
    iter->attrs_sz = 0;
    iter->encode_return = NULL;
    return Data_Wrap_Struct(class, hyperdex_ruby_client_iterator_mark, hyperdex_ruby_client_iterator_free, iter);
}

static VALUE
hyperdex_ruby_client_iterator_init(VALUE self, VALUE client)
{
    struct hyperdex_ruby_client_iterator* iter = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_iterator, iter);
    iter->client = client;
    return self;
}

static VALUE
hyperdex_ruby_client_iterator_callback(VALUE self)
{
    VALUE ops;
    VALUE obj;
    struct hyperdex_ruby_client_iterator* iter = NULL;
    struct hyperdex_client* client;
    Data_Get_Struct(self, struct hyperdex_ruby_client_iterator, iter);
    Data_Get_Struct(iter->client, struct hyperdex_client, client);

    if (iter->status == HYPERDEX_CLIENT_SEARCHDONE)
    {
        iter->finished = 1;
        ops = rb_iv_get(iter->client, "ops");
        rb_hash_delete(ops, INT2NUM(iter->reqid));
    }
    else if (iter->status == HYPERDEX_CLIENT_SUCCESS)
    {
        obj = iter->encode_return(iter);
        hyperdex_client_destroy_attrs(iter->attrs, iter->attrs_sz);
        iter->attrs = NULL;
        iter->attrs_sz = 0;
        rb_ary_push(iter->backlogged, obj);
    }
    else
    {
        obj = hyperdex_ruby_client_create_exception(iter->status,
                                                    hyperdex_client_error_message(client));
        rb_ary_push(iter->backlogged, obj);
    }

    return Qnil;
}

static VALUE
hyperdex_ruby_client_iterator_has_next(VALUE self)
{
    struct hyperdex_ruby_client_iterator* iter = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_iterator, iter);

    while (!iter->finished && RARRAY_LEN(iter->backlogged) == 0)
    {
        rb_funcall(iter->client, rb_intern("loop"), 0);
    }

    return RARRAY_LEN(iter->backlogged) > 0 ? Qtrue : Qfalse;
}

static VALUE
hyperdex_ruby_client_iterator_next(VALUE self)
{
    struct hyperdex_ruby_client_iterator* iter = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_iterator, iter);
    return rb_ary_shift(iter->backlogged);
}

static VALUE
hyperdex_ruby_client_iterator_encode_status_attributes(struct hyperdex_ruby_client_iterator* it)
{
    struct hyperdex_client* client = NULL;

    if (it->status == HYPERDEX_CLIENT_SUCCESS)
    {
        return hyperdex_ruby_client_build_attributes(it->attrs, it->attrs_sz);
    }
    else if (it->status == HYPERDEX_CLIENT_NOTFOUND)
    {
        return Qnil;
    }
    else if (it->status == HYPERDEX_CLIENT_CMPFAIL)
    {
        return Qfalse;
    }
    else
    {
        Data_Get_Struct(it->client, struct hyperdex_client, client);
        hyperdex_ruby_client_throw_exception(it->status, hyperdex_client_error_message(client));
        return Qnil;
    }
}

/******************************** Client Class ********************************/

static VALUE
hyperdex_ruby_client_alloc(VALUE class)
{
    struct hyperdex_client* client = NULL;
    return Data_Wrap_Struct(class, 0, hyperdex_client_destroy, client);
}

static VALUE
hyperdex_ruby_client_init(VALUE self, VALUE host, VALUE port)
{
    VALUE ops;
    struct hyperdex_client* client = NULL;
    client = hyperdex_client_create(StringValueCStr(host), NUM2UINT(port));

    if (!client)
    {
        rb_raise(rb_eSystemCallError, "Could not create Client instance");
        return Qnil;
    }

    DATA_PTR(self) = client;
    ops = rb_hash_new();
    if (ops == Qnil) return Qnil;
    rb_iv_set(self, "ops", ops);
    rb_iv_set(self, "tmp", Qnil);
    return self;
}

static VALUE
hyperdex_ruby_client_loop(VALUE self)
{
    struct hyperdex_client* client;
    enum hyperdex_client_returncode rc;
    int64_t ret;
    VALUE ops;
    VALUE op;

    Data_Get_Struct(self, struct hyperdex_client, client);
    ret = hyperdex_client_loop(client, -1, &rc);

    if (ret < 0)
    {
        hyperdex_ruby_client_throw_exception(rc, hyperdex_client_error_message(client));
        return Qnil;
    }
    else
    {
        ops = rb_iv_get(self, "ops");
        op = rb_hash_lookup(ops, LONG2NUM(ret));
        rb_funcall(op, rb_intern("callback"), 0);
        return op;
    }
}

#include "bindings/ruby/definitions.c"

/********************************* Predicates *********************************/

static void
hyperdex_ruby_client_predicate_mark(struct hyperdex_ruby_client_predicate* pred)
{
    size_t i = 0;

    if (!pred)
    {
        return;
    }

    for (i = 0; i < pred->num_checks; ++i)
    {
        rb_gc_mark(pred->checks[i].v);
    }
}

static VALUE
hyperdex_ruby_client_predicate_alloc1(VALUE class)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    size_t sz = offsetof(struct hyperdex_ruby_client_predicate, checks)
              + sizeof(struct hyperdex_ruby_client_predicate_inner);
    pred = malloc(sz);

    if (!pred)
    {
        hyperdex_ruby_out_of_memory();
        return Qnil;
    }

    memset(pred, 0, sz);
    pred->num_checks = 1;
    return Data_Wrap_Struct(class, hyperdex_ruby_client_predicate_mark, free, pred);
}

static VALUE
hyperdex_ruby_client_predicate_alloc2(VALUE class)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    size_t sz = offsetof(struct hyperdex_ruby_client_predicate, checks)
              + 2 * sizeof(struct hyperdex_ruby_client_predicate_inner);
    pred = malloc(sz);

    if (!pred)
    {
        hyperdex_ruby_out_of_memory();
        return Qnil;
    }

    memset(pred, 0, sz);
    pred->num_checks = 2;
    return Data_Wrap_Struct(class, hyperdex_ruby_client_predicate_mark, free, pred);
}

static VALUE
hyperdex_ruby_client_predicate_init(VALUE self)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = Qnil;
    pred->checks[0].predicate = HYPERPREDICATE_FAIL;
    return self;
}

static VALUE
hyperdex_ruby_client_predicate_equals_init(VALUE self, VALUE v)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = v;
    pred->checks[0].predicate = HYPERPREDICATE_EQUALS;
    return self;
}

static VALUE
hyperdex_ruby_client_predicate_lessequal_init(VALUE self, VALUE v)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = v;
    pred->checks[0].predicate = HYPERPREDICATE_LESS_EQUAL;
    return self;
}

static VALUE
hyperdex_ruby_client_predicate_greaterequal_init(VALUE self, VALUE v)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = v;
    pred->checks[0].predicate = HYPERPREDICATE_GREATER_EQUAL;
    return self;
}

static VALUE
hyperdex_ruby_client_predicate_lessthan_init(VALUE self, VALUE v)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = v;
    pred->checks[0].predicate = HYPERPREDICATE_LESS_THAN;
    return self;
}

static VALUE
hyperdex_ruby_client_predicate_greaterthan_init(VALUE self, VALUE v)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = v;
    pred->checks[0].predicate = HYPERPREDICATE_GREATER_THAN;
    return self;
}

static VALUE
hyperdex_ruby_client_predicate_range_init(VALUE self, VALUE lower, VALUE upper)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = lower;
    pred->checks[0].predicate = HYPERPREDICATE_GREATER_EQUAL;
    pred->checks[1].v = upper;
    pred->checks[1].predicate = HYPERPREDICATE_LESS_EQUAL;
    return self;
}

static VALUE
hyperdex_ruby_client_predicate_regex_init(VALUE self, VALUE v)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = v;
    pred->checks[0].predicate = HYPERPREDICATE_REGEX;
    return self;
}

static VALUE
hyperdex_ruby_client_predicate_lengthequals_init(VALUE self, VALUE v)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = v;
    pred->checks[0].predicate = HYPERPREDICATE_LENGTH_EQUALS;
    return self;
}

static VALUE
hyperdex_ruby_client_predicate_lengthlessequal_init(VALUE self, VALUE v)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = v;
    pred->checks[0].predicate = HYPERPREDICATE_LENGTH_LESS_EQUAL;
    return self;
}

static VALUE
hyperdex_ruby_client_predicate_lengthgreaterequal_init(VALUE self, VALUE v)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = v;
    pred->checks[0].predicate = HYPERPREDICATE_LENGTH_GREATER_EQUAL;
    return self;
}

static VALUE
hyperdex_ruby_client_predicate_contains_init(VALUE self, VALUE v)
{
    struct hyperdex_ruby_client_predicate* pred = NULL;
    Data_Get_Struct(self, struct hyperdex_ruby_client_predicate, pred);
    pred->checks[0].v = v;
    pred->checks[0].predicate = HYPERPREDICATE_CONTAINS;
    return self;
}

/******************************* Inititalization ******************************/

void
Init_hyperdex_client()
{
    /* require the set type */
    rb_require("set");
    Set = rb_path2class("Set");

    /* setup the module */
    mod_hyperdex_client = rb_define_module_under(mod_hyperdex, "Client");

    /* create the Exception class */
    class_exception = rb_define_class_under(mod_hyperdex_client, "HyperDexClientException", rb_eStandardError);
    rb_define_attr(class_exception, "status", 1, 0);
    rb_define_attr(class_exception, "symbol", 1, 0);

    /* create the Deferred class */
    class_deferred = rb_define_class_under(mod_hyperdex_client, "Deferred", rb_cObject);
    rb_define_alloc_func(class_deferred, hyperdex_ruby_client_deferred_alloc);
    rb_define_method(class_deferred, "initialize", hyperdex_ruby_client_deferred_init, 1);
    rb_define_method(class_deferred, "callback", hyperdex_ruby_client_deferred_callback, 0);
    rb_define_method(class_deferred, "wait", hyperdex_ruby_client_deferred_wait, 0);

    /* create the Iterator class */
    class_iterator = rb_define_class_under(mod_hyperdex_client, "Iterator", rb_cObject);
    rb_define_alloc_func(class_iterator, hyperdex_ruby_client_iterator_alloc);
    rb_define_method(class_iterator, "initialize", hyperdex_ruby_client_iterator_init, 1);
    rb_define_method(class_iterator, "callback", hyperdex_ruby_client_iterator_callback, 0);
    rb_define_method(class_iterator, "has_next", hyperdex_ruby_client_iterator_has_next, 0);
    rb_define_method(class_iterator, "next", hyperdex_ruby_client_iterator_next, 0);

    /* create the Client class */
    class_client = rb_define_class_under(mod_hyperdex_client, "Client", rb_cObject);
    rb_define_alloc_func(class_client, hyperdex_ruby_client_alloc);
    rb_define_method(class_client, "initialize", hyperdex_ruby_client_init, 2);
    rb_define_method(class_client, "loop", hyperdex_ruby_client_loop, 0);
    /* include the generated rb_define_* calls */
#include "bindings/ruby/prototypes.c"

    /* create the Predicate class */
    class_predicate = rb_define_class_under(mod_hyperdex_client, "Predicate", rb_cObject);
    rb_define_alloc_func(class_predicate, hyperdex_ruby_client_predicate_alloc1);
    rb_define_method(class_predicate, "initialize", hyperdex_ruby_client_predicate_init, 1);

    /* create the Equals class */
    class_equals = rb_define_class_under(mod_hyperdex_client, "Equals", class_predicate);
    rb_define_alloc_func(class_equals , hyperdex_ruby_client_predicate_alloc1);
    rb_define_method(class_equals , "initialize", hyperdex_ruby_client_predicate_equals_init, 1);

    /* create the LessEqual class */
    class_lessequal = rb_define_class_under(mod_hyperdex_client, "LessEqual", class_predicate);
    rb_define_alloc_func(class_lessequal , hyperdex_ruby_client_predicate_alloc1);
    rb_define_method(class_lessequal , "initialize", hyperdex_ruby_client_predicate_lessequal_init, 1);

    /* create the GreaterEqual class */
    class_greaterequal = rb_define_class_under(mod_hyperdex_client, "GreaterEqual", class_predicate);
    rb_define_alloc_func(class_greaterequal , hyperdex_ruby_client_predicate_alloc1);
    rb_define_method(class_greaterequal , "initialize", hyperdex_ruby_client_predicate_greaterequal_init, 1);

    /* create the LessThan class */
    class_lessthan = rb_define_class_under(mod_hyperdex_client, "LessThan", class_predicate);
    rb_define_alloc_func(class_lessthan , hyperdex_ruby_client_predicate_alloc1);
    rb_define_method(class_lessthan , "initialize", hyperdex_ruby_client_predicate_lessthan_init, 1);

    /* create the GreaterThan class */
    class_greaterthan = rb_define_class_under(mod_hyperdex_client, "GreaterThan", class_predicate);
    rb_define_alloc_func(class_greaterthan , hyperdex_ruby_client_predicate_alloc1);
    rb_define_method(class_greaterthan , "initialize", hyperdex_ruby_client_predicate_greaterthan_init, 1);

    /* create the Range class */
    class_range = rb_define_class_under(mod_hyperdex_client, "Range", class_predicate);
    rb_define_alloc_func(class_range , hyperdex_ruby_client_predicate_alloc2);
    rb_define_method(class_range , "initialize", hyperdex_ruby_client_predicate_range_init, 2);

    /* create the Regex class */
    class_regex = rb_define_class_under(mod_hyperdex_client, "Regex", class_predicate);
    rb_define_alloc_func(class_regex , hyperdex_ruby_client_predicate_alloc1);
    rb_define_method(class_regex , "initialize", hyperdex_ruby_client_predicate_regex_init, 1);

    /* create the LengthEquals class */
    class_lengthequals = rb_define_class_under(mod_hyperdex_client, "LengthEquals", class_predicate);
    rb_define_alloc_func(class_lengthequals , hyperdex_ruby_client_predicate_alloc1);
    rb_define_method(class_lengthequals , "initialize", hyperdex_ruby_client_predicate_lengthequals_init, 1);

    /* create the LengthLessEqual class */
    class_lengthlessequal = rb_define_class_under(mod_hyperdex_client, "LengthLessEqual", class_predicate);
    rb_define_alloc_func(class_lengthlessequal , hyperdex_ruby_client_predicate_alloc1);
    rb_define_method(class_lengthlessequal , "initialize", hyperdex_ruby_client_predicate_lengthlessequal_init, 1);

    /* create the LengthGreaterEqual class */
    class_lengthgreaterequal = rb_define_class_under(mod_hyperdex_client, "LengthGreaterEqual", class_predicate);
    rb_define_alloc_func(class_lengthgreaterequal , hyperdex_ruby_client_predicate_alloc1);
    rb_define_method(class_lengthgreaterequal , "initialize", hyperdex_ruby_client_predicate_lengthgreaterequal_init, 1);

    /* create the Contains class */
    class_contains = rb_define_class_under(mod_hyperdex_client, "Contains", class_predicate);
    rb_define_alloc_func(class_contains , hyperdex_ruby_client_predicate_alloc1);
    rb_define_method(class_contains , "initialize", hyperdex_ruby_client_predicate_contains_init, 1);
}
