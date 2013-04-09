/* Copyright (c) 2012, Cornell University
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
#include <stdio.h>
#include <stdlib.h>

/* Ruby */
#include <ruby.h>

/* HyperDex */
#include "hyperdex.h"
#include "common/macros.h"
#include "client/hyperclient.h"
#include "client/ruby/type_conversion.h"

static VALUE chyperclient = Qnil;
static VALUE cexcept = Qnil;
static VALUE cdeferred = Qnil;
static VALUE csearch = Qnil;

#define RAISEHC(rc) rhc_throw_hyperclient_exception(rc, NULL)

/******************************* Error Handling *******************************/

#define RHC_ERROR_CASE(STATUS, DESCRIPTION) \
    case STATUS: \
        exception = rb_exc_new2(cexcept, DESCRIPTION); \
        rb_iv_set(exception, "@status", rb_uint_new(status)); \
        rb_iv_set(exception, "@symbol", rb_str_new2(xstr(STATUS))); \
        break

VALUE
rhc_create_hyperclient_exception(enum hyperclient_returncode status, const char* attr)
{
    VALUE exception = Qnil;
    const char* real_attr = attr == NULL ? "" : attr;
    char buf[2048];
    size_t num = 2047;

    switch (status)
    {
        RHC_ERROR_CASE(HYPERCLIENT_SUCCESS, "Success");
        RHC_ERROR_CASE(HYPERCLIENT_NOTFOUND, "Not Found");
        RHC_ERROR_CASE(HYPERCLIENT_SEARCHDONE, "Search Done");
        RHC_ERROR_CASE(HYPERCLIENT_CMPFAIL, "Conditional Operation Did Not Match Object");
        RHC_ERROR_CASE(HYPERCLIENT_READONLY, "Cluster is in a Read-Only State");
        RHC_ERROR_CASE(HYPERCLIENT_UNKNOWNSPACE, "Unknown Space");
        RHC_ERROR_CASE(HYPERCLIENT_COORDFAIL, "Coordinator Failure");
        RHC_ERROR_CASE(HYPERCLIENT_SERVERERROR, "Server Error");
        RHC_ERROR_CASE(HYPERCLIENT_POLLFAILED, "Polling Failed");
        RHC_ERROR_CASE(HYPERCLIENT_OVERFLOW, "Integer-overflow or divide-by-zero");
        RHC_ERROR_CASE(HYPERCLIENT_RECONFIGURE, "Reconfiguration");
        RHC_ERROR_CASE(HYPERCLIENT_TIMEOUT, "Timeout");
        RHC_ERROR_CASE(HYPERCLIENT_UNKNOWNATTR, "Unknown attribute");
        RHC_ERROR_CASE(HYPERCLIENT_DUPEATTR, "Duplicate attribute");
        RHC_ERROR_CASE(HYPERCLIENT_NONEPENDING, "None pending");
        RHC_ERROR_CASE(HYPERCLIENT_DONTUSEKEY, "Do not specify the key in a search predicate and do not redundantly specify the key for an insert");
        RHC_ERROR_CASE(HYPERCLIENT_WRONGTYPE, "Attribute hsa the wrong type");
        RHC_ERROR_CASE(HYPERCLIENT_NOMEM, "Memory allocation failed");
        RHC_ERROR_CASE(HYPERCLIENT_EXCEPTION, "Internal Error (file a bug)");
        default:
            exception = rb_exc_new2(cexcept, "Unknown Error (file a bug)");
            rb_iv_set(exception, "@status", rb_uint_new(status));
            rb_iv_set(exception, "@symbol", rb_str_new2("BUG"));
            break;
    }

    return exception;
}

void
rhc_throw_hyperclient_exception(enum hyperclient_returncode status, const char* attr)
{
    VALUE exception = rhc_create_hyperclient_exception(status, attr);
    rb_exc_raise(exception);
}

/******************************* Deferred Class *******************************/

struct rhc_deferred
{
    VALUE client;
    int64_t reqid;
    enum hyperclient_returncode status;
    int finished;
    VALUE backings;
    VALUE space;
    const char* key;
    size_t key_sz;
    struct hyperclient_attribute_check* chks;
    size_t chks_sz;
    struct hyperclient_attribute* attrs;
    size_t attrs_sz;
    struct hyperclient_map_attribute* map_attrs;
    size_t map_attrs_sz;
    struct hyperclient_attribute* ret_attrs;
    size_t ret_attrs_sz;
    VALUE (*encode_return)(struct rhc_deferred* rhcd);
};

void
rhc_deferred_mark(struct rhc_deferred* rhcd)
{
    if (rhcd)
    {
        rb_gc_mark(rhcd->client);
        rb_gc_mark(rhcd->backings);
        rb_gc_mark(rhcd->space);
    }
}

void
rhc_deferred_free(struct rhc_deferred* rhcd)
{
    if (rhcd)
    {
        if (rhcd->chks)
        {
            rhc_free_checks(rhcd->chks);
        }

        if (rhcd->attrs)
        {
            rhc_free_attrs(rhcd->attrs);
        }

        if (rhcd->map_attrs)
        {
            rhc_free_map_attrs(rhcd->map_attrs);
        }

        if (rhcd->ret_attrs)
        {
            hyperclient_destroy_attrs(rhcd->ret_attrs, rhcd->ret_attrs_sz);
        }

        free(rhcd);
    }
}

VALUE
rhc_deferred_alloc(VALUE class)
{
    VALUE tdata;
    struct rhc_deferred* rhcd = malloc(sizeof(struct rhc_deferred));

    if (!rhcd)
    {
        rb_raise(rb_eNoMemError, "failed to allocate memory");
        return Qnil;
    }

    memset(rhcd, 0, sizeof(struct rhc_deferred));
    rhcd->client = Qnil;
    rhcd->reqid = -1;
    rhcd->status = HYPERCLIENT_GARBAGE;
    rhcd->finished = 0;
    rhcd->backings = rb_ary_new();
    rhcd->space = Qnil;
    rhcd->key = NULL;
    rhcd->key_sz = 0;
    rhcd->chks = NULL;
    rhcd->chks_sz = 0;
    rhcd->attrs = NULL;
    rhcd->attrs_sz = 0;
    rhcd->map_attrs = NULL;
    rhcd->map_attrs_sz = 0;
    rhcd->ret_attrs = NULL;
    rhcd->ret_attrs_sz = 0;
    tdata = Data_Wrap_Struct(class, rhc_deferred_mark, rhc_deferred_free, rhcd);
    return tdata;
}

VALUE
rhc_deferred_initialize(VALUE self, VALUE client)
{
    struct rhc_deferred* rhcd = NULL;
    Data_Get_Struct(self, struct rhc_deferred, rhcd);
    rhcd->client = client;
    return self;
}

VALUE
rhc_deferred_callback(VALUE self)
{
    VALUE ops;
    struct rhc_deferred* rhcd = NULL;
    Data_Get_Struct(self, struct rhc_deferred, rhcd);
    rhcd->finished = 1;
    ops = rb_iv_get(rhcd->client, "ops");
    rb_hash_delete(ops, INT2NUM(rhcd->reqid));
    return Qnil;
}

VALUE
rhc_deferred_wait(VALUE self)
{
    struct rhc_deferred* rhcd = NULL;
    Data_Get_Struct(self, struct rhc_deferred, rhcd);

    while (!rhcd->finished && rhcd->reqid >= 0)
    {
        rb_funcall(rhcd->client, rb_intern("loop"), 0);
    }

    rhcd->finished = 1;
    assert(rhcd->encode_return);
    return rhcd->encode_return(rhcd);
}

VALUE
rhc_encode_return_get(struct rhc_deferred* rhcd)
{
    if (rhcd->status == HYPERCLIENT_SUCCESS)
    {
        return rhc_attrs_to_hash(rhcd->ret_attrs, rhcd->ret_attrs_sz);
    }
    else if (rhcd->status == HYPERCLIENT_NOTFOUND)
    {
        return Qnil;
    }
    else
    {
        RAISEHC(rhcd->status);
    }
}

VALUE
rhc_encode_return_true(struct rhc_deferred* rhcd)
{
    if (rhcd->status == HYPERCLIENT_SUCCESS)
    {
        return Qtrue;
    }
    else
    {
        RAISEHC(rhcd->status);
    }
}

VALUE
rhc_encode_return_true_notfound(struct rhc_deferred* rhcd)
{
    if (rhcd->status == HYPERCLIENT_SUCCESS)
    {
        return Qtrue;
    }
    if (rhcd->status == HYPERCLIENT_NOTFOUND)
    {
        return Qfalse;
    }
    else
    {
        RAISEHC(rhcd->status);
    }
}

VALUE
rhc_encode_return_true_cmpfail(struct rhc_deferred* rhcd)
{
    if (rhcd->status == HYPERCLIENT_SUCCESS)
    {
        return Qtrue;
    }
    if (rhcd->status == HYPERCLIENT_CMPFAIL)
    {
        return Qfalse;
    }
    else
    {
        RAISEHC(rhcd->status);
    }
}

/******************************** Search Class ********************************/

struct rhc_search_base
{
    VALUE client;
    int64_t reqid;
    enum hyperclient_returncode status;
    int finished;
    VALUE backings;
    VALUE space;
    VALUE backlogged;
    struct hyperclient_attribute_check* chks;
    size_t chks_sz;
    struct hyperclient_attribute* ret_attrs;
    size_t ret_attrs_sz;
};

void
rhc_search_base_mark(struct rhc_search_base* rhcs)
{
    if (rhcs)
    {
        rb_gc_mark(rhcs->client);
        rb_gc_mark(rhcs->backings);
        rb_gc_mark(rhcs->space);
    }
}

void
rhc_search_base_free(struct rhc_search_base* rhcs)
{
    if (rhcs)
    {
        if (rhcs->chks)
        {
            rhc_free_checks(rhcs->chks);
        }

        if (rhcs->ret_attrs)
        {
            hyperclient_destroy_attrs(rhcs->ret_attrs, rhcs->ret_attrs_sz);
        }

        free(rhcs);
    }
}

VALUE
rhc_search_base_alloc(VALUE class)
{
    VALUE tdata;
    struct rhc_search_base* rhcs = malloc(sizeof(struct rhc_search_base));

    if (!rhcs)
    {
        rb_raise(rb_eNoMemError, "failed to allocate memory");
        return Qnil;
    }

    memset(rhcs, 0, sizeof(struct rhc_search_base));
    rhcs->client = Qnil;
    rhcs->reqid = -1;
    rhcs->status = HYPERCLIENT_GARBAGE;
    rhcs->finished = 0;
    rhcs->backings = rb_ary_new();
    rhcs->space = Qnil;
    rhcs->backlogged = rb_ary_new();
    rhcs->chks = NULL;
    rhcs->chks_sz = 0;
    rhcs->ret_attrs = NULL;
    rhcs->ret_attrs_sz = 0;
    tdata = Data_Wrap_Struct(class, rhc_search_base_mark, rhc_search_base_free, rhcs);
    return tdata;
}

VALUE
rhc_search_base_initialize(VALUE self, VALUE client)
{
    struct rhc_search_base* rhcs = NULL;
    Data_Get_Struct(self, struct rhc_search_base, rhcs);
    rhcs->client = client;
    return self;
}

VALUE
rhc_search_base_callback(VALUE self)
{
    VALUE ops;
    VALUE obj;
    struct rhc_search_base* rhcs = NULL;
    Data_Get_Struct(self, struct rhc_search_base, rhcs);

    if (rhcs->status == HYPERCLIENT_SEARCHDONE)
    {
        rhcs->finished = 1;
        ops = rb_iv_get(rhcs->client, "ops");
        rb_hash_delete(ops, INT2NUM(rhcs->reqid));
    }
    else if (rhcs->status == HYPERCLIENT_SUCCESS)
    {
        obj = rhc_attrs_to_hash(rhcs->ret_attrs, rhcs->ret_attrs_sz);
        hyperclient_destroy_attrs(rhcs->ret_attrs, rhcs->ret_attrs_sz);
        rhcs->ret_attrs = NULL;
        rhcs->ret_attrs_sz = 0;
        rb_ary_push(rhcs->backlogged, obj);
    }
    else
    {
        obj = rhc_create_hyperclient_exception(rhcs->status, NULL);
        rb_ary_push(rhcs->backlogged, obj);
    }

    return Qnil;
}

VALUE
rhc_search_base_has_next(VALUE self)
{
    struct rhc_search_base* rhcs = NULL;
    Data_Get_Struct(self, struct rhc_search_base, rhcs);

    while (!rhcs->finished && RARRAY_LEN(rhcs->backlogged) == 0)
    {
        rb_funcall(rhcs->client, rb_intern("loop"), 0);
    }

    return RARRAY_LEN(rhcs->backlogged) > 0 ? Qtrue : Qfalse;
}

VALUE
rhc_search_base_next(VALUE self)
{
    struct rhc_search_base* rhcs = NULL;
    Data_Get_Struct(self, struct rhc_search_base, rhcs);
    return rb_ary_shift(rhcs->backlogged);
}

/****************************** HyperClient Class *****************************/

VALUE
rhc_new(VALUE class, VALUE host, VALUE port)
{
    VALUE argv[2];
    VALUE tdata;
    struct hyperclient* client = hyperclient_create(StringValueCStr(host), NUM2UINT(port));

    if (!client)
    {
        rb_raise(rb_eSystemCallError, "Could not create HyperClient instance");
        return Qnil;
    }

    tdata = Data_Wrap_Struct(class, 0, hyperclient_destroy, client);
    argv[0] = host;
    argv[1] = port;
    rb_obj_call_init(tdata, 2, argv);
    return tdata;
}

VALUE
rhc_init(VALUE self, VALUE host, VALUE port)
{
    VALUE ops;
    ops = rb_hash_new();
    if (ops == Qnil) return Qnil;
    rb_iv_set(self, "ops", ops);
    return self;
}

VALUE
rhc_add_space(VALUE self, VALUE space)
{
    struct hyperclient* client;
    Data_Get_Struct(self, struct hyperclient, client);
    enum hyperclient_returncode rc = hyperclient_add_space(client, StringValueCStr(space));

    if (rc != HYPERCLIENT_SUCCESS)
    {
        RAISEHC(rc);
    }

    return Qnil;
}

VALUE
rhc_rm_space(VALUE self, VALUE spacename)
{
    struct hyperclient* client;
    Data_Get_Struct(self, struct hyperclient, client);
    enum hyperclient_returncode rc = hyperclient_rm_space(client, StringValueCStr(spacename));

    if (rc != HYPERCLIENT_SUCCESS)
    {
        RAISEHC(rc);
    }

    return Qnil;
}

#define RHC_SPACE_KEY_RETURN(func, encret) \
    VALUE \
    rhc_ ## func(VALUE self, VALUE space, VALUE key) \
    { \
        VALUE async; \
        async = rb_funcall(self, rb_intern("async_" str(func)), 2, space, key); \
        return rb_funcall(async, rb_intern("wait"), 0); \
    } \
    VALUE \
    rhc_async_ ## func(VALUE self, VALUE space, VALUE key) \
    { \
        VALUE async; \
        VALUE ops; \
        struct hyperclient* client; \
        struct rhc_deferred* rhcd; \
        enum hyperdatatype type; \
        async = rb_class_new_instance(1, &self, cdeferred); \
        Data_Get_Struct(self, struct hyperclient, client); \
        Data_Get_Struct(async, struct rhc_deferred, rhcd); \
        rhcd->space = space; \
        rhc_ruby_to_hyperdex(key, &rhcd->key, &rhcd->key_sz, &type, &rhcd->backings); \
        rhcd->reqid = hyperclient_ ## func(client, StringValueCStr(rhcd->space), \
                                           rhcd->key, rhcd->key_sz, \
                                           &rhcd->status, \
                                           &rhcd->ret_attrs, &rhcd->ret_attrs_sz); \
        if (rhcd->reqid < 0) \
        { \
            RAISEHC(rhcd->status); \
        } \
        rhcd->encode_return = encret; \
        ops = rb_iv_get(self, "ops"); \
        rb_hash_aset(ops, LONG2NUM(rhcd->reqid), async); \
        return async; \
    }

#define RHC_SPACE_KEY(func, encret) \
    VALUE \
    rhc_ ## func(VALUE self, VALUE space, VALUE key) \
    { \
        VALUE async; \
        async = rb_funcall(self, rb_intern("async_" str(func)), 2, space, key); \
        return rb_funcall(async, rb_intern("wait"), 0); \
    } \
    VALUE \
    rhc_async_ ## func(VALUE self, VALUE space, VALUE key) \
    { \
        VALUE async; \
        VALUE ops; \
        struct hyperclient* client; \
        struct rhc_deferred* rhcd; \
        enum hyperdatatype type; \
        async = rb_class_new_instance(1, &self, cdeferred); \
        Data_Get_Struct(self, struct hyperclient, client); \
        Data_Get_Struct(async, struct rhc_deferred, rhcd); \
        rhcd->space = space; \
        rhc_ruby_to_hyperdex(key, &rhcd->key, &rhcd->key_sz, &type, &rhcd->backings); \
        rhcd->reqid = hyperclient_ ## func(client, StringValueCStr(rhcd->space), \
                                           rhcd->key, rhcd->key_sz, \
                                           &rhcd->status); \
        if (rhcd->reqid < 0) \
        { \
            RAISEHC(rhcd->status); \
        } \
        rhcd->encode_return = encret; \
        ops = rb_iv_get(self, "ops"); \
        rb_hash_aset(ops, LONG2NUM(rhcd->reqid), async); \
        return async; \
    }

#define RHC_SPACE_KEY_VALUE(func, encret) \
    VALUE \
    rhc_ ## func(VALUE self, VALUE space, VALUE key, VALUE value) \
    { \
        VALUE async; \
        async = rb_funcall(self, rb_intern("async_" str(func)), 3, space, key, value); \
        return rb_funcall(async, rb_intern("wait"), 0); \
    } \
    VALUE \
    rhc_async_ ## func(VALUE self, VALUE space, VALUE key, VALUE value) \
    { \
        VALUE async; \
        VALUE ops; \
        struct hyperclient* client; \
        struct rhc_deferred* rhcd; \
        enum hyperdatatype type; \
        async = rb_class_new_instance(1, &self, cdeferred); \
        Data_Get_Struct(self, struct hyperclient, client); \
        Data_Get_Struct(async, struct rhc_deferred, rhcd); \
        rhcd->space = space; \
        rhc_ruby_to_hyperdex(key, &rhcd->key, &rhcd->key_sz, &type, &rhcd->backings); \
        rhc_hash_to_attrs(value, &rhcd->attrs, &rhcd->attrs_sz, &rhcd->backings); \
        rhcd->reqid = hyperclient_ ## func(client, StringValueCStr(rhcd->space), \
                                           rhcd->key, rhcd->key_sz, \
                                           rhcd->attrs, rhcd->attrs_sz, \
                                           &rhcd->status); \
        if (rhcd->reqid < 0) \
        { \
            RAISEHC(rhcd->status); \
        } \
        rhcd->encode_return = encret; \
        ops = rb_iv_get(self, "ops"); \
        rb_hash_aset(ops, LONG2NUM(rhcd->reqid), async); \
        return async; \
    }

#define RHC_SPACE_KEY_PREDICATE_VALUE(func, encret) \
    VALUE \
    rhc_ ## func(VALUE self, VALUE space, VALUE key, VALUE predicate, VALUE value) \
    { \
        VALUE async; \
        async = rb_funcall(self, rb_intern("async_" str(func)), 4, space, key, predicate, value); \
        return rb_funcall(async, rb_intern("wait"), 0); \
    } \
    VALUE \
    rhc_async_ ## func(VALUE self, VALUE space, VALUE key, VALUE predicate, VALUE value) \
    { \
        VALUE async; \
        VALUE ops; \
        struct hyperclient* client; \
        struct rhc_deferred* rhcd; \
        enum hyperdatatype type; \
        async = rb_class_new_instance(1, &self, cdeferred); \
        Data_Get_Struct(self, struct hyperclient, client); \
        Data_Get_Struct(async, struct rhc_deferred, rhcd); \
        rhcd->space = space; \
        rhc_ruby_to_hyperdex(key, &rhcd->key, &rhcd->key_sz, &type, &rhcd->backings); \
        rhc_hash_to_checks(predicate, &rhcd->chks, &rhcd->chks_sz, &rhcd->backings); \
        rhc_hash_to_attrs(value, &rhcd->attrs, &rhcd->attrs_sz, &rhcd->backings); \
        rhcd->reqid = hyperclient_ ## func(client, StringValueCStr(rhcd->space), \
                                           rhcd->key, rhcd->key_sz, \
                                           rhcd->chks, rhcd->chks_sz, \
                                           rhcd->attrs, rhcd->attrs_sz, \
                                           &rhcd->status); \
        if (rhcd->reqid < 0) \
        { \
            RAISEHC(rhcd->status); \
        } \
        rhcd->encode_return = encret; \
        ops = rb_iv_get(self, "ops"); \
        rb_hash_aset(ops, LONG2NUM(rhcd->reqid), async); \
        return async; \
    }

#define RHC_SPACE_KEY_MAPVALUE(func, encret) \
    VALUE \
    rhc_ ## func(VALUE self, VALUE space, VALUE key, VALUE mapvalue) \
    { \
        VALUE async; \
        async = rb_funcall(self, rb_intern("async_" str(func)), 3, space, key, mapvalue); \
        return rb_funcall(async, rb_intern("wait"), 0); \
    } \
    VALUE \
    rhc_async_ ## func(VALUE self, VALUE space, VALUE key, VALUE mapvalue) \
    { \
        VALUE async; \
        VALUE ops; \
        struct hyperclient* client; \
        struct rhc_deferred* rhcd; \
        enum hyperdatatype type; \
        async = rb_class_new_instance(1, &self, cdeferred); \
        Data_Get_Struct(self, struct hyperclient, client); \
        Data_Get_Struct(async, struct rhc_deferred, rhcd); \
        rhcd->space = space; \
        rhc_ruby_to_hyperdex(key, &rhcd->key, &rhcd->key_sz, &type, &rhcd->backings); \
        rhc_hash_to_map_attrs(mapvalue, &rhcd->map_attrs, &rhcd->map_attrs_sz, &rhcd->backings); \
        rhcd->reqid = hyperclient_ ## func(client, StringValueCStr(rhcd->space), \
                                           rhcd->key, rhcd->key_sz, \
                                           rhcd->map_attrs, rhcd->map_attrs_sz, \
                                           &rhcd->status); \
        if (rhcd->reqid < 0) \
        { \
            RAISEHC(rhcd->status); \
        } \
        rhcd->encode_return = encret; \
        ops = rb_iv_get(self, "ops"); \
        rb_hash_aset(ops, LONG2NUM(rhcd->reqid), async); \
        return async; \
    }

RHC_SPACE_KEY_RETURN(get, rhc_encode_return_get)
RHC_SPACE_KEY_VALUE(put, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(put_if_not_exist, rhc_encode_return_true_cmpfail)
RHC_SPACE_KEY_PREDICATE_VALUE(cond_put, rhc_encode_return_true_cmpfail)
RHC_SPACE_KEY(del, rhc_encode_return_true_notfound)
RHC_SPACE_KEY_VALUE(atomic_add, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(atomic_sub, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(atomic_mul, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(atomic_div, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(atomic_mod, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(atomic_and, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(atomic_or, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(atomic_xor, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(string_prepend, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(string_append, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(list_lpush, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(list_rpush, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(set_add, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(set_remove, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(set_intersect, rhc_encode_return_true)
RHC_SPACE_KEY_VALUE(set_union, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_add, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_remove, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_atomic_add, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_atomic_sub, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_atomic_mul, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_atomic_div, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_atomic_mod, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_atomic_and, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_atomic_or, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_atomic_xor, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_string_prepend, rhc_encode_return_true)
RHC_SPACE_KEY_MAPVALUE(map_string_append, rhc_encode_return_true)

VALUE
rhc_search(VALUE self, VALUE space, VALUE predicate)
{
    VALUE search;
    VALUE ops;
    struct hyperclient* client;
    struct rhc_search_base* rhcs;
    search = rb_class_new_instance(1, &self, csearch);
    Data_Get_Struct(self, struct hyperclient, client); \
    Data_Get_Struct(search, struct rhc_search_base, rhcs);
    rhcs->space = space;
    rhc_hash_to_checks(predicate, &rhcs->chks, &rhcs->chks_sz, &rhcs->backings);
    rhcs->reqid = hyperclient_search(client, StringValueCStr(rhcs->space),
                                     rhcs->chks, rhcs->chks_sz,
                                     &rhcs->status,
                                     &rhcs->ret_attrs, &rhcs->ret_attrs_sz);

    if (rhcs->reqid < 0)
    {
        RAISEHC(rhcs->status);
    }

    ops = rb_iv_get(self, "ops");
    rb_hash_aset(ops, LONG2NUM(rhcs->reqid), search);
    return search;
}

/* sorted_search */
/* group_del */
/* count */

VALUE
rhc_loop(VALUE self)
{
    struct hyperclient* c;
    enum hyperclient_returncode rc;
    int64_t ret;
    VALUE ops;
    VALUE op;

    Data_Get_Struct(self, struct hyperclient, c);
    ret = hyperclient_loop(c, -1, &rc);

    if (ret < 0)
    {
        RAISEHC(rc);
    }
    else
    {
        ops = rb_iv_get(self, "ops");
        op = rb_hash_lookup(ops, LONG2NUM(ret));
        rb_funcall(op, rb_intern("callback"), 0);
        return op;
    }
}

/****************************** Initialize Things *****************************/

void
Init_hyperclient()
{
    /* require the set type */
    rb_require("set");

    /* create the HyperClient class */
    chyperclient = rb_define_class("HyperClient", rb_cObject);
    rb_define_singleton_method(chyperclient, "new", rhc_new, 2);
    rb_define_method(chyperclient, "initialize", rhc_init, 2);

    /* space creation methods */
    rb_define_method(chyperclient, "add_space", rhc_add_space, 1);
    rb_define_method(chyperclient, "rm_space", rhc_rm_space, 1);

    /* methods of HyperClient that are not ruby-specific */
    rb_define_method(chyperclient, "get", rhc_get, 2);
    rb_define_method(chyperclient, "async_get", rhc_async_get, 2);
    rb_define_method(chyperclient, "put", rhc_put, 3);
    rb_define_method(chyperclient, "async_put", rhc_async_put, 3);
    rb_define_method(chyperclient, "put_if_not_exist", rhc_put_if_not_exist, 3);
    rb_define_method(chyperclient, "async_put_if_not_exist", rhc_async_put_if_not_exist, 3);
    rb_define_method(chyperclient, "cond_put", rhc_cond_put, 4);
    rb_define_method(chyperclient, "async_cond_put", rhc_async_cond_put, 4);
    rb_define_method(chyperclient, "del", rhc_del, 2);
    rb_define_method(chyperclient, "async_del", rhc_async_del, 2);
    rb_define_method(chyperclient, "atomic_add", rhc_atomic_add, 3);
    rb_define_method(chyperclient, "async_atomic_add", rhc_async_atomic_add, 3);
    rb_define_method(chyperclient, "atomic_sub", rhc_atomic_sub, 3);
    rb_define_method(chyperclient, "async_atomic_sub", rhc_async_atomic_sub, 3);
    rb_define_method(chyperclient, "atomic_mul", rhc_atomic_mul, 3);
    rb_define_method(chyperclient, "async_atomic_mul", rhc_async_atomic_mul, 3);
    rb_define_method(chyperclient, "atomic_div", rhc_atomic_div, 3);
    rb_define_method(chyperclient, "async_atomic_div", rhc_async_atomic_div, 3);
    rb_define_method(chyperclient, "atomic_mod", rhc_atomic_mod, 3);
    rb_define_method(chyperclient, "async_atomic_mod", rhc_async_atomic_mod, 3);
    rb_define_method(chyperclient, "atomic_and", rhc_atomic_and, 3);
    rb_define_method(chyperclient, "async_atomic_and", rhc_async_atomic_and, 3);
    rb_define_method(chyperclient, "atomic_or", rhc_atomic_or, 3);
    rb_define_method(chyperclient, "async_atomic_or", rhc_async_atomic_or, 3);
    rb_define_method(chyperclient, "atomic_xor", rhc_atomic_xor, 3);
    rb_define_method(chyperclient, "async_atomic_xor", rhc_async_atomic_xor, 3);
    rb_define_method(chyperclient, "string_prepend", rhc_string_prepend, 3);
    rb_define_method(chyperclient, "async_string_prepend", rhc_async_string_prepend, 3);
    rb_define_method(chyperclient, "string_append", rhc_string_append, 3);
    rb_define_method(chyperclient, "async_string_append", rhc_async_string_append, 3);
    rb_define_method(chyperclient, "list_lpush", rhc_list_lpush, 3);
    rb_define_method(chyperclient, "async_list_lpush", rhc_async_list_lpush, 3);
    rb_define_method(chyperclient, "list_rpush", rhc_list_rpush, 3);
    rb_define_method(chyperclient, "async_list_rpush", rhc_async_list_rpush, 3);
    rb_define_method(chyperclient, "set_add", rhc_set_add, 3);
    rb_define_method(chyperclient, "async_set_add", rhc_async_set_add, 3);
    rb_define_method(chyperclient, "set_remove", rhc_set_remove, 3);
    rb_define_method(chyperclient, "async_set_remove", rhc_async_set_remove, 3);
    rb_define_method(chyperclient, "set_intersect", rhc_set_intersect, 3);
    rb_define_method(chyperclient, "async_set_intersect", rhc_async_set_intersect, 3);
    rb_define_method(chyperclient, "set_union", rhc_set_union, 3);
    rb_define_method(chyperclient, "async_set_union", rhc_async_set_union, 3);
    rb_define_method(chyperclient, "map_add", rhc_map_add, 3);
    rb_define_method(chyperclient, "async_map_add", rhc_async_map_add, 3);
    rb_define_method(chyperclient, "map_remove", rhc_map_remove, 3);
    rb_define_method(chyperclient, "async_map_remove", rhc_async_map_remove, 3);
    rb_define_method(chyperclient, "map_atomic_add", rhc_map_atomic_add, 3);
    rb_define_method(chyperclient, "async_map_atomic_add", rhc_async_map_atomic_add, 3);
    rb_define_method(chyperclient, "map_atomic_sub", rhc_map_atomic_sub, 3);
    rb_define_method(chyperclient, "async_map_atomic_sub", rhc_async_map_atomic_sub, 3);
    rb_define_method(chyperclient, "map_atomic_mul", rhc_map_atomic_mul, 3);
    rb_define_method(chyperclient, "async_map_atomic_mul", rhc_async_map_atomic_mul, 3);
    rb_define_method(chyperclient, "map_atomic_div", rhc_map_atomic_div, 3);
    rb_define_method(chyperclient, "async_map_atomic_div", rhc_async_map_atomic_div, 3);
    rb_define_method(chyperclient, "map_atomic_mod", rhc_map_atomic_mod, 3);
    rb_define_method(chyperclient, "async_map_atomic_mod", rhc_async_map_atomic_mod, 3);
    rb_define_method(chyperclient, "map_atomic_and", rhc_map_atomic_and, 3);
    rb_define_method(chyperclient, "async_map_atomic_and", rhc_async_map_atomic_and, 3);
    rb_define_method(chyperclient, "map_atomic_or", rhc_map_atomic_or, 3);
    rb_define_method(chyperclient, "async_map_atomic_or", rhc_async_map_atomic_or, 3);
    rb_define_method(chyperclient, "map_atomic_xor", rhc_map_atomic_xor, 3);
    rb_define_method(chyperclient, "async_map_atomic_xor", rhc_async_map_atomic_xor, 3);
    rb_define_method(chyperclient, "map_string_prepend", rhc_map_string_prepend, 3);
    rb_define_method(chyperclient, "async_map_string_prepend", rhc_async_map_string_prepend, 3);
    rb_define_method(chyperclient, "map_string_append", rhc_map_string_append, 3);
    rb_define_method(chyperclient, "async_map_string_append", rhc_async_map_string_append, 3);
    rb_define_method(chyperclient, "search", rhc_search, 2);
    rb_define_method(chyperclient, "loop", rhc_loop, 0);

    /* HyperClientException */
    cexcept = rb_define_class_under(chyperclient, "HyperClientException", rb_eStandardError);
    rb_define_attr(cexcept, "status", 1, 0);
    rb_define_attr(cexcept, "symbol", 1, 0);

    /* HyperClient::Deferred */
    cdeferred = rb_define_class_under(chyperclient, "Deferred", rb_cObject);
    rb_define_alloc_func(cdeferred, rhc_deferred_alloc);
    rb_define_method(cdeferred, "initialize", rhc_deferred_initialize, 1);
    rb_define_method(cdeferred, "callback", rhc_deferred_callback, 0);
    rb_define_method(cdeferred, "wait", rhc_deferred_wait, 0);

    /* HyperClient::Search */
    csearch = rb_define_class_under(chyperclient, "Search", rb_cObject);
    rb_define_alloc_func(csearch, rhc_search_base_alloc);
    rb_define_method(csearch, "initialize", rhc_search_base_initialize, 1);
    rb_define_method(csearch, "callback", rhc_search_base_callback, 0);
    rb_define_method(csearch, "has_next", rhc_search_base_has_next, 0);
    rb_define_method(csearch, "next", rhc_search_base_next, 0);
}
