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

/* GENERATED!  Do not modify this file directly */

#include "visibility.h"

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key__status_attributes(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, enum hyperdex_client_returncode* status, const struct hyperdex_client_attribute** attrs, size_t* attrs_sz), jstring spacename, jobject key);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key__status_attributes(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, enum hyperdex_client_returncode* status, const struct hyperdex_client_attribute** attrs, size_t* attrs_sz), jstring spacename, jobject key)
{
    const char* in_space;
    const char* in_key;
    size_t in_key_sz;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject dfrd = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct hyperdex_java_client_deferred* d = NULL;
    ERROR_CHECK(0);
    d = hyperdex_get_deferred_ptr(env, dfrd);
    ERROR_CHECK(0);
    success = hyperdex_java_client_convert_spacename(env, obj, d->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_key(env, obj, d->arena, key, &in_key, &in_key_sz);
    if (success < 0) return 0;
    d->reqid = f(client, in_space, in_key, in_key_sz, &d->status, &d->attrs, &d->attrs_sz);

    if (d->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }

    d->encode_return = hyperdex_java_client_deferred_encode_status_attributes;
    (*env)->CallObjectMethod(env, obj, _client_add_op, d->reqid, dfrd);
    ERROR_CHECK(0);
    return dfrd;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key_attributes__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, const struct hyperdex_client_attribute* attrs, size_t attrs_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key, jobject attributes);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key_attributes__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, const struct hyperdex_client_attribute* attrs, size_t attrs_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key, jobject attributes)
{
    const char* in_space;
    const char* in_key;
    size_t in_key_sz;
    const struct hyperdex_client_attribute* in_attrs;
    size_t in_attrs_sz;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject dfrd = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct hyperdex_java_client_deferred* d = NULL;
    ERROR_CHECK(0);
    d = hyperdex_get_deferred_ptr(env, dfrd);
    ERROR_CHECK(0);
    success = hyperdex_java_client_convert_spacename(env, obj, d->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_key(env, obj, d->arena, key, &in_key, &in_key_sz);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_attributes(env, obj, d->arena, attributes, &in_attrs, &in_attrs_sz);
    if (success < 0) return 0;
    d->reqid = f(client, in_space, in_key, in_key_sz, in_attrs, in_attrs_sz, &d->status);

    if (d->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }

    d->encode_return = hyperdex_java_client_deferred_encode_status;
    (*env)->CallObjectMethod(env, obj, _client_add_op, d->reqid, dfrd);
    ERROR_CHECK(0);
    return dfrd;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, const struct hyperdex_client_attribute* attrs, size_t attrs_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key, jobject predicates, jobject attributes);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, const struct hyperdex_client_attribute* attrs, size_t attrs_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    const char* in_space;
    const char* in_key;
    size_t in_key_sz;
    const struct hyperdex_client_attribute_check* in_checks;
    size_t in_checks_sz;
    const struct hyperdex_client_attribute* in_attrs;
    size_t in_attrs_sz;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject dfrd = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct hyperdex_java_client_deferred* d = NULL;
    ERROR_CHECK(0);
    d = hyperdex_get_deferred_ptr(env, dfrd);
    ERROR_CHECK(0);
    success = hyperdex_java_client_convert_spacename(env, obj, d->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_key(env, obj, d->arena, key, &in_key, &in_key_sz);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_predicates(env, obj, d->arena, predicates, &in_checks, &in_checks_sz);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_attributes(env, obj, d->arena, attributes, &in_attrs, &in_attrs_sz);
    if (success < 0) return 0;
    d->reqid = f(client, in_space, in_key, in_key_sz, in_checks, in_checks_sz, in_attrs, in_attrs_sz, &d->status);

    if (d->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }

    d->encode_return = hyperdex_java_client_deferred_encode_status;
    (*env)->CallObjectMethod(env, obj, _client_add_op, d->reqid, dfrd);
    ERROR_CHECK(0);
    return dfrd;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key)
{
    const char* in_space;
    const char* in_key;
    size_t in_key_sz;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject dfrd = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct hyperdex_java_client_deferred* d = NULL;
    ERROR_CHECK(0);
    d = hyperdex_get_deferred_ptr(env, dfrd);
    ERROR_CHECK(0);
    success = hyperdex_java_client_convert_spacename(env, obj, d->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_key(env, obj, d->arena, key, &in_key, &in_key_sz);
    if (success < 0) return 0;
    d->reqid = f(client, in_space, in_key, in_key_sz, &d->status);

    if (d->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }

    d->encode_return = hyperdex_java_client_deferred_encode_status;
    (*env)->CallObjectMethod(env, obj, _client_add_op, d->reqid, dfrd);
    ERROR_CHECK(0);
    return dfrd;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key_predicates__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key, jobject predicates);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key_predicates__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key, jobject predicates)
{
    const char* in_space;
    const char* in_key;
    size_t in_key_sz;
    const struct hyperdex_client_attribute_check* in_checks;
    size_t in_checks_sz;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject dfrd = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct hyperdex_java_client_deferred* d = NULL;
    ERROR_CHECK(0);
    d = hyperdex_get_deferred_ptr(env, dfrd);
    ERROR_CHECK(0);
    success = hyperdex_java_client_convert_spacename(env, obj, d->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_key(env, obj, d->arena, key, &in_key, &in_key_sz);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_predicates(env, obj, d->arena, predicates, &in_checks, &in_checks_sz);
    if (success < 0) return 0;
    d->reqid = f(client, in_space, in_key, in_key_sz, in_checks, in_checks_sz, &d->status);

    if (d->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }

    d->encode_return = hyperdex_java_client_deferred_encode_status;
    (*env)->CallObjectMethod(env, obj, _client_add_op, d->reqid, dfrd);
    ERROR_CHECK(0);
    return dfrd;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key_mapattributes__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key, jobject mapattributes);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key_mapattributes__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key, jobject mapattributes)
{
    const char* in_space;
    const char* in_key;
    size_t in_key_sz;
    const struct hyperdex_client_map_attribute* in_mapattrs;
    size_t in_mapattrs_sz;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject dfrd = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct hyperdex_java_client_deferred* d = NULL;
    ERROR_CHECK(0);
    d = hyperdex_get_deferred_ptr(env, dfrd);
    ERROR_CHECK(0);
    success = hyperdex_java_client_convert_spacename(env, obj, d->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_key(env, obj, d->arena, key, &in_key, &in_key_sz);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_mapattributes(env, obj, d->arena, mapattributes, &in_mapattrs, &in_mapattrs_sz);
    if (success < 0) return 0;
    d->reqid = f(client, in_space, in_key, in_key_sz, in_mapattrs, in_mapattrs_sz, &d->status);

    if (d->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }

    d->encode_return = hyperdex_java_client_deferred_encode_status;
    (*env)->CallObjectMethod(env, obj, _client_add_op, d->reqid, dfrd);
    ERROR_CHECK(0);
    return dfrd;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key, jobject predicates, jobject mapattributes);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const char* key, size_t key_sz, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    const char* in_space;
    const char* in_key;
    size_t in_key_sz;
    const struct hyperdex_client_attribute_check* in_checks;
    size_t in_checks_sz;
    const struct hyperdex_client_map_attribute* in_mapattrs;
    size_t in_mapattrs_sz;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject dfrd = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct hyperdex_java_client_deferred* d = NULL;
    ERROR_CHECK(0);
    d = hyperdex_get_deferred_ptr(env, dfrd);
    ERROR_CHECK(0);
    success = hyperdex_java_client_convert_spacename(env, obj, d->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_key(env, obj, d->arena, key, &in_key, &in_key_sz);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_predicates(env, obj, d->arena, predicates, &in_checks, &in_checks_sz);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_mapattributes(env, obj, d->arena, mapattributes, &in_mapattrs, &in_mapattrs_sz);
    if (success < 0) return 0;
    d->reqid = f(client, in_space, in_key, in_key_sz, in_checks, in_checks_sz, in_mapattrs, in_mapattrs_sz, &d->status);

    if (d->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }

    d->encode_return = hyperdex_java_client_deferred_encode_status;
    (*env)->CallObjectMethod(env, obj, _client_add_op, d->reqid, dfrd);
    ERROR_CHECK(0);
    return dfrd;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_iterator__spacename_predicates__status_attributes(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, enum hyperdex_client_returncode* status, const struct hyperdex_client_attribute** attrs, size_t* attrs_sz), jstring spacename, jobject predicates);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_iterator__spacename_predicates__status_attributes(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, enum hyperdex_client_returncode* status, const struct hyperdex_client_attribute** attrs, size_t* attrs_sz), jstring spacename, jobject predicates)
{
    const char* in_space;
    const struct hyperdex_client_attribute_check* in_checks;
    size_t in_checks_sz;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject iter = (*env)->NewObject(env, _iterator, _iterator_init, obj);
    struct hyperdex_java_client_iterator* it = NULL;
    ERROR_CHECK(0);
    it = hyperdex_get_iterator_ptr(env, iter);
    ERROR_CHECK(0);
    hyperdex_java_client_convert_spacename(env, obj, it->arena, spacename, &in_space);
    if (success < 0) return 0;
    hyperdex_java_client_convert_predicates(env, obj, it->arena, predicates, &in_checks, &in_checks_sz);
    if (success < 0) return 0;
    it->reqid = f(client, in_space, in_checks, in_checks_sz, &it->status, &it->attrs, &it->attrs_sz);

    if (it->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, it->status, hyperdex_client_error_message(client));
        return 0;
    }

    it->encode_return = hyperdex_java_client_iterator_encode_status_attributes;
    (*env)->CallObjectMethod(env, obj, _client_add_op, it->reqid, iter);
    ERROR_CHECK(0);
    return iter;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_predicates__status_description(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, enum hyperdex_client_returncode* status, const char** description), jstring spacename, jobject predicates);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_predicates__status_description(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, enum hyperdex_client_returncode* status, const char** description), jstring spacename, jobject predicates)
{
    const char* in_space;
    const struct hyperdex_client_attribute_check* in_checks;
    size_t in_checks_sz;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject dfrd = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct hyperdex_java_client_deferred* d = NULL;
    ERROR_CHECK(0);
    d = hyperdex_get_deferred_ptr(env, dfrd);
    ERROR_CHECK(0);
    success = hyperdex_java_client_convert_spacename(env, obj, d->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_predicates(env, obj, d->arena, predicates, &in_checks, &in_checks_sz);
    if (success < 0) return 0;
    d->reqid = f(client, in_space, in_checks, in_checks_sz, &d->status, &d->description);

    if (d->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }

    d->encode_return = hyperdex_java_client_deferred_encode_status_description;
    (*env)->CallObjectMethod(env, obj, _client_add_op, d->reqid, dfrd);
    ERROR_CHECK(0);
    return dfrd;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_iterator__spacename_predicates_sortby_limit_maxmin__status_attributes(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, const char* sort_by, uint64_t limit, int maxmin, enum hyperdex_client_returncode* status, const struct hyperdex_client_attribute** attrs, size_t* attrs_sz), jstring spacename, jobject predicates, jstring sortby, jint limit, jboolean maxmin);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_iterator__spacename_predicates_sortby_limit_maxmin__status_attributes(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, const char* sort_by, uint64_t limit, int maxmin, enum hyperdex_client_returncode* status, const struct hyperdex_client_attribute** attrs, size_t* attrs_sz), jstring spacename, jobject predicates, jstring sortby, jint limit, jboolean maxmin)
{
    const char* in_space;
    const struct hyperdex_client_attribute_check* in_checks;
    size_t in_checks_sz;
    const char* in_sort_by;
    uint64_t in_limit;
    int in_maxmin;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject iter = (*env)->NewObject(env, _iterator, _iterator_init, obj);
    struct hyperdex_java_client_iterator* it = NULL;
    ERROR_CHECK(0);
    it = hyperdex_get_iterator_ptr(env, iter);
    ERROR_CHECK(0);
    hyperdex_java_client_convert_spacename(env, obj, it->arena, spacename, &in_space);
    if (success < 0) return 0;
    hyperdex_java_client_convert_predicates(env, obj, it->arena, predicates, &in_checks, &in_checks_sz);
    if (success < 0) return 0;
    hyperdex_java_client_convert_sortby(env, obj, it->arena, sortby, &in_sort_by);
    if (success < 0) return 0;
    hyperdex_java_client_convert_limit(env, obj, it->arena, limit, &in_limit);
    if (success < 0) return 0;
    hyperdex_java_client_convert_maxmin(env, obj, it->arena, maxmin, &in_maxmin);
    if (success < 0) return 0;
    it->reqid = f(client, in_space, in_checks, in_checks_sz, in_sort_by, in_limit, in_maxmin, &it->status, &it->attrs, &it->attrs_sz);

    if (it->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, it->status, hyperdex_client_error_message(client));
        return 0;
    }

    it->encode_return = hyperdex_java_client_iterator_encode_status_attributes;
    (*env)->CallObjectMethod(env, obj, _client_add_op, it->reqid, iter);
    ERROR_CHECK(0);
    return iter;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_predicates__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject predicates);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_predicates__status(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, enum hyperdex_client_returncode* status), jstring spacename, jobject predicates)
{
    const char* in_space;
    const struct hyperdex_client_attribute_check* in_checks;
    size_t in_checks_sz;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject dfrd = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct hyperdex_java_client_deferred* d = NULL;
    ERROR_CHECK(0);
    d = hyperdex_get_deferred_ptr(env, dfrd);
    ERROR_CHECK(0);
    success = hyperdex_java_client_convert_spacename(env, obj, d->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_predicates(env, obj, d->arena, predicates, &in_checks, &in_checks_sz);
    if (success < 0) return 0;
    d->reqid = f(client, in_space, in_checks, in_checks_sz, &d->status);

    if (d->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }

    d->encode_return = hyperdex_java_client_deferred_encode_status;
    (*env)->CallObjectMethod(env, obj, _client_add_op, d->reqid, dfrd);
    ERROR_CHECK(0);
    return dfrd;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_predicates__status_count(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, enum hyperdex_client_returncode* status, uint64_t* count), jstring spacename, jobject predicates);

JNIEXPORT HYPERDEX_API jobject JNICALL
_hyperdex_java_client_asynccall__spacename_predicates__status_count(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, const char* space, const struct hyperdex_client_attribute_check* checks, size_t checks_sz, enum hyperdex_client_returncode* status, uint64_t* count), jstring spacename, jobject predicates)
{
    const char* in_space;
    const struct hyperdex_client_attribute_check* in_checks;
    size_t in_checks_sz;
    int success = 0;
    struct hyperdex_client* client = hyperdex_get_client_ptr(env, obj);
    jobject dfrd = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct hyperdex_java_client_deferred* d = NULL;
    ERROR_CHECK(0);
    d = hyperdex_get_deferred_ptr(env, dfrd);
    ERROR_CHECK(0);
    success = hyperdex_java_client_convert_spacename(env, obj, d->arena, spacename, &in_space);
    if (success < 0) return 0;
    success = hyperdex_java_client_convert_predicates(env, obj, d->arena, predicates, &in_checks, &in_checks_sz);
    if (success < 0) return 0;
    d->reqid = f(client, in_space, in_checks, in_checks_sz, &d->status, &d->count);

    if (d->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }

    d->encode_return = hyperdex_java_client_deferred_encode_status_count;
    (*env)->CallObjectMethod(env, obj, _client_add_op, d->reqid, dfrd);
    ERROR_CHECK(0);
    return dfrd;
}


JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1get(JNIEnv* env, jobject obj, jstring spacename, jobject key)
{
    return _hyperdex_java_client_asynccall__spacename_key__status_attributes(env, obj, hyperdex_client_get, spacename, key);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1put(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_put, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1put(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_put, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1put_1if_1not_1exist(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_put_if_not_exist, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1del(JNIEnv* env, jobject obj, jstring spacename, jobject key)
{
    return _hyperdex_java_client_asynccall__spacename_key__status(env, obj, hyperdex_client_del, spacename, key);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1del(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates__status(env, obj, hyperdex_client_cond_del, spacename, key, predicates);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1atomic_1add(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_atomic_add, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1atomic_1add(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_atomic_add, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1atomic_1sub(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_atomic_sub, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1atomic_1sub(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_atomic_sub, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1atomic_1mul(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_atomic_mul, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1atomic_1mul(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_atomic_mul, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1atomic_1div(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_atomic_div, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1atomic_1div(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_atomic_div, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1atomic_1mod(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_atomic_mod, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1atomic_1mod(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_atomic_mod, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1atomic_1and(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_atomic_and, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1atomic_1and(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_atomic_and, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1atomic_1or(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_atomic_or, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1atomic_1or(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_atomic_or, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1atomic_1xor(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_atomic_xor, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1atomic_1xor(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_atomic_xor, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1string_1prepend(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_string_prepend, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1string_1prepend(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_string_prepend, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1string_1append(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_string_append, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1string_1append(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_string_append, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1list_1lpush(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_list_lpush, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1list_1lpush(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_list_lpush, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1list_1rpush(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_list_rpush, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1list_1rpush(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_list_rpush, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1set_1add(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_set_add, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1set_1add(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_set_add, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1set_1remove(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_set_remove, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1set_1remove(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_set_remove, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1set_1intersect(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_set_intersect, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1set_1intersect(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_set_intersect, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1set_1union(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_set_union, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1set_1union(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_set_union, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1add(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_mapattributes__status(env, obj, hyperdex_client_map_add, spacename, key, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1add(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(env, obj, hyperdex_client_cond_map_add, spacename, key, predicates, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1remove(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_attributes__status(env, obj, hyperdex_client_map_remove, spacename, key, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1remove(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject attributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_attributes__status(env, obj, hyperdex_client_cond_map_remove, spacename, key, predicates, attributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1atomic_1add(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_mapattributes__status(env, obj, hyperdex_client_map_atomic_add, spacename, key, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1atomic_1add(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(env, obj, hyperdex_client_cond_map_atomic_add, spacename, key, predicates, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1atomic_1sub(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_mapattributes__status(env, obj, hyperdex_client_map_atomic_sub, spacename, key, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1atomic_1sub(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(env, obj, hyperdex_client_cond_map_atomic_sub, spacename, key, predicates, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1atomic_1mul(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_mapattributes__status(env, obj, hyperdex_client_map_atomic_mul, spacename, key, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1atomic_1mul(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(env, obj, hyperdex_client_cond_map_atomic_mul, spacename, key, predicates, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1atomic_1div(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_mapattributes__status(env, obj, hyperdex_client_map_atomic_div, spacename, key, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1atomic_1div(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(env, obj, hyperdex_client_cond_map_atomic_div, spacename, key, predicates, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1atomic_1mod(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_mapattributes__status(env, obj, hyperdex_client_map_atomic_mod, spacename, key, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1atomic_1mod(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(env, obj, hyperdex_client_cond_map_atomic_mod, spacename, key, predicates, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1atomic_1and(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_mapattributes__status(env, obj, hyperdex_client_map_atomic_and, spacename, key, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1atomic_1and(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(env, obj, hyperdex_client_cond_map_atomic_and, spacename, key, predicates, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1atomic_1or(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_mapattributes__status(env, obj, hyperdex_client_map_atomic_or, spacename, key, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1atomic_1or(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(env, obj, hyperdex_client_cond_map_atomic_or, spacename, key, predicates, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1atomic_1xor(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_mapattributes__status(env, obj, hyperdex_client_map_atomic_xor, spacename, key, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1atomic_1xor(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(env, obj, hyperdex_client_cond_map_atomic_xor, spacename, key, predicates, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1string_1prepend(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_mapattributes__status(env, obj, hyperdex_client_map_string_prepend, spacename, key, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1string_1prepend(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(env, obj, hyperdex_client_cond_map_string_prepend, spacename, key, predicates, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1map_1string_1append(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_mapattributes__status(env, obj, hyperdex_client_map_string_append, spacename, key, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1cond_1map_1string_1append(JNIEnv* env, jobject obj, jstring spacename, jobject key, jobject predicates, jobject mapattributes)
{
    return _hyperdex_java_client_asynccall__spacename_key_predicates_mapattributes__status(env, obj, hyperdex_client_cond_map_string_append, spacename, key, predicates, mapattributes);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_search(JNIEnv* env, jobject obj, jstring spacename, jobject predicates)
{
    return _hyperdex_java_client_iterator__spacename_predicates__status_attributes(env, obj, hyperdex_client_search, spacename, predicates);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1search_1describe(JNIEnv* env, jobject obj, jstring spacename, jobject predicates)
{
    return _hyperdex_java_client_asynccall__spacename_predicates__status_description(env, obj, hyperdex_client_search_describe, spacename, predicates);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_sorted_1search(JNIEnv* env, jobject obj, jstring spacename, jobject predicates, jstring sortby, jint limit, jboolean maxmin)
{
    return _hyperdex_java_client_iterator__spacename_predicates_sortby_limit_maxmin__status_attributes(env, obj, hyperdex_client_sorted_search, spacename, predicates, sortby, limit, maxmin);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1group_1del(JNIEnv* env, jobject obj, jstring spacename, jobject predicates)
{
    return _hyperdex_java_client_asynccall__spacename_predicates__status(env, obj, hyperdex_client_group_del, spacename, predicates);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Client_async_1count(JNIEnv* env, jobject obj, jstring spacename, jobject predicates)
{
    return _hyperdex_java_client_asynccall__spacename_predicates__status_count(env, obj, hyperdex_client_count, spacename, predicates);
}
