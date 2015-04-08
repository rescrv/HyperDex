/* Copyright (c) 2013-2015, Cornell University
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
#include <string.h>

/* HyperDex */
#include <hyperdex.h>
#include <hyperdex/client.h>
#include <hyperdex/datastructures.h>
#include "visibility.h"
#include "bindings/java/org_hyperdex_client_Client.h"
#include "bindings/java/org_hyperdex_client_Deferred.h"
#include "bindings/java/org_hyperdex_client_GreaterEqual.h"
#include "bindings/java/org_hyperdex_client_GreaterThan.h"
#include "bindings/java/org_hyperdex_client_Iterator.h"
#include "bindings/java/org_hyperdex_client_LengthEquals.h"
#include "bindings/java/org_hyperdex_client_LengthGreaterEqual.h"
#include "bindings/java/org_hyperdex_client_LengthLessEqual.h"
#include "bindings/java/org_hyperdex_client_LessEqual.h"
#include "bindings/java/org_hyperdex_client_LessThan.h"
#include "bindings/java/org_hyperdex_client_Range.h"
#include "bindings/java/org_hyperdex_client_Regex.h"

#include "client-util.h"

/********************************* Cached IDs *********************************/

static jclass _string;

static jclass _byte_string;
static jmethodID _byte_string_init;
static jmethodID _byte_string_get;
static jmethodID _byte_string_to_string;

static jclass _document;
static jmethodID _document_init;
static jmethodID _document_to_string;

static jclass _boolean;
static jmethodID _boolean_init;

static jclass _long;
static jmethodID _long_init;
static jmethodID _long_longValue;

static jclass _integer;
static jmethodID _integer_init;
static jmethodID _integer_intValue;

static jclass _double;
static jmethodID _double_init;
static jmethodID _double_doubleValue;

static jclass _list;
static jmethodID _list_iterator;
static jmethodID _list_get;
static jmethodID _list_size;

static jclass _array_list;
static jmethodID _array_list_init;
static jmethodID _array_list_add;

static jclass _set;
static jmethodID _set_iterator;

static jclass _hash_set;
static jmethodID _hash_set_init;
static jmethodID _hash_set_add;

static jclass _map;
static jmethodID _map_entrySet;
static jmethodID _map_size;

static jclass _map_entry;
static jmethodID _map_entry_getKey;
static jmethodID _map_entry_getValue;

static jclass _hash_map;
static jmethodID _hash_map_init;
static jmethodID _hash_map_put;

static jclass _java_iterator;
static jmethodID _java_iterator_hasNext;
static jmethodID _java_iterator_next;

static jclass _hd_except;
static jmethodID _hd_except_init;

static jclass _predicate;
static jmethodID _predicate_checksSize;
static jmethodID _predicate_convertChecks;

static jclass _deferred;
static jfieldID _deferred_c;
static jfieldID _deferred_ptr;
static jmethodID _deferred_init;
static jmethodID _deferred_loop;

static jclass _iterator;
static jfieldID _iterator_c;
static jfieldID _iterator_ptr;
static jmethodID _iterator_init;
static jmethodID _iterator_appendBacklogged;

static jclass _client;
static jfieldID _client_ptr;
static jmethodID _client_add_op;
static jmethodID _client_remove_op;

static jclass _pred_less_equal;
static jfieldID _pred_less_equal_x;

static jclass _pred_greater_equal;
static jfieldID _pred_greater_equal_x;

static jclass _pred_range;
static jfieldID _pred_range_x;
static jfieldID _pred_range_y;

static jclass _pred_regex;
static jfieldID _pred_regex_x;

static jclass _pred_length_equals;
static jfieldID _pred_length_equals_x;

static jclass _pred_length_less_equal;
static jfieldID _pred_length_less_equal_x;

static jclass _pred_length_greater_equal;
static jfieldID _pred_length_greater_equal_x;

#define CHECK_CACHE(X) assert((X))

#define ERROR_CHECK(RET) if ((*env)->ExceptionCheck(env) == JNI_TRUE) return (RET)
#define ERROR_CHECK_VOID() if ((*env)->ExceptionCheck(env) == JNI_TRUE) return

#define REF(NAME, DEF) \
    tmp_cls = (DEF); \
    NAME = (jclass) (*env)->NewGlobalRef(env, tmp_cls); \
    (*env)->DeleteLocalRef(env, tmp_cls);

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Client_initialize(JNIEnv* env, jclass client)
{
    jclass tmp_cls;

    /* cache class String */
    REF(_string, (*env)->FindClass(env, "java/lang/String"));
    /* cache class Document */
    REF(_document, (*env)->FindClass(env, "org/hyperdex/client/Document"));
    _document_init = (*env)->GetMethodID(env, _document, "<init>", "(Lorg/hyperdex/client/ByteString;)V");
    _document_to_string = (*env)->GetMethodID(env, _document, "toString", "()Ljava/lang/String;");
    /* cache class ByteString */
    REF(_byte_string, (*env)->FindClass(env, "org/hyperdex/client/ByteString"));
    _byte_string_init = (*env)->GetMethodID(env, _byte_string, "<init>", "([B)V");
    _byte_string_get = (*env)->GetMethodID(env, _byte_string, "getBytes", "()[B");
    _byte_string_to_string = (*env)->GetMethodID(env, _byte_string, "toString", "()Ljava/lang/String;");
    /* cache class Boolean */
    REF(_boolean, (*env)->FindClass(env, "java/lang/Boolean"));
    _boolean_init = (*env)->GetMethodID(env, _boolean, "<init>", "(Z)V");
    /* cache class Integer */
    REF(_long, (*env)->FindClass(env, "java/lang/Long"));
    _long_init = (*env)->GetMethodID(env, _long, "<init>", "(J)V");
    _long_longValue = (*env)->GetMethodID(env, _long, "longValue", "()J");
    /* cache class Integer */
    REF(_integer, (*env)->FindClass(env, "java/lang/Integer"));
    _integer_init = (*env)->GetMethodID(env, _integer, "<init>", "(I)V");
    _integer_intValue = (*env)->GetMethodID(env, _integer, "intValue", "()I");
    /* cache class Double */
    REF(_double, (*env)->FindClass(env, "java/lang/Double"));
    _double_init = (*env)->GetMethodID(env, _double, "<init>", "(D)V");
    _double_doubleValue = (*env)->GetMethodID(env, _double, "doubleValue", "()D");
    /* cache class List */
    REF(_list, (*env)->FindClass(env, "java/util/List"));
    _list_iterator = (*env)->GetMethodID(env, _list, "iterator", "()Ljava/util/Iterator;");
    _list_get = (*env)->GetMethodID(env, _list, "get", "(I)Ljava/lang/Object;");
    _list_size = (*env)->GetMethodID(env, _list, "size", "()I");
    /* cache class ArrayList */
    REF(_array_list, (*env)->FindClass(env, "java/util/ArrayList"));
    _array_list_init = (*env)->GetMethodID(env, _array_list, "<init>", "()V");
    _array_list_add = (*env)->GetMethodID(env, _array_list, "add", "(Ljava/lang/Object;)Z");
    /* cache class Set */
    REF(_set, (*env)->FindClass(env, "java/util/Set"));
    _set_iterator = (*env)->GetMethodID(env, _set, "iterator", "()Ljava/util/Iterator;");
    /* cache class HashSet */
    REF(_hash_set, (*env)->FindClass(env, "java/util/HashSet"));
    _hash_set_init = (*env)->GetMethodID(env, _hash_set, "<init>", "()V");
    _hash_set_add = (*env)->GetMethodID(env, _hash_set, "add", "(Ljava/lang/Object;)Z");
    /* cache class Map */
    REF(_map, (*env)->FindClass(env, "java/util/Map"));
    _map_entrySet = (*env)->GetMethodID(env, _map, "entrySet", "()Ljava/util/Set;");
    _map_size = (*env)->GetMethodID(env, _map, "size", "()I");
    /* cache class Map$Entry */
    REF(_map_entry, (*env)->FindClass(env, "java/util/Map$Entry"));
    _map_entry_getKey = (*env)->GetMethodID(env, _map_entry, "getKey", "()Ljava/lang/Object;");
    _map_entry_getValue = (*env)->GetMethodID(env, _map_entry, "getValue", "()Ljava/lang/Object;");
    /* cache class HashMap */
    REF(_hash_map, (*env)->FindClass(env, "java/util/HashMap"));
    _hash_map_init = (*env)->GetMethodID(env, _hash_map, "<init>", "()V");
    _hash_map_put = (*env)->GetMethodID(env, _hash_map, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    /* cache class Iterator */
    REF(_java_iterator, (*env)->FindClass(env, "java/util/Iterator"));
    _java_iterator_hasNext = (*env)->GetMethodID(env, _java_iterator, "hasNext", "()Z");
    _java_iterator_next = (*env)->GetMethodID(env, _java_iterator, "next", "()Ljava/lang/Object;");
    /* cache class HyperDexException */
    REF(_hd_except, (*env)->FindClass(env, "org/hyperdex/client/HyperDexClientException"));
    _hd_except_init = (*env)->GetMethodID(env, _hd_except, "<init>", "(JLjava/lang/String;Ljava/lang/String;)V");
    /* cache class Predicate */
    REF(_predicate, (*env)->FindClass(env, "org/hyperdex/client/Predicate"));
    _predicate_checksSize = (*env)->GetMethodID(env, _predicate, "checksSize", "()J");
    _predicate_convertChecks = (*env)->GetMethodID(env, _predicate, "convertChecks", "(JJJ)J");
    /* cache class Deferred */
    REF(_deferred, (*env)->FindClass(env, "org/hyperdex/client/Deferred"));
    _deferred_c = (*env)->GetFieldID(env, _deferred, "c", "Lorg/hyperdex/client/Client;");
    _deferred_ptr = (*env)->GetFieldID(env, _deferred, "ptr", "J");
    _deferred_init = (*env)->GetMethodID(env, _deferred, "<init>", "(Lorg/hyperdex/client/Client;)V");
    _deferred_loop = (*env)->GetMethodID(env, _deferred, "loop", "()V");
    /* cache class Iterator */
    REF(_iterator, (*env)->FindClass(env, "org/hyperdex/client/Iterator"));
    _iterator_c = (*env)->GetFieldID(env, _iterator, "c", "Lorg/hyperdex/client/Client;");
    _iterator_ptr = (*env)->GetFieldID(env, _iterator, "ptr", "J");
    _iterator_init = (*env)->GetMethodID(env, _iterator, "<init>", "(Lorg/hyperdex/client/Client;)V");
    _iterator_appendBacklogged = (*env)->GetMethodID(env, _iterator, "appendBacklogged", "(Ljava/lang/Object;)V");
    /* cache class Client */
    REF(_client, (*env)->FindClass(env, "org/hyperdex/client/Client"));
    _client_ptr = (*env)->GetFieldID(env, _client, "ptr", "J");
    _client_add_op = (*env)->GetMethodID(env, _client, "add_op", "(JLorg/hyperdex/client/Operation;)V");
    _client_remove_op = (*env)->GetMethodID(env, _client, "remove_op", "(J)V");
    /* cache class LessEqual */
    REF(_pred_less_equal, (*env)->FindClass(env, "org/hyperdex/client/LessEqual"));
    _pred_less_equal_x = (*env)->GetFieldID(env, _pred_less_equal, "x", "Ljava/lang/Object;");
    /* cache class GreaterEqual */
    REF(_pred_greater_equal, (*env)->FindClass(env, "org/hyperdex/client/GreaterEqual"));
    _pred_greater_equal_x = (*env)->GetFieldID(env, _pred_greater_equal, "x", "Ljava/lang/Object;");
    /* cache class Range */
    REF(_pred_range, (*env)->FindClass(env, "org/hyperdex/client/Range"));
    _pred_range_x = (*env)->GetFieldID(env, _pred_range, "x", "Ljava/lang/Object;");
    _pred_range_y = (*env)->GetFieldID(env, _pred_range, "y", "Ljava/lang/Object;");
    /* cache class Regex */
    REF(_pred_regex, (*env)->FindClass(env, "org/hyperdex/client/Regex"));
    _pred_regex_x = (*env)->GetFieldID(env, _pred_regex, "x", "Ljava/lang/String;");
    /* cache class LengthEquals */
    REF(_pred_length_equals, (*env)->FindClass(env, "org/hyperdex/client/LengthEquals"));
    _pred_length_equals_x = (*env)->GetFieldID(env, _pred_length_equals, "x", "Ljava/lang/Object;");
    /* cache class LengthLessEqual */
    REF(_pred_length_less_equal, (*env)->FindClass(env, "org/hyperdex/client/LengthLessEqual"));
    _pred_length_less_equal_x = (*env)->GetFieldID(env, _pred_length_less_equal, "x", "Ljava/lang/Object;");
    /* cache class LengthGreaterEqual */
    REF(_pred_length_greater_equal, (*env)->FindClass(env, "org/hyperdex/client/LengthGreaterEqual"));
    _pred_length_greater_equal_x = (*env)->GetFieldID(env, _pred_length_greater_equal, "x", "Ljava/lang/Object;");

    CHECK_CACHE(_string);
    CHECK_CACHE(_document);
    CHECK_CACHE(_document_init);
    CHECK_CACHE(_document_to_string);
    CHECK_CACHE(_byte_string);
    CHECK_CACHE(_byte_string_init);
    CHECK_CACHE(_byte_string_get);
    CHECK_CACHE(_byte_string_to_string);
    CHECK_CACHE(_boolean);
    CHECK_CACHE(_boolean_init);
    CHECK_CACHE(_long);
    CHECK_CACHE(_long_init);
    CHECK_CACHE(_long_longValue);
    CHECK_CACHE(_integer);
    CHECK_CACHE(_integer_init);
    CHECK_CACHE(_integer_intValue);
    CHECK_CACHE(_double);
    CHECK_CACHE(_double_init);
    CHECK_CACHE(_double_doubleValue);
    CHECK_CACHE(_list);
    CHECK_CACHE(_list_iterator);
    CHECK_CACHE(_list_get);
    CHECK_CACHE(_list_size);
    CHECK_CACHE(_array_list);
    CHECK_CACHE(_array_list_init);
    CHECK_CACHE(_array_list_add);
    CHECK_CACHE(_set);
    CHECK_CACHE(_set_iterator);
    CHECK_CACHE(_hash_set);
    CHECK_CACHE(_hash_set_init);
    CHECK_CACHE(_hash_set_add);
    CHECK_CACHE(_map);
    CHECK_CACHE(_map_entrySet);
    CHECK_CACHE(_map_size);
    CHECK_CACHE(_map_entry);
    CHECK_CACHE(_map_entry_getKey);
    CHECK_CACHE(_map_entry_getValue);
    CHECK_CACHE(_hash_map);
    CHECK_CACHE(_hash_map_init);
    CHECK_CACHE(_hash_map_put);
    CHECK_CACHE(_java_iterator);
    CHECK_CACHE(_java_iterator_hasNext);
    CHECK_CACHE(_java_iterator_next);
    CHECK_CACHE(_hd_except);
    CHECK_CACHE(_hd_except_init);
    CHECK_CACHE(_predicate);
    CHECK_CACHE(_predicate_checksSize);
    CHECK_CACHE(_predicate_convertChecks);
    CHECK_CACHE(_deferred);
    CHECK_CACHE(_deferred_c);
    CHECK_CACHE(_deferred_ptr);
    CHECK_CACHE(_deferred_init);
    CHECK_CACHE(_deferred_loop);
    CHECK_CACHE(_iterator);
    CHECK_CACHE(_iterator_c);
    CHECK_CACHE(_iterator_ptr);
    CHECK_CACHE(_iterator_init);
    CHECK_CACHE(_iterator_appendBacklogged);
    CHECK_CACHE(_client);
    CHECK_CACHE(_client_ptr);
    CHECK_CACHE(_client_add_op);
    CHECK_CACHE(_client_remove_op);
    CHECK_CACHE(_pred_less_equal);
    CHECK_CACHE(_pred_less_equal_x);
    CHECK_CACHE(_pred_greater_equal);
    CHECK_CACHE(_pred_greater_equal_x);
    CHECK_CACHE(_pred_range);
    CHECK_CACHE(_pred_range_x);
    CHECK_CACHE(_pred_range_y);
    CHECK_CACHE(_pred_regex);
    CHECK_CACHE(_pred_regex_x);
    CHECK_CACHE(_pred_length_equals);
    CHECK_CACHE(_pred_length_equals_x);
    CHECK_CACHE(_pred_length_less_equal);
    CHECK_CACHE(_pred_length_less_equal_x);
    CHECK_CACHE(_pred_length_greater_equal);
    CHECK_CACHE(_pred_length_greater_equal_x);

    (void) client;
}

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Client_terminate(JNIEnv* env, jclass client)
{
    (*env)->DeleteGlobalRef(env, _document);
    (*env)->DeleteGlobalRef(env, _string);
    (*env)->DeleteGlobalRef(env, _byte_string);
    (*env)->DeleteGlobalRef(env, _boolean);
    (*env)->DeleteGlobalRef(env, _long);
    (*env)->DeleteGlobalRef(env, _integer);
    (*env)->DeleteGlobalRef(env, _double);
    (*env)->DeleteGlobalRef(env, _list);
    (*env)->DeleteGlobalRef(env, _array_list);
    (*env)->DeleteGlobalRef(env, _set);
    (*env)->DeleteGlobalRef(env, _hash_set);
    (*env)->DeleteGlobalRef(env, _map);
    (*env)->DeleteGlobalRef(env, _map_entry);
    (*env)->DeleteGlobalRef(env, _hash_map);
    (*env)->DeleteGlobalRef(env, _java_iterator);
    (*env)->DeleteGlobalRef(env, _hd_except);
    (*env)->DeleteGlobalRef(env, _predicate);
    (*env)->DeleteGlobalRef(env, _deferred);
    (*env)->DeleteGlobalRef(env, _iterator);
    (*env)->DeleteGlobalRef(env, _client);
    (*env)->DeleteGlobalRef(env, _pred_less_equal);
    (*env)->DeleteGlobalRef(env, _pred_greater_equal);
    (*env)->DeleteGlobalRef(env, _pred_range);
    (*env)->DeleteGlobalRef(env, _pred_regex);
    (*env)->DeleteGlobalRef(env, _pred_length_equals);
    (*env)->DeleteGlobalRef(env, _pred_length_less_equal);
    (*env)->DeleteGlobalRef(env, _pred_length_greater_equal);

    (void) client;
}

/******************************* Pointer Unwrap *******************************/

static struct hyperdex_client*
hyperdex_get_client_ptr(JNIEnv* env, jobject obj)
{
    struct hyperdex_client* x;
    x = (struct hyperdex_client*) (*env)->GetLongField(env, obj, _client_ptr);
    assert(x);
    return x;
}

static struct hyperdex_java_client_deferred*
hyperdex_get_deferred_ptr(JNIEnv* env, jobject obj)
{
    struct hyperdex_java_client_deferred* x;
    x = (struct hyperdex_java_client_deferred*) (*env)->GetLongField(env, obj, _deferred_ptr);
    assert(x);
    return x;
}

static struct hyperdex_java_client_iterator*
hyperdex_get_iterator_ptr(JNIEnv* env, jobject obj)
{
    struct hyperdex_java_client_iterator* x;
    x = (struct hyperdex_java_client_iterator*) (*env)->GetLongField(env, obj, _iterator_ptr);
    assert(x);
    return x;
}

/******************************* Error Handling *******************************/

static int
hyperdex_java_out_of_memory(JNIEnv* env)
{
    jclass oom;
    jmethodID init;
    jobject exc;

    oom  = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
    ERROR_CHECK(-1);
    init = (*env)->GetMethodID(env, oom, "<init>", "()V");
    ERROR_CHECK(-1);
    exc = (*env)->NewObject(env, oom, init);
    ERROR_CHECK(-1);

    (*env)->ExceptionClear(env);
    (*env)->Throw(env, exc);
    return -1;
}

static jobject
hyperdex_java_client_create_exception(JNIEnv* env,
                                      enum hyperdex_client_returncode _rc,
                                      const char* message)
{
    jlong rc = _rc;
    jstring str = (*env)->NewStringUTF(env, hyperdex_client_returncode_to_string(_rc));
    jstring msg = (*env)->NewStringUTF(env, message);
    jobject err = (*env)->NewObject(env, _hd_except, _hd_except_init, rc, str, msg);
    ERROR_CHECK(0);
    return err;
}

int
hyperdex_java_client_throw_exception(JNIEnv* env,
                                     enum hyperdex_client_returncode _rc,
                                     const char* message)
{
    jobject err = hyperdex_java_client_create_exception(env, _rc, message);
    ERROR_CHECK(-1);
    (*env)->ExceptionClear(env);
    return (*env)->Throw(env, err);
}

/********************************** Java -> C *********************************/
typedef int (*elem_string_fptr)(void*, const char*, size_t, enum hyperdex_ds_returncode*);
typedef int (*elem_int_fptr)(void*, int64_t, enum hyperdex_ds_returncode*);
typedef int (*elem_float_fptr)(void*, double, enum hyperdex_ds_returncode*);

#define HDJAVA_HANDLE_ELEM_ERROR(X, TYPE) \
    switch (X) \
    { \
        case HYPERDEX_DS_NOMEM: \
            hyperdex_java_out_of_memory(env); \
            return -1; \
        case HYPERDEX_DS_MIXED_TYPES: \
            hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_WRONGTYPE, "Cannot add " TYPE " to a heterogenous container"); \
            return -1; \
        case HYPERDEX_DS_SUCCESS: \
        case HYPERDEX_DS_STRING_TOO_LONG: \
        case HYPERDEX_DS_WRONG_STATE: \
        default: \
            hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_WRONGTYPE, "Cannot convert " TYPE " to a HyperDex type"); \
            return -1; \
    }

/* Convert a single elment of a list, map, or set*/
static int
hyperdex_java_client_convert_elem(JNIEnv* env,
                                  jobject obj,
                                  void* container,
                                  elem_string_fptr f_string,
                                  elem_int_fptr f_int,
                                  elem_float_fptr f_float)
{
    enum hyperdex_ds_returncode error;
    int success;
    const char* tmp_str;
    size_t tmp_str_sz;
    jbyte* tmp_bytes;
    size_t tmp_bytes_sz;
    int64_t tmp_l;
    int32_t tmp_i;
    double tmp_d;

    if ((*env)->IsInstanceOf(env, obj, _string) == JNI_TRUE)
    {
        tmp_str = (*env)->GetStringUTFChars(env, obj, 0);
        ERROR_CHECK(-1);
        tmp_str_sz = (*env)->GetStringUTFLength(env, obj);
        ERROR_CHECK(-1);
        success = f_string(container, tmp_str, tmp_str_sz, &error);
        (*env)->ReleaseStringUTFChars(env, obj, tmp_str);
        ERROR_CHECK(-1);

        if (success < 0)
        {
            HDJAVA_HANDLE_ELEM_ERROR(error, "string");
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, obj, _byte_string) == JNI_TRUE)
    {
        obj = (*env)->CallObjectMethod(env, obj, _byte_string_get);
        ERROR_CHECK(-1);
        tmp_bytes = (*env)->GetByteArrayElements(env, obj, 0);
        ERROR_CHECK(-1);
        tmp_bytes_sz = (*env)->GetArrayLength(env, obj);
        ERROR_CHECK(-1);
        success = f_string(container, (const char*)tmp_bytes, tmp_bytes_sz, &error);
        (*env)->ReleaseByteArrayElements(env, obj, tmp_bytes, 0);
        ERROR_CHECK(-1);

        if (success < 0)
        {
            HDJAVA_HANDLE_ELEM_ERROR(error, "bytes");
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, obj, _long) == JNI_TRUE)
    {
        tmp_l = (*env)->CallLongMethod(env, obj, _long_longValue);
        ERROR_CHECK(-1);

        if (f_int(container, tmp_l, &error) < 0)
        {
            HDJAVA_HANDLE_ELEM_ERROR(error, "long");
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, obj, _integer) == JNI_TRUE)
    {
        tmp_i = (*env)->CallIntMethod(env, obj, _integer_intValue);
        ERROR_CHECK(-1);

        if (f_int(container, tmp_i, &error) < 0)
        {
            HDJAVA_HANDLE_ELEM_ERROR(error, "int");
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, obj, _double) == JNI_TRUE)
    {
        tmp_d = (*env)->CallDoubleMethod(env, obj, _double_doubleValue);
        ERROR_CHECK(-1);

        if (f_float(container, tmp_d, &error) < 0)
        {
            HDJAVA_HANDLE_ELEM_ERROR(error, "float");
        }

        return 0;
    }
    else
    {
        hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_WRONGTYPE,
                                             "Cannot convert unknown type to a HyperDex type");
        return -1;
    }
}

static int
hyperdex_java_client_convert_list(JNIEnv* env,
                                  struct hyperdex_ds_arena* arena,
                                  jobject x,
                                  const char** value,
                                  size_t* value_sz,
                                  enum hyperdatatype* datatype)
{
    struct hyperdex_ds_list* list;
    enum hyperdex_ds_returncode error;
    jobject entry;
    jobject it = (*env)->CallObjectMethod(env, x, _list_iterator);
    ERROR_CHECK(-1);

    list = hyperdex_ds_allocate_list(arena);

    if (!list)
    {
        hyperdex_java_out_of_memory(env);
        return -1;
    }

    while ((*env)->CallBooleanMethod(env, it, _java_iterator_hasNext) == JNI_TRUE)
    {
        entry = (*env)->CallObjectMethod(env, it, _java_iterator_next);
        ERROR_CHECK(-1);

        if (hyperdex_java_client_convert_elem(env, entry, list,
                    (elem_string_fptr) hyperdex_ds_list_append_string,
                    (elem_int_fptr) hyperdex_ds_list_append_int,
                    (elem_float_fptr) hyperdex_ds_list_append_float) < 0)
        {
            return -1;
        }

        (*env)->DeleteLocalRef(env, entry);
    }

    if (hyperdex_ds_list_finalize(list, &error, value, value_sz, datatype) < 0)
    {
        hyperdex_java_out_of_memory(env);
        return -1;
    }

    (*env)->DeleteLocalRef(env, it);
    return 0;
}

static int
hyperdex_java_client_convert_set(JNIEnv* env,
                                 struct hyperdex_ds_arena* arena,
                                 jobject x,
                                 const char** value,
                                 size_t* value_sz,
                                 enum hyperdatatype* datatype)
{
    struct hyperdex_ds_set* set;
    enum hyperdex_ds_returncode error;
    jobject it = (*env)->CallObjectMethod(env, x, _set_iterator);
    jobject entry;
    ERROR_CHECK(-1);

    set = hyperdex_ds_allocate_set(arena);

    if (!set)
    {
        hyperdex_java_out_of_memory(env);
        return -1;
    }

    while ((*env)->CallBooleanMethod(env, it, _java_iterator_hasNext) == JNI_TRUE)
    {
        entry = (*env)->CallObjectMethod(env, it, _java_iterator_next);
        ERROR_CHECK(-1);

        if (hyperdex_java_client_convert_elem(env, entry, set,
                    (elem_string_fptr) hyperdex_ds_set_insert_string,
                    (elem_int_fptr) hyperdex_ds_set_insert_int,
                    (elem_float_fptr) hyperdex_ds_set_insert_float) < 0)
        {
            return -1;
        }

        (*env)->DeleteLocalRef(env, entry);
    }

    if (hyperdex_ds_set_finalize(set, &error, value, value_sz, datatype) < 0)
    {
        hyperdex_java_out_of_memory(env);
        return -1;
    }

    (*env)->DeleteLocalRef(env, it);
    return 0;
}

static int
hyperdex_java_client_convert_map(JNIEnv* env,
                                 struct hyperdex_ds_arena* arena,
                                 jobject x,
                                 const char** value,
                                 size_t* value_sz,
                                 enum hyperdatatype* datatype)
{
    struct hyperdex_ds_map* map;
    enum hyperdex_ds_returncode error;
    jobject set;
    jobject it;
    jobject entry;
    jobject key;
    jobject val;

    set = (*env)->CallObjectMethod(env, x, _map_entrySet);
    ERROR_CHECK(-1);
    it = (*env)->CallObjectMethod(env, set, _set_iterator);
    ERROR_CHECK(-1);

    map = hyperdex_ds_allocate_map(arena);

    if (!map)
    {
        return hyperdex_java_out_of_memory(env);
    }

    while ((*env)->CallBooleanMethod(env, it, _java_iterator_hasNext) == JNI_TRUE)
    {
        entry = (*env)->CallObjectMethod(env, it, _java_iterator_next);
        ERROR_CHECK(-1);
        key = (*env)->CallObjectMethod(env, entry, _map_entry_getKey);
        ERROR_CHECK(-1);
        val = (*env)->CallObjectMethod(env, entry, _map_entry_getValue);
        ERROR_CHECK(-1);

        if (hyperdex_java_client_convert_elem(env, key, map,
                    (elem_string_fptr) hyperdex_ds_map_insert_key_string,
                    (elem_int_fptr) hyperdex_ds_map_insert_key_int,
                    (elem_float_fptr) hyperdex_ds_map_insert_key_float) < 0)
        {
            return -1;
        }

        if (hyperdex_java_client_convert_elem(env, val, map,
                    (elem_string_fptr) hyperdex_ds_map_insert_val_string,
                    (elem_int_fptr) hyperdex_ds_map_insert_val_int,
                    (elem_float_fptr) hyperdex_ds_map_insert_val_float) < 0)
        {
            return -1;
        }

        (*env)->DeleteLocalRef(env, entry);
        (*env)->DeleteLocalRef(env, key);
        (*env)->DeleteLocalRef(env, val);
    }

    if (hyperdex_ds_map_finalize(map, &error, value, value_sz, datatype) < 0)
    {
        hyperdex_java_out_of_memory(env);
        return -1;
    }

    (*env)->DeleteLocalRef(env, it);
    (*env)->DeleteLocalRef(env, set);
    return 0;
}

static int
hyperdex_java_client_convert_type(JNIEnv* env,
                                  struct hyperdex_ds_arena* arena,
                                  jobject x,
                                  const char** value,
                                  size_t* value_sz,
                                  enum hyperdatatype* datatype)
{
    enum hyperdex_ds_returncode error;
    int success;
    const char* tmp_str;
    size_t tmp_str_sz;
    jbyte* tmp_bytes;
    size_t tmp_bytes_sz;
    int64_t tmp_l;
    int32_t tmp_i;
    double tmp_d;

    if (x == NULL)
    {
        *value = "";
        *value_sz = 0;
        *datatype = HYPERDATATYPE_GENERIC;
        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _document) == JNI_TRUE)
    {
        x = (*env)->CallObjectMethod(env, x, _document_to_string);
        tmp_str = (*env)->GetStringUTFChars(env, x, 0);
        ERROR_CHECK(-1);
        tmp_str_sz = (*env)->GetStringUTFLength(env, x);
        ERROR_CHECK(-1);
        success = hyperdex_ds_copy_string(arena, tmp_str, tmp_str_sz,
                                          &error, value, value_sz);
        (*env)->ReleaseStringUTFChars(env, x, tmp_str);
        ERROR_CHECK(-1);
        *datatype = HYPERDATATYPE_DOCUMENT;

        if (success < 0)
        {
            hyperdex_java_out_of_memory(env);
            return -1;
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _string) == JNI_TRUE)
    {
        tmp_str = (*env)->GetStringUTFChars(env, x, 0);
        ERROR_CHECK(-1);
        tmp_str_sz = (*env)->GetStringUTFLength(env, x);
        ERROR_CHECK(-1);
        success = hyperdex_ds_copy_string(arena, tmp_str, tmp_str_sz,
                                          &error, value, value_sz);
        (*env)->ReleaseStringUTFChars(env, x, tmp_str);
        ERROR_CHECK(-1);
        *datatype = HYPERDATATYPE_STRING;

        if (success < 0)
        {
            hyperdex_java_out_of_memory(env);
            return -1;
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _byte_string) == JNI_TRUE)
    {
        x = (*env)->CallObjectMethod(env, x, _byte_string_get);
        tmp_bytes = (*env)->GetByteArrayElements(env, x, 0);
        ERROR_CHECK(-1);
        tmp_bytes_sz = (*env)->GetArrayLength(env, x);
        ERROR_CHECK(-1);
        success = hyperdex_ds_copy_string(arena, (const char*)tmp_bytes, tmp_bytes_sz,
                                          &error, value, value_sz);
        (*env)->ReleaseByteArrayElements(env, x, tmp_bytes, 0);
        ERROR_CHECK(-1);
        *datatype = HYPERDATATYPE_STRING;

        if (success < 0)
        {
            hyperdex_java_out_of_memory(env);
            return -1;
        }

        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _long) == JNI_TRUE)
    {
        tmp_l = (*env)->CallLongMethod(env, x, _long_longValue);
        ERROR_CHECK(-1);

        if (hyperdex_ds_copy_int(arena, tmp_l, &error, value, value_sz) < 0)
        {
            hyperdex_java_out_of_memory(env);
            return -1;
        }

        *datatype = HYPERDATATYPE_INT64;
        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _integer) == JNI_TRUE)
    {
        tmp_i = (*env)->CallIntMethod(env, x, _integer_intValue);
        ERROR_CHECK(-1);

        if (hyperdex_ds_copy_int(arena, tmp_i, &error, value, value_sz) < 0)
        {
            hyperdex_java_out_of_memory(env);
            return -1;
        }

        *datatype = HYPERDATATYPE_INT64;
        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _double) == JNI_TRUE)
    {
        tmp_d = (*env)->CallDoubleMethod(env, x, _double_doubleValue);
        ERROR_CHECK(-1);

        if (hyperdex_ds_copy_float(arena, tmp_d, &error, value, value_sz) < 0)
        {
            hyperdex_java_out_of_memory(env);
            return -1;
        }

        *datatype = HYPERDATATYPE_FLOAT;
        return 0;
    }
    else if ((*env)->IsInstanceOf(env, x, _list) == JNI_TRUE)
    {
        return hyperdex_java_client_convert_list(env, arena, x,
                                                 value, value_sz, datatype);
    }
    else if ((*env)->IsInstanceOf(env, x, _set) == JNI_TRUE)
    {
        return hyperdex_java_client_convert_set(env, arena, x,
                                                value, value_sz, datatype);
    }
    else if ((*env)->IsInstanceOf(env, x, _map) == JNI_TRUE)
    {
        return hyperdex_java_client_convert_map(env, arena, x,
                                                value, value_sz, datatype);
    }
    else
    {
        hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_WRONGTYPE,
                                             "Cannot convert unknown type to a HyperDex type");
        return -1;
    }
}

int
hyperdex_java_client_convert_key(JNIEnv* env, jobject client,
                                 struct hyperdex_ds_arena* arena,
                                 jobject x,
                                 const char** key,
                                 size_t* key_sz)
{
    enum hyperdatatype datatype;
    (void)client;
    return hyperdex_java_client_convert_type(env, arena, x, key, key_sz, &datatype);
}

static int
hyperdex_java_client_convert_limit(JNIEnv* env, jobject client,
                                   struct hyperdex_ds_arena* arena,
                                   jint x,
                                   uint64_t* limit)
{
    *limit = x;
    (void)env;
    (void)client;
    (void)arena;
    return 0;
}

static int
hyperdex_java_client_convert_maxmin(JNIEnv* env, jobject client,
                                    struct hyperdex_ds_arena* arena,
                                    jboolean x,
                                    int* maxmin)
{
    *maxmin = x == JNI_TRUE ? 1 : 0;
    (void)env;
    (void)client;
    (void)arena;
    return 0;
}

static int
hyperdex_java_client_convert_mapattributes(JNIEnv* env, jobject client,
                                           struct hyperdex_ds_arena* arena,
                                           jobject x,
                                           const struct hyperdex_client_map_attribute** _mapattrs,
                                           size_t* _mapattrs_sz)
{
    jobject outer_set;
    jobject outer_it;
    jobject outer_entry;
    jobject inner_map;
    jobject inner_set;
    jobject inner_it;
    jobject inner_entry;
    jobject attr;
    const char* attr_cstr;
    jobject key;
    jobject val;
    struct hyperdex_client_map_attribute* mapattrs = NULL;
    size_t mapattrs_sz = 0;
    size_t mapattrs_idx = 0;

    outer_set = (*env)->CallObjectMethod(env, x, _map_entrySet);
    ERROR_CHECK(-1);
    outer_it = (*env)->CallObjectMethod(env, outer_set, _set_iterator);
    ERROR_CHECK(-1);

    while ((*env)->CallBooleanMethod(env, outer_it, _java_iterator_hasNext) == JNI_TRUE)
    {
        outer_entry = (*env)->CallObjectMethod(env, outer_it, _java_iterator_next);
        ERROR_CHECK(-1);
        inner_map = (*env)->CallObjectMethod(env, outer_entry, _map_entry_getValue);
        ERROR_CHECK(-1);
        mapattrs_sz += (*env)->CallIntMethod(env, inner_map, _map_size);
        ERROR_CHECK(-1);
        (*env)->DeleteLocalRef(env, outer_entry);
        ERROR_CHECK(-1);
        (*env)->DeleteLocalRef(env, inner_map);
        ERROR_CHECK(-1);
    }

    mapattrs = hyperdex_ds_allocate_map_attribute(arena, mapattrs_sz);

    if (!mapattrs)
    {
        hyperdex_java_out_of_memory(env);
        return -1;
    }

    *_mapattrs = mapattrs;
    *_mapattrs_sz = mapattrs_sz;
    mapattrs_idx = 0;

    (*env)->DeleteLocalRef(env, outer_it);
    ERROR_CHECK(-1);
    outer_it = (*env)->CallObjectMethod(env, outer_set, _set_iterator);
    ERROR_CHECK(-1);

    while ((*env)->CallBooleanMethod(env, outer_it, _java_iterator_hasNext) == JNI_TRUE)
    {
        outer_entry = (*env)->CallObjectMethod(env, outer_it, _java_iterator_next);
        ERROR_CHECK(-1);
        attr = (*env)->CallObjectMethod(env, outer_entry, _map_entry_getKey);
        ERROR_CHECK(-1);
        inner_map = (*env)->CallObjectMethod(env, outer_entry, _map_entry_getValue);
        ERROR_CHECK(-1);
        inner_set = (*env)->CallObjectMethod(env, inner_map, _map_entrySet);
        ERROR_CHECK(-1);
        inner_it = (*env)->CallObjectMethod(env, inner_set, _set_iterator);
        ERROR_CHECK(-1);
        attr_cstr = hyperdex_java_client_convert_cstring(env, arena, attr);

        if (!attr_cstr)
        {
            return -1;
        }

        while ((*env)->CallBooleanMethod(env, inner_it, _java_iterator_hasNext) == JNI_TRUE)
        {
            assert(mapattrs_idx < mapattrs_sz);
            inner_entry = (*env)->CallObjectMethod(env, inner_it, _java_iterator_next);
            ERROR_CHECK(-1);
            key = (*env)->CallObjectMethod(env, inner_entry, _map_entry_getKey);
            ERROR_CHECK(-1);
            val = (*env)->CallObjectMethod(env, inner_entry, _map_entry_getValue);
            ERROR_CHECK(-1);
            mapattrs[mapattrs_idx].attr = attr_cstr;

            if (hyperdex_java_client_convert_type(env, arena, key,
                                                  &mapattrs[mapattrs_idx].map_key,
                                                  &mapattrs[mapattrs_idx].map_key_sz,
                                                  &mapattrs[mapattrs_idx].map_key_datatype) < 0)
            {
                return -1;
            }

            if (hyperdex_java_client_convert_type(env, arena, val,
                                                  &mapattrs[mapattrs_idx].value,
                                                  &mapattrs[mapattrs_idx].value_sz,
                                                  &mapattrs[mapattrs_idx].value_datatype) < 0)
            {
                return -1;
            }

            ++mapattrs_idx;
            (*env)->DeleteLocalRef(env, val);
            ERROR_CHECK(-1);
            (*env)->DeleteLocalRef(env, key);
            ERROR_CHECK(-1);
            (*env)->DeleteLocalRef(env, inner_entry);
            ERROR_CHECK(-1);
        }

        (*env)->DeleteLocalRef(env, inner_it);
        ERROR_CHECK(-1);
        (*env)->DeleteLocalRef(env, inner_set);
        ERROR_CHECK(-1);
        (*env)->DeleteLocalRef(env, inner_map);
        ERROR_CHECK(-1);
        (*env)->DeleteLocalRef(env, attr);
        ERROR_CHECK(-1);
        (*env)->DeleteLocalRef(env, outer_entry);
        ERROR_CHECK(-1);
    }

    (void)client;
    return 0;
}

static ssize_t
hyperdex_java_client_estimate_predicate_size(JNIEnv* env, jobject x)
{
    jobject it;
    jobject entry;
    size_t sum = 0;

    if ((*env)->IsInstanceOf(env, x, _predicate) == JNI_TRUE)
    {
        sum = (*env)->CallLongMethod(env, x, _predicate_checksSize);
        ERROR_CHECK(-1);
        return sum;
    }
    else if ((*env)->IsInstanceOf(env, x, _list) == JNI_TRUE &&
             (*env)->IsInstanceOf(env,
                 (*env)->CallObjectMethod(env, x, _list_get, 0), _predicate) == JNI_TRUE)
    {
        it = (*env)->CallObjectMethod(env, x, _list_iterator);
        ERROR_CHECK(-1);

        while ((*env)->CallBooleanMethod(env, it, _java_iterator_hasNext) == JNI_TRUE)
        {
            entry = (*env)->CallObjectMethod(env, it, _java_iterator_next);
            ERROR_CHECK(-1);

            if ((*env)->IsInstanceOf(env, entry, _predicate) != JNI_TRUE)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_WRONGTYPE,
                                                     "Cannot convert unknown type to a HyperDex predicate");
                return -1;
            }

            ERROR_CHECK(-1);
            sum += (*env)->CallLongMethod(env, x, _predicate_checksSize);
            ERROR_CHECK(-1);
        }

        return sum;
    }
    else
    {
        ERROR_CHECK(-1);
        return 1;
    }
}

static ssize_t
hyperdex_java_client_convert_predicate(JNIEnv* env,
                                       struct hyperdex_ds_arena* arena,
                                       const char* attr,
                                       jobject x,
                                       struct hyperdex_client_attribute_check* checks,
                                       size_t checks_idx)
{
    jobject it;
    jobject entry;
    jlong tmp;
    ssize_t i;

    if ((*env)->IsInstanceOf(env, x, _predicate) == JNI_TRUE)
    {
        tmp = (*env)->CallLongMethod(env, x, _predicate_convertChecks, arena, checks, checks_idx);
        ERROR_CHECK(-1);

        for (i = checks_idx; i < tmp; ++i)
        {
            checks[i].attr = attr;
        }

        return tmp;
    }
    else if ((*env)->IsInstanceOf(env, x, _list) == JNI_TRUE &&
             (*env)->IsInstanceOf(env,
                 (*env)->CallObjectMethod(env, x, _list_get, 0), _predicate) == JNI_TRUE)
    {
        it = (*env)->CallObjectMethod(env, x, _list_iterator);
        ERROR_CHECK(-1);
        tmp = checks_idx;

        while ((*env)->CallBooleanMethod(env, it, _java_iterator_hasNext))
        {
            entry = (*env)->CallObjectMethod(env, it, _java_iterator_next);
            ERROR_CHECK(-1);
            tmp = (*env)->CallLongMethod(env, entry, _predicate_convertChecks, arena, checks, tmp);
            ERROR_CHECK(-1);
        }

        for (i = checks_idx; i < tmp; ++i)
        {
            checks[i].attr = attr;
        }

        return tmp;
    }
    else
    {
        checks[checks_idx].attr = attr;
        checks[checks_idx].predicate = HYPERPREDICATE_EQUALS;

        if (hyperdex_java_client_convert_type(env, arena, x,
                                              &checks[checks_idx].value,
                                              &checks[checks_idx].value_sz,
                                              &checks[checks_idx].datatype) < 0)
        {
            return -1;
        }

        return checks_idx + 1;
    }
}

int
hyperdex_java_client_convert_predicates(JNIEnv* env, jobject client,
                                        struct hyperdex_ds_arena* arena,
                                        jobject x,
                                        const struct hyperdex_client_attribute_check** _checks,
                                        size_t* _checks_sz)
{
    jobject set;
    jobject it;
    jobject entry;
    jobject key;
    jobject val;
    const char* attr;
    struct hyperdex_client_attribute_check* checks = NULL;
    size_t checks_sz = 0;
    size_t checks_idx = 0;
    ssize_t these_checks = 0;

    set = (*env)->CallObjectMethod(env, x, _map_entrySet);
    ERROR_CHECK(-1);
    it = (*env)->CallObjectMethod(env, set, _set_iterator);
    ERROR_CHECK(-1);

    while ((*env)->CallBooleanMethod(env, it, _java_iterator_hasNext) == JNI_TRUE)
    {
        entry = (*env)->CallObjectMethod(env, it, _java_iterator_next);
        ERROR_CHECK(-1);
        val = (*env)->CallObjectMethod(env, entry, _map_entry_getValue);
        ERROR_CHECK(-1);
        these_checks = hyperdex_java_client_estimate_predicate_size(env, val);

        if (these_checks < 0)
        {
            return -1;
        }

        checks_sz += these_checks;
    }

    (*env)->DeleteLocalRef(env, it);
    ERROR_CHECK(-1);
    it = (*env)->CallObjectMethod(env, set, _set_iterator);
    ERROR_CHECK(-1);

    checks = hyperdex_ds_allocate_attribute_check(arena, checks_sz);

    if (!checks)
    {
        hyperdex_java_out_of_memory(env);
        return -1;
    }

    *_checks = checks;
    *_checks_sz = checks_sz;

    while ((*env)->CallBooleanMethod(env, it, _java_iterator_hasNext) == JNI_TRUE)
    {
        entry = (*env)->CallObjectMethod(env, it, _java_iterator_next);
        ERROR_CHECK(-1);
        key = (*env)->CallObjectMethod(env, entry, _map_entry_getKey);
        ERROR_CHECK(-1);
        val = (*env)->CallObjectMethod(env, entry, _map_entry_getValue);
        ERROR_CHECK(-1);
        attr = hyperdex_java_client_convert_cstring(env, arena, key);

        if (!attr)
        {
            return -1;
        }

        checks_idx = hyperdex_java_client_convert_predicate(env, arena, attr, val, checks, checks_idx);
    }

    (*env)->DeleteLocalRef(env, it);
    ERROR_CHECK(-1);
    (*env)->DeleteLocalRef(env, set);
    ERROR_CHECK(-1);
    (void)client;
    return 0;
}

static int
hyperdex_java_client_convert_attributenames(JNIEnv* env, jobject client,
                                            struct hyperdex_ds_arena* arena,
                                            jobject attrs,
                                            const char*** names, size_t* names_sz)
{
    size_t idx = 0;
    jobject entry;
    jobject it = (*env)->CallObjectMethod(env, attrs, _list_iterator);
    ERROR_CHECK(-1);
    *names_sz = (*env)->CallIntMethod(env, attrs, _list_size);
    ERROR_CHECK(-1);
    *names = hyperdex_ds_malloc(arena, sizeof(char*) * (*names_sz));

    if (!(*names))
    {
        hyperdex_java_out_of_memory(env);
        return -1;
    }

    idx = 0;

    while ((*env)->CallIntMethod(env, it, _java_iterator_hasNext) == JNI_TRUE)
    {
        entry = (*env)->CallObjectMethod(env, it, _java_iterator_next);
        ERROR_CHECK(-1);
        (*names)[idx] = hyperdex_java_client_convert_cstring(env, arena, entry);

        if (!(*names)[idx])
        {
            return -1;
        }

        (*env)->DeleteLocalRef(env, entry);
        ++idx;
    }

    (*env)->DeleteLocalRef(env, it);
    (void) client;
    return 0;
}

static int
hyperdex_java_client_convert_sortby(JNIEnv* env, jobject client,
                                    struct hyperdex_ds_arena* arena,
                                    jstring str,
                                    const char** sortby)
{
    (void)client;
    *sortby = hyperdex_java_client_convert_cstring(env, arena, str);
    return *sortby != NULL ? 0 : -1;
}

static int
hyperdex_java_client_convert_spacename(JNIEnv* env, jobject client,
                                       struct hyperdex_ds_arena* arena,
                                       jstring str,
                                       const char** spacename)
{
    (void)client;
    *spacename = hyperdex_java_client_convert_cstring(env, arena, str);
    return *spacename != NULL ? 0 : -1;
}

/********************************** C -> Java *********************************/

#define BUILD_STRING(OUT, X, X_SZ) \
    do { \
        tmp_array = (*env)->NewByteArray(env, X_SZ); \
        ERROR_CHECK(0); \
        (*env)->SetByteArrayRegion(env, tmp_array, 0, X_SZ, (const jbyte*)X); \
        ERROR_CHECK(0); \
        OUT = (*env)->NewObject(env, _byte_string, _byte_string_init, tmp_array); \
        ERROR_CHECK(0); \
    } while (0)

#define BUILD_INT(OUT, X) \
    do { \
        OUT = (*env)->NewObject(env, _long, _long_init, X); \
        ERROR_CHECK(0); \
    } while (0)

#define BUILD_FLOAT(OUT, X) \
    do { \
        OUT = (*env)->NewObject(env, _double, _double_init, X); \
        ERROR_CHECK(0); \
    } while (0)

static jobject
hyperdex_java_client_build_attribute(JNIEnv* env,
                                     const struct hyperdex_client_attribute* attr)
{
    struct hyperdex_ds_iterator iter;
    jbyteArray tmp_array;
    const char* tmp_str = NULL;
    size_t tmp_str_sz = 0;
    const char* tmp_str2 = NULL;
    size_t tmp_str2_sz = 0;
    int64_t tmp_i = 0;
    int64_t tmp_i2 = 0;
    double tmp_d = 0;
    double tmp_d2 = 0;
    int result = 0;
    jobject ret = NULL;
    jobject tmp = NULL;
    jobject tmp2 = NULL;

    switch (attr->datatype)
    {
        case HYPERDATATYPE_STRING:
            BUILD_STRING(tmp, attr->value, attr->value_sz);
            return tmp;
        case HYPERDATATYPE_INT64:
            if (hyperdex_ds_unpack_int(attr->value, attr->value_sz, &tmp_i) < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed int");
                return 0;
            }

            BUILD_INT(tmp, tmp_i);
            return tmp;
        case HYPERDATATYPE_FLOAT:
            if (hyperdex_ds_unpack_float(attr->value, attr->value_sz, &tmp_d) < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed float");
                return 0;
            }

            BUILD_FLOAT(tmp, tmp_d);
            return tmp;
        case HYPERDATATYPE_DOCUMENT:
            BUILD_STRING(tmp, attr->value, attr->value_sz);
            // Build document from string
            return (*env)->NewObject(env, _document, _document_init, tmp);
        case HYPERDATATYPE_LIST_STRING:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _array_list, _array_list_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_list_string_next(&iter, &tmp_str, &tmp_str_sz)) > 0)
            {
                BUILD_STRING(tmp, tmp_str, tmp_str_sz);
                (*env)->CallBooleanMethod(env, ret, _array_list_add, tmp);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed list(string)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_LIST_INT64:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _array_list, _array_list_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_list_int_next(&iter, &tmp_i)) > 0)
            {
                BUILD_INT(tmp, tmp_i);
                (*env)->CallBooleanMethod(env, ret, _array_list_add, tmp);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed list(int)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_LIST_FLOAT:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _array_list, _array_list_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_list_float_next(&iter, &tmp_d)) > 0)
            {
                BUILD_FLOAT(tmp, tmp_d);
                (*env)->CallBooleanMethod(env, ret, _array_list_add, tmp);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed list(float)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_SET_STRING:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_set, _hash_set_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_set_string_next(&iter, &tmp_str, &tmp_str_sz)) > 0)
            {
                BUILD_STRING(tmp, tmp_str, tmp_str_sz);
                (*env)->CallBooleanMethod(env, ret, _hash_set_add, tmp);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed set(string)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_SET_INT64:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_set, _hash_set_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_set_int_next(&iter, &tmp_i)) > 0)
            {
                BUILD_INT(tmp, tmp_i);
                (*env)->CallObjectMethod(env, ret, _hash_set_add, tmp);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed set(int)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_SET_FLOAT:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_set, _hash_set_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_set_float_next(&iter, &tmp_d)) > 0)
            {
                BUILD_FLOAT(tmp, tmp_d);
                (*env)->CallObjectMethod(env, ret, _hash_set_add, tmp);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed set(float)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_MAP_STRING_STRING:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_map, _hash_map_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_map_string_string_next(&iter, &tmp_str, &tmp_str_sz, &tmp_str2, &tmp_str2_sz)) > 0)
            {
                BUILD_STRING(tmp, tmp_str, tmp_str_sz);
                BUILD_STRING(tmp2, tmp_str2, tmp_str2_sz);
                (*env)->CallObjectMethod(env, ret, _hash_map_put, tmp, tmp2);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(string, string)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_MAP_STRING_INT64:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_map, _hash_map_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_map_string_int_next(&iter, &tmp_str, &tmp_str_sz, &tmp_i)) > 0)
            {
                BUILD_STRING(tmp, tmp_str, tmp_str_sz);
                BUILD_INT(tmp2, tmp_i);
                (*env)->CallObjectMethod(env, ret, _hash_map_put, tmp, tmp2);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(string, int)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_MAP_STRING_FLOAT:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_map, _hash_map_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_map_string_float_next(&iter, &tmp_str, &tmp_str_sz, &tmp_d)) > 0)
            {
                BUILD_STRING(tmp, tmp_str, tmp_str_sz);
                BUILD_FLOAT(tmp2, tmp_d);
                (*env)->CallObjectMethod(env, ret, _hash_map_put, tmp, tmp2);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(string, float)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_MAP_INT64_STRING:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_map, _hash_map_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_map_int_string_next(&iter, &tmp_i, &tmp_str, &tmp_str_sz)) > 0)
            {
                BUILD_INT(tmp, tmp_i);
                BUILD_STRING(tmp2, tmp_str, tmp_str_sz);
                (*env)->CallObjectMethod(env, ret, _hash_map_put, tmp, tmp2);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(int, string)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_MAP_INT64_INT64:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_map, _hash_map_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_map_int_int_next(&iter, &tmp_i, &tmp_i2)) > 0)
            {
                BUILD_INT(tmp, tmp_i);
                BUILD_INT(tmp2, tmp_i2);
                (*env)->CallObjectMethod(env, ret, _hash_map_put, tmp, tmp2);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(int, int)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_MAP_INT64_FLOAT:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_map, _hash_map_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_map_int_float_next(&iter, &tmp_i, &tmp_d)) > 0)
            {
                BUILD_INT(tmp, tmp_i);
                BUILD_FLOAT(tmp2, tmp_d);
                (*env)->CallObjectMethod(env, ret, _hash_map_put, tmp, tmp2);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(int, float)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_MAP_FLOAT_STRING:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_map, _hash_map_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_map_float_string_next(&iter, &tmp_d, &tmp_str, &tmp_str_sz)) > 0)
            {
                BUILD_FLOAT(tmp, tmp_d);
                BUILD_STRING(tmp2, tmp_str, tmp_str_sz);
                (*env)->CallObjectMethod(env, ret, _hash_map_put, tmp, tmp2);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(float, string)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_MAP_FLOAT_INT64:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_map, _hash_map_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_map_float_int_next(&iter, &tmp_d, &tmp_i)) > 0)
            {
                BUILD_FLOAT(tmp, tmp_d);
                BUILD_INT(tmp2, tmp_i);
                (*env)->CallObjectMethod(env, ret, _hash_map_put, tmp, tmp2);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(float, int)");
                return 0;
            }

            return ret;
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
            hyperdex_ds_iterator_init(&iter, attr->datatype, attr->value, attr->value_sz);
            ret = (*env)->NewObject(env, _hash_map, _hash_map_init);
            ERROR_CHECK(0);

            while ((result = hyperdex_ds_iterate_map_float_float_next(&iter, &tmp_d, &tmp_d2)) > 0)
            {
                BUILD_FLOAT(tmp, tmp_d);
                BUILD_FLOAT(tmp2, tmp_d2);
                (*env)->CallObjectMethod(env, ret, _hash_map_put, tmp, tmp2);
                ERROR_CHECK(0);
            }

            if (result < 0)
            {
                hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed map(float, float)");
                return 0;
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
            hyperdex_java_client_throw_exception(env, HYPERDEX_CLIENT_SERVERERROR, "server sent malformed attributes");
            return 0;
    }
}

static jobject
hyperdex_java_client_build_attributes(JNIEnv* env,
                                      const struct hyperdex_client_attribute* attrs,
                                      size_t attrs_sz)
{
    jobject ret;
    jobject str;
    jobject val;
    size_t i = 0;

    ret = (*env)->NewObject(env, _hash_map, _hash_map_init);
    ERROR_CHECK(0);

    for (i = 0; i < attrs_sz; ++i)
    {
        val = hyperdex_java_client_build_attribute(env, attrs + i);

        if (val == 0)
        {
            return 0;
        }

        str = (*env)->NewStringUTF(env, attrs[i].attr);
        ERROR_CHECK(0);
        (*env)->CallObjectMethod(env, ret, _hash_map_put, str, val);
        ERROR_CHECK(0);
    }

    return ret;
}

/******************************* Deferred Class *******************************/
JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Deferred__1create(JNIEnv* env, jobject deferred)
{
    struct hyperdex_java_client_deferred* ptr;

    ERROR_CHECK_VOID();
    ptr = malloc(sizeof(struct hyperdex_java_client_deferred));

    if (!ptr)
    {
        hyperdex_java_out_of_memory(env);
        return;
    }

    memset(ptr, 0, sizeof(struct hyperdex_java_client_deferred));
    (*env)->SetLongField(env, deferred, _deferred_ptr, (long)ptr);
    ERROR_CHECK_VOID();

    ptr->arena = hyperdex_ds_arena_create();

    if (!ptr->arena)
    {
        /* all other resources are caught by the finalizer? */
        hyperdex_java_out_of_memory(env);
        return;
    }

    ptr->reqid = -1;
    ptr->status = HYPERDEX_CLIENT_GARBAGE;
    ptr->attrs = NULL;
    ptr->attrs_sz = 0;
    ptr->description = NULL;
    ptr->count = 0;
    ptr->finished = 0;
    ptr->encode_return = NULL;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Deferred__1destroy(JNIEnv* env, jobject dfrd)
{
    jlong lptr;
    struct hyperdex_java_client_deferred* ptr;

    lptr = (*env)->GetLongField(env, dfrd, _deferred_ptr);
    ERROR_CHECK_VOID();
    ptr = (struct hyperdex_java_client_deferred*)lptr;

    if (ptr)
    {
        if (ptr->arena)
        {
            hyperdex_ds_arena_destroy(ptr->arena);
        }

        if (ptr->attrs)
        {
            hyperdex_client_destroy_attrs(ptr->attrs, ptr->attrs_sz);
        }

        if (ptr->description)
        {
            free((void*)ptr->description);
        }

        free(ptr);
    }

    (*env)->SetLongField(env, dfrd, _deferred_ptr, 0);
    ERROR_CHECK_VOID();
}
#pragma GCC diagnostic pop

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Deferred_waitForIt(JNIEnv* env, jobject obj)
{
    struct hyperdex_java_client_deferred* dfrd = NULL;
    dfrd = hyperdex_get_deferred_ptr(env, obj);
    ERROR_CHECK(0);

    while (!dfrd->finished && dfrd->reqid >= 0)
    {
        (*env)->CallObjectMethod(env, obj, _deferred_loop);
        ERROR_CHECK(0);
    }

    assert(dfrd->encode_return);
    return dfrd->encode_return(env, obj, dfrd);
}

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Deferred_callback(JNIEnv* env, jobject obj)
{
    jobject client_obj;
    struct hyperdex_java_client_deferred* dfrd = NULL;
    dfrd = hyperdex_get_deferred_ptr(env, obj);
    ERROR_CHECK_VOID();
    dfrd->finished = 1;
    client_obj = (*env)->GetObjectField(env, obj, _deferred_c);
    (*env)->CallObjectMethod(env, client_obj, _client_remove_op, dfrd->reqid);
    ERROR_CHECK_VOID();
}

jobject
hyperdex_java_client_deferred_encode_status(JNIEnv* env, jobject obj, struct hyperdex_java_client_deferred* d)
{
    jobject ret;
    jobject client_obj;
    struct hyperdex_client* client;

    if (d->status == HYPERDEX_CLIENT_SUCCESS)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_TRUE);
        ERROR_CHECK(0);
        return ret;
    }
    else if (d->status == HYPERDEX_CLIENT_NOTFOUND)
    {
        return NULL;
    }
    else if (d->status == HYPERDEX_CLIENT_CMPFAIL)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_FALSE);
        ERROR_CHECK(0);
        return ret;
    }
    else
    {
        client_obj = (*env)->GetObjectField(env, obj, _deferred_c);
        ERROR_CHECK(0);
        client = hyperdex_get_client_ptr(env, client_obj);
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }
}

static jobject
hyperdex_java_client_deferred_encode_status_attributes(JNIEnv* env, jobject obj, struct hyperdex_java_client_deferred* d)
{
    jobject ret;
    jobject client_obj;
    struct hyperdex_client* client;

    if (d->status == HYPERDEX_CLIENT_SUCCESS)
    {
        ret = hyperdex_java_client_build_attributes(env, d->attrs, d->attrs_sz);
        ERROR_CHECK(0);
        return ret;
    }
    else if (d->status == HYPERDEX_CLIENT_NOTFOUND)
    {
        return NULL;
    }
    else if (d->status == HYPERDEX_CLIENT_CMPFAIL)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_FALSE);
        ERROR_CHECK(0);
        return ret;
    }
    else
    {
        client_obj = (*env)->GetObjectField(env, obj, _deferred_c);
        client = hyperdex_get_client_ptr(env, client_obj);
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }
}

jobject
hyperdex_java_client_deferred_encode_status_count(JNIEnv* env, jobject obj, struct hyperdex_java_client_deferred* d)
{
    jobject ret;
    jobject client_obj;
    struct hyperdex_client* client;

    if (d->status == HYPERDEX_CLIENT_SUCCESS)
    {
        ret = (*env)->NewObject(env, _long, _long_init, d->count);
        ERROR_CHECK(0);
        return ret;
    }
    else if (d->status == HYPERDEX_CLIENT_NOTFOUND)
    {
        return NULL;
    }
    else if (d->status == HYPERDEX_CLIENT_CMPFAIL)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_FALSE);
        ERROR_CHECK(0);
        return ret;
    }
    else
    {
        client_obj = (*env)->GetObjectField(env, obj, _deferred_c);
        client = hyperdex_get_client_ptr(env, client_obj);
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }
}

static jobject
hyperdex_java_client_deferred_encode_status_description(JNIEnv* env, jobject obj, struct hyperdex_java_client_deferred* d)
{
    jobject ret;
    jobject client_obj;
    struct hyperdex_client* client;

    if (d->status == HYPERDEX_CLIENT_SUCCESS)
    {
        ret = (*env)->NewStringUTF(env, d->description);
        ERROR_CHECK(0);
        return ret;
    }
    else if (d->status == HYPERDEX_CLIENT_NOTFOUND)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_TRUE);
        ERROR_CHECK(0);
        return ret;
    }
    else if (d->status == HYPERDEX_CLIENT_CMPFAIL)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_FALSE);
        ERROR_CHECK(0);
        return ret;
    }
    else
    {
        client_obj = (*env)->GetObjectField(env, obj, _deferred_c);
        client = hyperdex_get_client_ptr(env, client_obj);
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }
}

/******************************* Iterator Class *******************************/

struct hyperdex_java_client_iterator
{
    struct hyperdex_ds_arena* arena;
    int64_t reqid;
    enum hyperdex_client_returncode status;
    const struct hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    int finished;
    jobject (*encode_return)(JNIEnv* env, jobject obj, struct hyperdex_java_client_iterator* d);
};

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Iterator__1create(JNIEnv* env, jobject iterator)
{
    jlong lptr;
    struct hyperdex_java_client_iterator* ptr;

    lptr = (*env)->GetLongField(env, iterator, _iterator_ptr);
    ERROR_CHECK_VOID();
    ptr = malloc(sizeof(struct hyperdex_java_client_iterator));

    if (!ptr)
    {
        hyperdex_java_out_of_memory(env);
        return;
    }

    memset(ptr, 0, sizeof(struct hyperdex_java_client_iterator));
    lptr = (long) ptr;
    (*env)->SetLongField(env, iterator, _iterator_ptr, lptr);
    ERROR_CHECK_VOID();

    ptr->arena = hyperdex_ds_arena_create();

    if (!ptr->arena)
    {
        /* all other resources are caught by the finalizer? */
        hyperdex_java_out_of_memory(env);
        return;
    }

    ptr->reqid = -1;
    ptr->status = HYPERDEX_CLIENT_GARBAGE;
    ptr->attrs = NULL;
    ptr->attrs_sz = 0;
    ptr->finished = 0;
    ptr->encode_return = NULL;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Iterator__1destroy(JNIEnv* env, jobject iter)
{
    jlong lptr;
    struct hyperdex_java_client_iterator* ptr;

    lptr = (*env)->GetLongField(env, iter, _iterator_ptr);
    ERROR_CHECK_VOID();
    ptr = (struct hyperdex_java_client_iterator*)lptr;

    if (ptr)
    {
        if (ptr->arena)
        {
            hyperdex_ds_arena_destroy(ptr->arena);
        }

        if (ptr->attrs)
        {
            hyperdex_client_destroy_attrs(ptr->attrs, ptr->attrs_sz);
        }

        free(ptr);
    }

    (*env)->SetLongField(env, iter, _iterator_ptr, 0);
    ERROR_CHECK_VOID();
}

JNIEXPORT HYPERDEX_API jboolean JNICALL
Java_org_hyperdex_client_Iterator_finished(JNIEnv* env, jobject obj)
{
    struct hyperdex_java_client_iterator* iter = NULL;
    iter = hyperdex_get_iterator_ptr(env, obj);
    return iter->finished == 1 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Iterator_callback(JNIEnv* env, jobject obj)
{
    jobject tmp;
    jobject client_obj;
    struct hyperdex_client* client;
    struct hyperdex_java_client_iterator* iter = NULL;
    iter = hyperdex_get_iterator_ptr(env, obj);
    ERROR_CHECK_VOID();
    client_obj = (*env)->GetObjectField(env, obj, _iterator_c);
    ERROR_CHECK_VOID();

    if (iter->status == HYPERDEX_CLIENT_SEARCHDONE)
    {
        iter->finished = 1;
        (*env)->CallObjectMethod(env, client_obj, _client_remove_op, iter->reqid);
        ERROR_CHECK_VOID();
    }
    else if (iter->status == HYPERDEX_CLIENT_SUCCESS)
    {
        tmp = iter->encode_return(env, obj, iter);

        if (iter->attrs)
        {
            hyperdex_client_destroy_attrs(iter->attrs, iter->attrs_sz);
        }

        iter->attrs = NULL;
        iter->attrs_sz = 0;
        (*env)->CallObjectMethod(env, obj, _iterator_appendBacklogged, tmp);
        ERROR_CHECK_VOID();
    }
    else
    {
        client_obj = (*env)->GetObjectField(env, obj, _iterator_c);
        client = hyperdex_get_client_ptr(env, client_obj);
        tmp = hyperdex_java_client_create_exception(env, iter->status,
                                                    hyperdex_client_error_message(client));
        (*env)->CallObjectMethod(env, obj, _iterator_appendBacklogged, tmp);
        ERROR_CHECK_VOID();
    }
}

static jobject
hyperdex_java_client_iterator_encode_status_attributes(JNIEnv* env, jobject obj, struct hyperdex_java_client_iterator* it)
{
    jobject ret;
    jobject client_obj;
    struct hyperdex_client* client;

    if (it->status == HYPERDEX_CLIENT_SUCCESS)
    {
        ret = hyperdex_java_client_build_attributes(env, it->attrs, it->attrs_sz);
        ERROR_CHECK(0);
        return ret;
    }
    else if (it->status == HYPERDEX_CLIENT_NOTFOUND)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_TRUE);
        ERROR_CHECK(0);
        return ret;
    }
    else if (it->status == HYPERDEX_CLIENT_CMPFAIL)
    {
        ret = (*env)->NewObject(env, _boolean, _boolean_init, JNI_FALSE);
        ERROR_CHECK(0);
        return ret;
    }
    else
    {
        client_obj = (*env)->GetObjectField(env, obj, _deferred_c);
        client = hyperdex_get_client_ptr(env, client_obj);
        hyperdex_java_client_throw_exception(env, it->status, hyperdex_client_error_message(client));
        return 0;
    }
}


const char*
hyperdex_java_client_convert_cstring(JNIEnv* env,
                                     struct hyperdex_ds_arena* arena,
                                     jobject str)
{
    const char* tmp = NULL;
    const char* ret = NULL;
    size_t ret_sz = 0;
    enum hyperdex_ds_returncode rc;
    int success;
    tmp = (*env)->GetStringUTFChars(env, str, 0);
    ERROR_CHECK(NULL);
    success = hyperdex_ds_copy_string(arena, tmp, strlen(tmp) + 1, &rc, &ret, &ret_sz);
    (*env)->ReleaseStringUTFChars(env, str, tmp);
    ERROR_CHECK(NULL);

    if (success < 0)
    {
        hyperdex_java_out_of_memory(env);
        return NULL;
    }

    return ret;
}

int
hyperdex_java_client_convert_attributes(JNIEnv* env, jobject client,
                                        struct hyperdex_ds_arena* arena,
                                        jobject x,
                                        const struct hyperdex_client_attribute** _attrs,
                                        size_t* _attrs_sz)
{
    jobject set;
    jobject it;
    jobject entry;
    jobject key;
    jobject val;
    struct hyperdex_client_attribute* attrs = NULL;
    size_t attrs_sz = 0;
    size_t attrs_idx = 0;

    set = (*env)->CallObjectMethod(env, x, _map_entrySet);
    ERROR_CHECK(-1);
    it = (*env)->CallObjectMethod(env, set, _set_iterator);
    ERROR_CHECK(-1);
    attrs_sz = (*env)->CallIntMethod(env, x, _map_size);
    ERROR_CHECK(-1);
    attrs = hyperdex_ds_allocate_attribute(arena, attrs_sz);

    if (!attrs)
    {
        hyperdex_java_out_of_memory(env);
        return -1;
    }

    *_attrs = attrs;
    *_attrs_sz = attrs_sz;
    attrs_idx = 0;

    while ((*env)->CallBooleanMethod(env, it, _java_iterator_hasNext) == JNI_TRUE)
    {
        entry = (*env)->CallObjectMethod(env, it, _java_iterator_next);
        ERROR_CHECK(-1);
        key = (*env)->CallObjectMethod(env, entry, _map_entry_getKey);
        ERROR_CHECK(-1);
        val = (*env)->CallObjectMethod(env, entry, _map_entry_getValue);
        ERROR_CHECK(-1);
        attrs[attrs_idx].attr = hyperdex_java_client_convert_cstring(env, arena, key);

        if (!attrs[attrs_idx].attr)
        {
            return -1;
        }

        if (hyperdex_java_client_convert_type(env, arena, val,
                                              &attrs[attrs_idx].value,
                                              &attrs[attrs_idx].value_sz,
                                              &attrs[attrs_idx].datatype) < 0)
        {
            return -1;
        }

        (*env)->DeleteLocalRef(env, val);
        (*env)->DeleteLocalRef(env, key);
        (*env)->DeleteLocalRef(env, entry);
        ++attrs_idx;
    }

    (*env)->DeleteLocalRef(env, it);
    (*env)->DeleteLocalRef(env, set);
    (void)client;
    return 0;
}

/********************************* Predicates *********************************/

#define SINGLE_OBJECT_PREDICATE(CamelCase, lower_case, PREDICATE) \
    JNIEXPORT HYPERDEX_API jlong JNICALL \
    Java_org_hyperdex_client_ ## CamelCase ## _checksSize(JNIEnv* env, jobject obj) \
    { \
        (void) env; \
        (void) obj; \
        return 1; \
    } \
    JNIEXPORT HYPERDEX_API jlong JNICALL \
    Java_org_hyperdex_client_ ## CamelCase ## _convertChecks(JNIEnv* env, jobject obj, jlong arena, \
                                                             jlong _checks, jlong checks_sz) \
    { \
        struct hyperdex_client_attribute_check* checks = (struct hyperdex_client_attribute_check*) _checks; \
        jobject x = (*env)->GetObjectField(env, obj, _pred_ ## lower_case ## _x); \
        ERROR_CHECK(-1); \
        checks[checks_sz].predicate = HYPERPREDICATE_ ## PREDICATE; \
        if (hyperdex_java_client_convert_type(env, (struct hyperdex_ds_arena*)arena, x, \
                                              &checks[checks_sz].value, \
                                              &checks[checks_sz].value_sz, \
                                              &checks[checks_sz].datatype) < 0) \
        { \
            return -1; \
        } \
        ERROR_CHECK(-1); \
        return checks_sz + 1; \
    }

SINGLE_OBJECT_PREDICATE(LessEqual, less_equal, LESS_EQUAL)
SINGLE_OBJECT_PREDICATE(GreaterEqual, greater_equal, GREATER_EQUAL)
SINGLE_OBJECT_PREDICATE(LessThan, less_equal, LESS_THAN)
SINGLE_OBJECT_PREDICATE(GreaterThan, greater_equal, GREATER_THAN)

JNIEXPORT HYPERDEX_API jlong JNICALL
Java_org_hyperdex_client_Range_checksSize(JNIEnv* env, jobject obj)
{
    (void) env;
    (void) obj;
    return 2;
}

JNIEXPORT HYPERDEX_API jlong JNICALL
Java_org_hyperdex_client_Range_convertChecks(JNIEnv* env, jobject obj, jlong arena,
                                             jlong _checks, jlong checks_sz)
{
    struct hyperdex_client_attribute_check* checks = (struct hyperdex_client_attribute_check*) _checks;
    jobject x;
    jobject y;
    x = (*env)->GetObjectField(env, obj, _pred_range_x);
    y = (*env)->GetObjectField(env, obj, _pred_range_y);
    ERROR_CHECK(-1);
    checks[checks_sz + 0].predicate = HYPERPREDICATE_GREATER_EQUAL;
    checks[checks_sz + 1].predicate = HYPERPREDICATE_LESS_EQUAL;

    if (hyperdex_java_client_convert_type(env, (struct hyperdex_ds_arena*)arena, x,
                                          &checks[checks_sz].value,
                                          &checks[checks_sz].value_sz,
                                          &checks[checks_sz].datatype) < 0)
    {
        return -1;
    }

    if (hyperdex_java_client_convert_type(env, (struct hyperdex_ds_arena*)arena, y,
                                          &checks[checks_sz + 1].value,
                                          &checks[checks_sz + 1].value_sz,
                                          &checks[checks_sz + 1].datatype) < 0)
    {
        return -1;
    }

    ERROR_CHECK(-1);
    return checks_sz + 2;
}

SINGLE_OBJECT_PREDICATE(Regex, regex, REGEX)
SINGLE_OBJECT_PREDICATE(LengthEquals, length_equals, LENGTH_EQUALS)
SINGLE_OBJECT_PREDICATE(LengthLessEqual, length_less_equal, LENGTH_LESS_EQUAL)
SINGLE_OBJECT_PREDICATE(LengthGreaterEqual, length_greater_equal, LENGTH_GREATER_EQUAL)

/******************************** Client Class ********************************/

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Client__1create(JNIEnv* env, jobject client, jstring _host, jint port)
{
    jlong lptr;
    const char* host;
    struct hyperdex_client* ptr;

    lptr = (*env)->GetLongField(env, client, _client_ptr);
    ERROR_CHECK_VOID();
    host = (*env)->GetStringUTFChars(env, _host, NULL);
    ERROR_CHECK_VOID();
    ptr = hyperdex_client_create(host, port);
    (*env)->ReleaseStringUTFChars(env, _host, host);

    if (!ptr)
    {
        hyperdex_java_out_of_memory(env);
        return;
    }

    ERROR_CHECK_VOID();
    lptr = (long) ptr;
    (*env)->SetLongField(env, client, _client_ptr, lptr);
    ERROR_CHECK_VOID();
    assert(sizeof(long) >= sizeof(struct hyperdex_client*));
}

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Client__1destroy(JNIEnv* env, jobject client)
{
    jlong lptr;
    struct hyperdex_client* ptr;

    lptr = (*env)->GetLongField(env, client, _client_ptr);
    ERROR_CHECK_VOID();
    ptr = (struct hyperdex_client*)lptr;

    if (ptr)
    {
        hyperdex_client_destroy(ptr);
    }

    (*env)->SetLongField(env, client, _client_ptr, 0);
    ERROR_CHECK_VOID();
}

JNIEXPORT HYPERDEX_API jlong JNICALL
Java_org_hyperdex_client_Client_inner_1loop(JNIEnv* env, jobject client)
{
    struct hyperdex_client* ptr;
    int64_t x;
    jlong y;
    enum hyperdex_client_returncode rc;

    ptr = hyperdex_get_client_ptr(env, client);
    x = hyperdex_client_loop(ptr, -1, &rc);

    if (x < 0)
    {
        hyperdex_java_client_throw_exception(env, rc, hyperdex_client_error_message(ptr));
        return -1;
    }

    y = (jlong)x;
    assert(x == y);
    return y;
}

#include "bindings/java/org_hyperdex_client_Client.definitions.c"
