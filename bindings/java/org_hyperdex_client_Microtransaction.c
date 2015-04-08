/* Copyright (c) 2015, Cornell University
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
#include "bindings/java/org_hyperdex_client_Microtransaction.h"
#include "bindings/java/org_hyperdex_client_Deferred.h"

#include "client-util.h"

#define CHECK_CACHE(X) assert((X))
#define ERROR_CHECK(RET) if ((*env)->ExceptionCheck(env) == JNI_TRUE) return (RET)
#define ERROR_CHECK_VOID() if ((*env)->ExceptionCheck(env) == JNI_TRUE) return

#define REF(NAME, DEF) \
    tmp_cls = (DEF); \
    NAME = (jclass) (*env)->NewGlobalRef(env, tmp_cls); \
    (*env)->DeleteLocalRef(env, tmp_cls);

static jclass _uxact;
static jfieldID _uxact_client;
static jfieldID _uxact_deferred_ptr;
static jfieldID _uxact_uxact_ptr;

static jclass _deferred;
static jmethodID _deferred_init;
static jfieldID _deferred_ptr;
static jfieldID _deferred_c;

static jclass _client;
static jfieldID _client_ptr;
static jmethodID _client_add_op;
static jmethodID _client_remove_op;

static jclass _boolean;
static jmethodID _boolean_init;

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Microtransaction_initialize(JNIEnv* env, jclass client)
{
    jclass tmp_cls;

    /* cache class Client */
    REF(_uxact, (*env)->FindClass(env, "org/hyperdex/client/Microtransaction"));
    _uxact_client = (*env)->GetFieldID(env, _uxact, "client", "Lorg/hyperdex/client/Client;");
    _uxact_uxact_ptr = (*env)->GetFieldID(env, _uxact, "uxact_ptr", "J");
    _uxact_deferred_ptr = (*env)->GetFieldID(env, _uxact, "deferred_ptr", "J");
    REF(_client, (*env)->FindClass(env, "org/hyperdex/client/Client"));
    _client_ptr = (*env)->GetFieldID(env, _client, "ptr", "J");
    _client_add_op = (*env)->GetMethodID(env, _client, "add_op", "(JLorg/hyperdex/client/Operation;)V");
    _client_remove_op = (*env)->GetMethodID(env, _client, "remove_op", "(J)V");
    REF(_deferred, (*env)->FindClass(env, "org/hyperdex/client/Deferred"));
    _deferred_ptr = (*env)->GetFieldID(env, _deferred, "ptr", "J");
    _deferred_init = (*env)->GetMethodID(env, _deferred, "<init>", "(Lorg/hyperdex/client/Client;)V");
    _deferred_c = (*env)->GetFieldID(env, _deferred, "c", "Lorg/hyperdex/client/Client;");
    REF(_boolean, (*env)->FindClass(env, "java/lang/Boolean"));
    _boolean_init = (*env)->GetMethodID(env, _boolean, "<init>", "(Z)V");

    CHECK_CACHE(_uxact);
    CHECK_CACHE(_uxact_client);
    CHECK_CACHE(_uxact_uxact_ptr);
    CHECK_CACHE(_uxact_deferred_ptr);
    CHECK_CACHE(_client);
    CHECK_CACHE(_client_ptr);
    CHECK_CACHE(_client_add_op);
    CHECK_CACHE(_client_remove_op);
    CHECK_CACHE(_boolean);
    CHECK_CACHE(_boolean_init);
    CHECK_CACHE(_deferred);
    CHECK_CACHE(_deferred_init);
    CHECK_CACHE(_deferred_c);
    CHECK_CACHE(_deferred_ptr);

    ERROR_CHECK_VOID();
}

static jobject
hyperdex_uxact_get_client(JNIEnv* env, jobject obj)
{
    jobject c;
    c = (*env)->GetObjectField(env, obj, _uxact_client);
    assert(c);
    return c;
}

static struct hyperdex_client*
hyperdex_uxact_get_client_ptr(JNIEnv* env, jobject obj)
{
    jobject c;
    struct hyperdex_client* x;
    c = hyperdex_uxact_get_client(env, obj);
    x = (struct hyperdex_client*) (*env)->GetLongField(env, c, _client_ptr);
    assert(x);
    return x;
}

static struct hyperdex_client*
hyperdex_client_get_client_ptr(JNIEnv* env, jobject obj)
{
    struct hyperdex_client* x;
    x = (struct hyperdex_client*) (*env)->GetLongField(env, obj, _client_ptr);
    assert(x);
    return x;
}

static struct hyperdex_java_client_deferred*
hyperdex_uxact_get_deferred_ptr(JNIEnv* env, jobject obj)
{
    struct hyperdex_java_client_deferred* x;
    x = (struct hyperdex_java_client_deferred*) (*env)->GetLongField(env, obj, _uxact_deferred_ptr);
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

static struct hyperdex_ds_arena*
hyperdex_uxact_get_arena_ptr(JNIEnv* env, jobject obj)
{
    struct hyperdex_java_client_deferred* x;
    x = (struct hyperdex_java_client_deferred*) (*env)->GetLongField(env, obj, _uxact_deferred_ptr);
    assert(x);
    return x->arena;
}

static struct hyperdex_client_microtransaction*
hyperdex_uxact_get_uxact_ptr(JNIEnv* env, jobject obj)
{
    struct hyperdex_client_microtransaction* x;
    x = (struct hyperdex_client_microtransaction*) (*env)->GetLongField(env, obj, _uxact_uxact_ptr);
    assert(x);
    return x;
}

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Microtransaction__1create(JNIEnv* env, jobject uxact, jstring _space)
{
    struct hyperdex_client *client;
    struct hyperdex_client_microtransaction *uxact_ptr;
    const char *space;
    struct hyperdex_java_client_deferred *deferred;

    deferred = (struct hyperdex_java_client_deferred*)malloc(sizeof(struct hyperdex_java_client_deferred));
    memset(deferred, 0, sizeof(struct hyperdex_java_client_deferred));

    deferred->arena = hyperdex_ds_arena_create();

    if (!deferred->arena)
    {
        /* all other resources are caught by the finalizer? */
        hyperdex_java_out_of_memory(env);
        return;
    }

    deferred->status = HYPERDEX_CLIENT_GARBAGE;

    space = (*env)->GetStringUTFChars(env, _space, NULL);

    client = hyperdex_uxact_get_client_ptr(env, uxact);
    uxact_ptr = hyperdex_client_uxact_init(client, space, &deferred->status);

    (*env)->SetLongField(env, uxact, _uxact_uxact_ptr, (long)uxact_ptr);
    (*env)->SetLongField(env, uxact, _uxact_deferred_ptr, (long)deferred);
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Microtransaction_async_1cond_1commit(JNIEnv* env, jobject uxact, jstring jkey, jobject checks)
{
    const char* key;
    size_t key_sz;
    const struct hyperdex_client_attribute_check *chks;
    size_t chks_sz;
    struct hyperdex_client* client_ptr = hyperdex_uxact_get_client_ptr(env, uxact);
    struct hyperdex_client_microtransaction *uxact_ptr = hyperdex_uxact_get_uxact_ptr(env, uxact);
    jobject client = hyperdex_uxact_get_client(env, uxact);
    jobject op = (*env)->NewObject(env, _deferred, _deferred_init, client);
    struct hyperdex_java_client_deferred* o = hyperdex_uxact_get_deferred_ptr(env, uxact);
    ERROR_CHECK(0);

    hyperdex_java_client_convert_key(env, client, o->arena, jkey, &key, &key_sz);
    hyperdex_java_client_convert_predicates(env, client, o->arena, checks, &chks, &chks_sz);
    o->encode_return = hyperdex_java_client_deferred_encode_status;
    o->reqid = hyperdex_client_uxact_cond_commit(client_ptr, uxact_ptr, key, key_sz, chks, chks_sz);

    (*env)->SetLongField(env, op, _deferred_ptr, (long)o);

    if (o->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, o->status, hyperdex_client_error_message(client_ptr));
        return NULL;
    }
    (*env)->CallObjectMethod(env, client, _client_add_op, o->reqid, op);
    ERROR_CHECK(0);
    return op;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Microtransaction_async_1group_1commit(JNIEnv* env, jobject uxact, jobject checks)
{
    const struct hyperdex_client_attribute_check *chks;
    size_t chks_sz;
    struct hyperdex_client* client_ptr = hyperdex_uxact_get_client_ptr(env, uxact);
    struct hyperdex_client_microtransaction *uxact_ptr = hyperdex_uxact_get_uxact_ptr(env, uxact);
    jobject client = hyperdex_uxact_get_client(env, uxact);
    jobject op = (*env)->NewObject(env, _deferred, _deferred_init, client);
    struct hyperdex_java_client_deferred* o = hyperdex_uxact_get_deferred_ptr(env, uxact);
    ERROR_CHECK(0);

    hyperdex_java_client_convert_predicates(env, client, o->arena, checks, &chks, &chks_sz);
    o->encode_return = hyperdex_java_client_deferred_encode_status_count;
    o->reqid = hyperdex_client_uxact_group_commit(client_ptr, uxact_ptr, chks, chks_sz, &o->count);

    (*env)->SetLongField(env, op, _deferred_ptr, (long)o);

    if (o->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, o->status, hyperdex_client_error_message(client_ptr));
        return NULL;
    }
    (*env)->CallObjectMethod(env, client, _client_add_op, o->reqid, op);
    ERROR_CHECK(0);
    return op;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Microtransaction_async_1commit(JNIEnv* env, jobject uxact, jstring key)
{
    const char* in_key;
    size_t in_key_sz;
    struct hyperdex_client* client_ptr = hyperdex_uxact_get_client_ptr(env, uxact);
    struct hyperdex_client_microtransaction *uxact_ptr = hyperdex_uxact_get_uxact_ptr(env, uxact);
    jobject client = hyperdex_uxact_get_client(env, uxact);
    jobject op = (*env)->NewObject(env, _deferred, _deferred_init, client);
    struct hyperdex_java_client_deferred* o = hyperdex_uxact_get_deferred_ptr(env, uxact);
    ERROR_CHECK(0);

    hyperdex_java_client_convert_key(env, client, o->arena, key, &in_key, &in_key_sz);
    o->encode_return = hyperdex_java_client_deferred_encode_status;
    o->reqid = hyperdex_client_uxact_commit(client_ptr, uxact_ptr, in_key, in_key_sz);

    (*env)->SetLongField(env, op, _deferred_ptr, (long)o);

    if (o->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, o->status, hyperdex_client_error_message(client_ptr));
        return NULL;
    }
    (*env)->CallObjectMethod(env, client, _client_add_op, o->reqid, op);
    ERROR_CHECK(0);
    return op;
}

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Microtransaction__1destroy(JNIEnv* env, jclass uxact)
{
    ERROR_CHECK_VOID();
}

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Microtransaction_terminate(JNIEnv* env, jclass uxact)
{
    (*env)->DeleteGlobalRef(env, _uxact);
    (*env)->DeleteGlobalRef(env, _client);
    (void) uxact;
}

#include "bindings/java/org_hyperdex_client_Microtransaction.definitions.c"
