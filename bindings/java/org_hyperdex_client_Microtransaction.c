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
static jfieldID _uxact_arena_ptr;
static jfieldID _uxact_uxact_ptr;
static jfieldID _uxact_status_ptr;

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
    _uxact_arena_ptr = (*env)->GetFieldID(env, _uxact, "arena_ptr", "J");
    _uxact_status_ptr = (*env)->GetFieldID(env, _uxact, "status_ptr", "J");
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
    CHECK_CACHE(_uxact_arena_ptr);
    CHECK_CACHE(_uxact_status_ptr);
    CHECK_CACHE(_client);
    CHECK_CACHE(_client_ptr);
    CHECK_CACHE(_client_add_op);
    CHECK_CACHE(_client_remove_op);
    CHECK_CACHE(_boolean);
    CHECK_CACHE(_boolean_init);

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

static enum hyperdex_client_returncode*
hyperdex_uxact_get_status_ptr(JNIEnv* env, jobject obj)
{
    enum hyperdex_client_returncode* x;
    x = (enum hyperdex_client_returncode*) (*env)->GetLongField(env, obj, _uxact_status_ptr);
    assert(x);
    return x;
}

static struct hyperdex_java_uxact_deferred*
hyperdex_get_deferred_ptr(JNIEnv* env, jobject obj)
{
    struct hyperdex_java_uxact_deferred* x;
    x = (struct hyperdex_java_uxact_deferred*) (*env)->GetLongField(env, obj, _deferred_ptr);
    assert(x);
    return x;
}

struct hyperdex_java_uxact_deferred
{
    struct hyperdex_ds_arena* arena;
    int64_t reqid;
    enum hyperdex_client_returncode status;
    int finished;
    jobject (*encode_return)(JNIEnv* env, jobject obj, struct hyperdex_java_uxact_deferred* d);
};

static struct hyperdex_ds_arena*
hyperdex_uxact_get_arena_ptr(JNIEnv* env, jobject obj)
{
    struct hyperdex_ds_arena* x;
    x = (struct hyperdex_ds_arena*) (*env)->GetLongField(env, obj, _uxact_arena_ptr);
    assert(x);
    return x;
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
    enum hyperdex_client_returncode *status;
    const char *space;
    struct hyperdex_ds_arena *arena;

    arena = hyperdex_ds_arena_create();

    status = (enum hyperdex_client_returncode*)malloc(sizeof(enum hyperdex_client_returncode));
    *status = HYPERDEX_CLIENT_SUCCESS;

    space = (*env)->GetStringUTFChars(env, _space, NULL);

    client = hyperdex_uxact_get_client_ptr(env, uxact);
    uxact_ptr = hyperdex_client_uxact_init(client, space, status);

    (*env)->SetLongField(env, uxact, _uxact_uxact_ptr, (long)uxact_ptr);
    (*env)->SetLongField(env, uxact, _uxact_arena_ptr, (long)arena);
    (*env)->SetLongField(env, uxact, _uxact_status_ptr, (long)status);
}

static jobject
hyperdex_java_uxact_deferred_encode_status(JNIEnv* env, jobject obj, struct hyperdex_java_uxact_deferred* d)
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
        client = hyperdex_uxact_get_client_ptr(env, client_obj);
        hyperdex_java_client_throw_exception(env, d->status, hyperdex_client_error_message(client));
        return 0;
    }
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Microtransaction_asynccall__spacename_key_attributenames__status_attributes(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, struct hyperdex_client_microtransaction *uaxct, const char* key, size_t key_sz), jobject key);

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Microtransaction_asynccall__spacename_key_attributenames__status_attributes(JNIEnv* env, jobject obj, int64_t (*f)(struct hyperdex_client* client, struct hyperdex_client_microtransaction *uaxct, const char* key, size_t key_sz), jobject key)
{
    const char* in_key;
    size_t in_key_sz;
    struct hyperdex_client* client = hyperdex_uxact_get_client_ptr(env, obj);
    struct hyperdex_client_microtransaction *uxact = hyperdex_uxact_get_uxact_ptr(env, obj);
    jobject op = (*env)->NewObject(env, _deferred, _deferred_init, obj);
    struct hyperdex_java_uxact_deferred* o = NULL;
    ERROR_CHECK(0);
    o = hyperdex_get_deferred_ptr(env, op);
    ERROR_CHECK(0);
    o->reqid = f(client, uxact, in_key, in_key_sz);

    if (o->reqid < 0)
    {
        hyperdex_java_client_throw_exception(env, o->status, hyperdex_client_error_message(client));
        return 0;
    }

    o->encode_return = hyperdex_java_uxact_deferred_encode_status;
    (*env)->CallObjectMethod(env, obj, _client_add_op, o->reqid, op);
    ERROR_CHECK(0);
    return op;
}

JNIEXPORT HYPERDEX_API jobject JNICALL
Java_org_hyperdex_client_Microtransaction_async_1commit(JNIEnv* env, jobject uxact, jstring key)
{
    jobject client;
    struct hyperdex_client *client_ptr;
    struct hyperdex_ds_arena *arena;

    client = hyperdex_uxact_get_client(env, uxact);
    client_ptr = hyperdex_uxact_get_client_ptr(env, uxact);
    arena = hyperdex_uxact_get_arena_ptr(env, uxact);

    return Java_org_hyperdex_client_Microtransaction_asynccall__spacename_key_attributenames__status_attributes(env, uxact, hyperdex_client_uxact_commit, key);
}

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Microtransaction__1destroy(JNIEnv* env, jclass uxact)
{
    enum hyperdex_client_returncode *status;
    struct hyperdex_ds_arena *arena;

    status = hyperdex_uxact_get_status_ptr(env, uxact);
    arena = hyperdex_uxact_get_arena_ptr(env, uxact);

    free(status);
    hyperdex_ds_arena_destroy(arena);
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
