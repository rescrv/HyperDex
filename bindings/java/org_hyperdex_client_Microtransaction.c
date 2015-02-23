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

#include "client-util.h"

#define REF(NAME, DEF) \
    tmp_cls = (DEF); \
    NAME = (jclass) (*env)->NewGlobalRef(env, tmp_cls); \
    (*env)->DeleteLocalRef(env, tmp_cls);

static jclass _uxact;
static jfieldID _uxact_client;
static jfieldID _uxact_arena_ptr;
static jfieldID _uxact_uxact_ptr;

static jclass _client;
static jfieldID _client_ptr;

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Microtransaction_initialize(JNIEnv* env, jclass client)
{
    jclass tmp_cls;

    /* cache class Client */
    REF(_uxact, (*env)->FindClass(env, "org/hyperdex/client/Microtransaction"));
    _uxact_client = (*env)->GetFieldID(env, _uxact, "client_ptr", "Lorg/hyperdex/client/Microtransaction;");
    _uxact_uxact_ptr = (*env)->GetFieldID(env, _uxact, "uxact_ptr", "J");
    _uxact_arena_ptr = (*env)->GetFieldID(env, _uxact, "uxact_arena_ptr", "J");
    REF(_client, (*env)->FindClass(env, "org/hyperdex/client/Client"));
    _client_ptr = (*env)->GetFieldID(env, _client, "client_ptr", "J");

    CHECK_CACHE(_uxact);
    CHECK_CACHE(_uxact_client);
    CHECK_CACHE(_uxact_uxact_ptr);
    CHECK_CACHE(_uxact_arena_ptr);
    CHECK_CACHE(_client);
    CHECK_CACHE(_client_ptr);

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
}

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Microtransaction__1destroy(JNIEnv* env, jclass uxact)
{
    (void) uxact;
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
