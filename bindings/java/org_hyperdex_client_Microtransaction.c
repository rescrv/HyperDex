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

JNIEXPORT HYPERDEX_API void JNICALL
Java_org_hyperdex_client_Microtransaction__1create(JNIEnv* env, jobject client, jstring _host, jint port)
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
Java_org_hyperdex_client_Microtransactoin__1destroy(JNIEnv* env, jobject client)
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

#include "bindings/java/org_hyperdex_client_Microtransaction.definitions.c"
