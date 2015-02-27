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

#ifndef JAVA_UTIL_H
#define JAVA_UTIL_H

#include <jni.h>

/* HyperDex */
#include <hyperdex/client.h>
#include <hyperdex/datastructures.h>

const char*
hyperdex_java_client_convert_cstring(JNIEnv* env,
                                     struct hyperdex_ds_arena* arena,
                                     jobject str);

int
hyperdex_java_client_convert_attributes(JNIEnv* env, jobject client,
                                        struct hyperdex_ds_arena* arena,
                                        jobject x,
                                        const struct hyperdex_client_attribute** _attrs,
                                        size_t* _attrs_sz);

int
hyperdex_java_client_convert_key(JNIEnv* env, jobject client,
                                 struct hyperdex_ds_arena* arena,
                                 jobject x,
                                 const char** key,
                                 size_t* key_sz);

int
hyperdex_java_client_convert_predicates(JNIEnv* env, jobject client,
                                        struct hyperdex_ds_arena* arena,
                                        jobject x,
                                        const struct hyperdex_client_attribute_check** _checks,
                                        size_t* _checks_sz);

int
hyperdex_java_client_throw_exception(JNIEnv* env,
                                     enum hyperdex_client_returncode _rc,
                                     const char* message);

struct hyperdex_java_client_deferred
{
    struct hyperdex_ds_arena* arena;
    int64_t reqid;
    enum hyperdex_client_returncode status;
    const struct hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    const char* description;
    uint64_t count;
    int finished;
    jobject (*encode_return)(JNIEnv* env, jobject obj, struct hyperdex_java_client_deferred* d);
};

jobject
hyperdex_java_client_deferred_encode_status_count(JNIEnv* env, jobject obj, struct hyperdex_java_client_deferred* d);

jobject
hyperdex_java_client_deferred_encode_status(JNIEnv* env, jobject obj, struct hyperdex_java_client_deferred* d);

#endif
