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

#ifndef hyperdex_admin_h_
#define hyperdex_admin_h_

/* C */
#include <stdint.h>
#include <stdlib.h>

/* HyperDex */
#include <hyperdex.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

struct hyperdex_admin;

struct hyperdex_admin_perf_counter
{
    uint64_t id;
    uint64_t time;
    const char* property;
    uint64_t measurement;
};

/* hyperdex_admin_returncode occupies [8704, 8832) */
enum hyperdex_admin_returncode
{
    HYPERDEX_ADMIN_SUCCESS      = 8704,

    /* Error conditions */
    HYPERDEX_ADMIN_NOMEM        = 8768,
    HYPERDEX_ADMIN_NONEPENDING  = 8769,
    HYPERDEX_ADMIN_POLLFAILED   = 8770,
    HYPERDEX_ADMIN_TIMEOUT      = 8771,
    HYPERDEX_ADMIN_INTERRUPTED  = 8772,
    HYPERDEX_ADMIN_COORDFAIL    = 8774,
    HYPERDEX_ADMIN_SERVERERROR  = 8773,

    /* This should never happen.  It indicates a bug */
    HYPERDEX_ADMIN_INTERNAL     = 8829,
    HYPERDEX_ADMIN_EXCEPTION    = 8830,
    HYPERDEX_ADMIN_GARBAGE      = 8831
};

struct hyperdex_admin*
hyperdex_admin_create(const char* coordinator, uint16_t port);

void
hyperdex_admin_destroy(struct hyperdex_admin* admin);

int64_t
hyperdex_admin_dump_config(struct hyperdex_admin* admin,
                           enum hyperdex_admin_returncode* status,
                           const char** config);

int
hyperdex_admin_validate_space(struct hyperdex_admin* admin,
                              const char* description,
                              enum hyperdex_admin_returncode* status,
                              const char** error_msg);

int64_t
hyperdex_admin_add_space(struct hyperdex_admin* admin,
                         const char* description,
                         enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_rm_space(struct hyperdex_admin* admin,
                        const char* name,
                        enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_enable_perf_counters(struct hyperdex_admin* admin,
                                    enum hyperdex_admin_returncode* status,
                                    struct hyperdex_admin_perf_counter* pc);

void
hyperdex_admin_disable_perf_counters(struct hyperdex_admin* admin);

int64_t
hyperdex_admin_loop(struct hyperdex_admin* admin, int timeout,
                    enum hyperdex_admin_returncode* status);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* hyperdex_admin_h_ */
