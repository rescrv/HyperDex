/* Copyright (c) 2013-2014, Cornell University
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
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef hyperdex_admin_h_
#define hyperdex_admin_h_

/* C */
#include <stdint.h>
#include <stdio.h>
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
    HYPERDEX_ADMIN_SUCCESS     = 8704,

    /* Error conditions */
    HYPERDEX_ADMIN_NOMEM       = 8768,
    HYPERDEX_ADMIN_NONEPENDING = 8769,
    HYPERDEX_ADMIN_POLLFAILED  = 8770,
    HYPERDEX_ADMIN_TIMEOUT     = 8771,
    HYPERDEX_ADMIN_INTERRUPTED = 8772,
    HYPERDEX_ADMIN_SERVERERROR = 8773,
    HYPERDEX_ADMIN_COORDFAIL   = 8774,
    HYPERDEX_ADMIN_BADSPACE    = 8775,
    HYPERDEX_ADMIN_DUPLICATE   = 8776,
    HYPERDEX_ADMIN_NOTFOUND    = 8777,
    HYPERDEX_ADMIN_LOCALERROR  = 8778,

    /* This should never happen.  It indicates a bug */
    HYPERDEX_ADMIN_INTERNAL    = 8829,
    HYPERDEX_ADMIN_EXCEPTION   = 8830,
    HYPERDEX_ADMIN_GARBAGE     = 8831
};

struct hyperdex_admin*
hyperdex_admin_create(const char* coordinator, uint16_t port);

void
hyperdex_admin_destroy(struct hyperdex_admin* admin);

int64_t
hyperdex_admin_dump_config(struct hyperdex_admin* admin,
                           enum hyperdex_admin_returncode* status,
                           const char** config);
int64_t
hyperdex_admin_read_only(struct hyperdex_admin* admin,
                         int ro,
                         enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_wait_until_stable(struct hyperdex_admin* admin,
                                 enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_fault_tolerance(struct hyperdex_admin* admin,
                               const char* space,
                               uint64_t ft,
                               enum hyperdex_admin_returncode* status);

int
hyperdex_admin_validate_space(struct hyperdex_admin* admin,
                              const char* description,
                              enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_add_space(struct hyperdex_admin* admin,
                         const char* description,
                         enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_rm_space(struct hyperdex_admin* admin,
                        const char* space,
                        enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_mv_space(struct hyperdex_admin* admin,
                        const char* source,
                        const char* target,
                        enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_list_spaces(struct hyperdex_admin* admin,
                           enum hyperdex_admin_returncode* status,
                           const char** spaces);

int64_t
hyperdex_admin_list_indices(struct hyperdex_admin* admin,
                            const char* space,
                            enum hyperdex_admin_returncode* status,
                            const char** indexes);

int64_t
hyperdex_admin_list_subspaces(struct hyperdex_admin* admin,
                              const char* space,
                              enum hyperdex_admin_returncode* status,
                              const char** subspaces);

int64_t
hyperdex_admin_add_index(struct hyperdex_admin* admin,
                         const char* space,
                         const char* attribute,
                         enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_rm_index(struct hyperdex_admin* admin,
                        uint64_t idxid,
                        enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_server_register(struct hyperdex_admin* admin,
                               uint64_t token,
                               const char* address,
                               enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_server_online(struct hyperdex_admin* admin,
                             uint64_t token,
                             enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_server_offline(struct hyperdex_admin* admin,
                              uint64_t token,
                              enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_server_forget(struct hyperdex_admin* admin,
                             uint64_t token,
                             enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_server_kill(struct hyperdex_admin* admin,
                           uint64_t token,
                           enum hyperdex_admin_returncode* status);

int64_t
hyperdex_admin_backup(struct hyperdex_admin* admin,
                      const char* backup,
                      enum hyperdex_admin_returncode* status,
                      const char** backups);

int64_t
hyperdex_admin_enable_perf_counters(struct hyperdex_admin* admin,
                                    enum hyperdex_admin_returncode* status,
                                    struct hyperdex_admin_perf_counter* pc);

void
hyperdex_admin_disable_perf_counters(struct hyperdex_admin* admin);

int64_t
hyperdex_admin_loop(struct hyperdex_admin* admin, int timeout,
                    enum hyperdex_admin_returncode* status);

int
hyperdex_admin_raw_backup(const char* host, uint16_t port,
                          const char* name,
                          enum hyperdex_admin_returncode* status);

const char*
hyperdex_admin_error_message(struct hyperdex_admin* admin);
const char*
hyperdex_admin_error_location(struct hyperdex_admin* admin);

const char*
hyperdex_admin_returncode_to_string(enum hyperdex_admin_returncode);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* hyperdex_admin_h_ */
