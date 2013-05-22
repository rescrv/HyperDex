/* Copyright (c) 2011-2013, Cornell University
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

#ifndef hyperclient_h_
#define hyperclient_h_

/* C */
#include <stdint.h>
#include <stdlib.h>

/* HyperDex */
#include <hyperdex.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

struct hyperclient;

struct hyperclient_attribute
{
    const char* attr; /* NULL-terminated */
    const char* value;
    size_t value_sz;
    enum hyperdatatype datatype;
};

struct hyperclient_map_attribute
{
    const char* attr; /* NULL-terminated */
    const char* map_key;
    size_t map_key_sz;
    enum hyperdatatype map_key_datatype;
    const char* value;
    size_t value_sz;
    enum hyperdatatype value_datatype;
};

struct hyperclient_attribute_check
{
    const char* attr; /* NULL-terminated */
    const char* value;
    size_t value_sz;
    enum hyperdatatype datatype;
    enum hyperpredicate predicate;
};

/* HyperClient returncode occupies [8448, 8576) */
enum hyperclient_returncode
{
    HYPERCLIENT_SUCCESS      = 8448,
    HYPERCLIENT_NOTFOUND     = 8449,
    HYPERCLIENT_SEARCHDONE   = 8450,
    HYPERCLIENT_CMPFAIL      = 8451,
    HYPERCLIENT_READONLY     = 8452,

    /* Error conditions */
    HYPERCLIENT_UNKNOWNSPACE = 8512,
    HYPERCLIENT_COORDFAIL    = 8513,
    HYPERCLIENT_SERVERERROR  = 8514,
    HYPERCLIENT_POLLFAILED   = 8515,
    HYPERCLIENT_OVERFLOW     = 8516,
    HYPERCLIENT_RECONFIGURE  = 8517,
    HYPERCLIENT_TIMEOUT      = 8519,
    HYPERCLIENT_UNKNOWNATTR  = 8520,
    HYPERCLIENT_DUPEATTR     = 8521,
    HYPERCLIENT_NONEPENDING  = 8523,
    HYPERCLIENT_DONTUSEKEY   = 8524,
    HYPERCLIENT_WRONGTYPE    = 8525,
    HYPERCLIENT_NOMEM        = 8526,
    HYPERCLIENT_BADCONFIG    = 8527,
    HYPERCLIENT_BADSPACE     = 8528,
    HYPERCLIENT_DUPLICATE    = 8529,
    HYPERCLIENT_INTERRUPTED  = 8530,
    HYPERCLIENT_CLUSTER_JUMP = 8531,
    HYPERCLIENT_COORD_LOGGED = 8532,
    HYPERCLIENT_OFFLINE      = 8533,

    /* This should never happen.  It indicates a bug */
    HYPERCLIENT_INTERNAL     = 8573,
    HYPERCLIENT_EXCEPTION    = 8574,
    HYPERCLIENT_GARBAGE      = 8575
};

struct hyperclient*
hyperclient_create(const char* coordinator, uint16_t port);
void
hyperclient_destroy(struct hyperclient* client);

enum hyperclient_returncode
hyperclient_add_space(struct hyperclient* client,
                      const char* str);

enum hyperclient_returncode
hyperclient_rm_space(struct hyperclient* client,
                     const char* str);

int64_t
hyperclient_get(struct hyperclient* client,
                const char* space,
                const char* key, size_t key_sz,
                enum hyperclient_returncode* status,
                struct hyperclient_attribute** attrs, size_t* attrs_sz);

int64_t
hyperclient_put(struct hyperclient* client,
                const char* space,
                const char* key, size_t key_sz,
                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                enum hyperclient_returncode* status);

int64_t
hyperclient_cond_put(struct hyperclient* client,
                     const char* space,
                     const char* key, size_t key_sz,
                     const struct hyperclient_attribute_check* checks, size_t checks_sz,
                     const struct hyperclient_attribute* attrs, size_t attrs_sz,
                     enum hyperclient_returncode* status);

int64_t
hyperclient_put_if_not_exist(struct hyperclient* client,
                             const char* space,
                             const char* key, size_t key_sz,
                             const struct hyperclient_attribute* attrs, size_t attrs_sz,
                             enum hyperclient_returncode* status);

int64_t
hyperclient_del(struct hyperclient* client,
                const char* space,
                const char* key, size_t key_sz,
                enum hyperclient_returncode* status);

int64_t
hyperclient_cond_del(struct hyperclient* client,
                     const char* space,
                     const char* key, size_t key_sz,
                     const struct hyperclient_attribute_check* checks, size_t checks_sz,
                     enum hyperclient_returncode* status);

int64_t
hyperclient_atomic_add(struct hyperclient* client,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_cond_atomic_add(struct hyperclient* client,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status);

int64_t
hyperclient_atomic_sub(struct hyperclient* client,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_cond_atomic_sub(struct hyperclient* client,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status);

int64_t
hyperclient_atomic_mul(struct hyperclient* client,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_cond_atomic_mul(struct hyperclient* client,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status);

int64_t
hyperclient_atomic_div(struct hyperclient* client,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_cond_atomic_div(struct hyperclient* client,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status);

int64_t
hyperclient_atomic_mod(struct hyperclient* client,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_cond_atomic_mod(struct hyperclient* client,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status);

int64_t
hyperclient_atomic_and(struct hyperclient* client,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_cond_atomic_and(struct hyperclient* client,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status);

int64_t
hyperclient_atomic_or(struct hyperclient* client,
                      const char* space,
                      const char* key, size_t key_sz,
                      const struct hyperclient_attribute* attrs, size_t attrs_sz,
                      enum hyperclient_returncode* status);

int64_t
hyperclient_cond_atomic_or(struct hyperclient* client,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_attribute_check* checks, size_t checks_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_atomic_xor(struct hyperclient* client,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_cond_atomic_xor(struct hyperclient* client,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status);

int64_t
hyperclient_string_prepend(struct hyperclient* client,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_cond_string_prepend(struct hyperclient* client,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                enum hyperclient_returncode* status);

int64_t
hyperclient_string_append(struct hyperclient* client,
                          const char* space,
                          const char* key, size_t key_sz,
                          const struct hyperclient_attribute* attrs, size_t attrs_sz,
                          enum hyperclient_returncode* status);

int64_t
hyperclient_cond_string_append(struct hyperclient* client,
                               const char* space,
                               const char* key, size_t key_sz,
                               const struct hyperclient_attribute_check* checks, size_t checks_sz,
                               const struct hyperclient_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status);

int64_t
hyperclient_list_lpush(struct hyperclient* client,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_cond_list_lpush(struct hyperclient* client,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status);

int64_t
hyperclient_list_rpush(struct hyperclient* client,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_cond_list_rpush(struct hyperclient* client,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status);

int64_t
hyperclient_set_add(struct hyperclient* client,
                    const char* space,
                    const char* key, size_t key_sz,
                    const struct hyperclient_attribute* attrs, size_t attrs_sz,
                    enum hyperclient_returncode* status);

int64_t
hyperclient_cond_set_add(struct hyperclient* client,
                         const char* space,
                         const char* key, size_t key_sz,
                         const struct hyperclient_attribute_check* checks, size_t checks_sz,
                         const struct hyperclient_attribute* attrs, size_t attrs_sz,
                         enum hyperclient_returncode* status);

int64_t
hyperclient_set_remove(struct hyperclient* client,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_cond_set_remove(struct hyperclient* client,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status);

int64_t
hyperclient_set_intersect(struct hyperclient* client,
                          const char* space,
                          const char* key, size_t key_sz,
                          const struct hyperclient_attribute* attrs, size_t attrs_sz,
                          enum hyperclient_returncode* status);

int64_t
hyperclient_cond_set_intersect(struct hyperclient* client,
                               const char* space,
                               const char* key, size_t key_sz,
                               const struct hyperclient_attribute_check* checks, size_t checks_sz,
                               const struct hyperclient_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status);

int64_t
hyperclient_set_union(struct hyperclient* client,
                      const char* space,
                      const char* key, size_t key_sz,
                      const struct hyperclient_attribute* attrs, size_t attrs_sz,
                      enum hyperclient_returncode* status);

int64_t
hyperclient_cond_set_union(struct hyperclient* client,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_attribute_check* checks, size_t checks_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_map_add(struct hyperclient* client,
                    const char* space,
                    const char* key, size_t key_sz,
                    const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                    enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_add(struct hyperclient* client,
                         const char* space,
                         const char* key, size_t key_sz,
                         const struct hyperclient_attribute_check* checks, size_t checks_sz,
                         const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                         enum hyperclient_returncode* status);

int64_t
hyperclient_map_remove(struct hyperclient* client,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_remove(struct hyperclient* client,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_add(struct hyperclient* client,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_atomic_add(struct hyperclient* client,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_sub(struct hyperclient* client,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_atomic_sub(struct hyperclient* client,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_mul(struct hyperclient* client,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_atomic_mul(struct hyperclient* client,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_div(struct hyperclient* client,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_atomic_div(struct hyperclient* client,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_mod(struct hyperclient* client,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_atomic_mod(struct hyperclient* client,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_and(struct hyperclient* client,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_atomic_and(struct hyperclient* client,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_or(struct hyperclient* client,
                          const char* space,
                          const char* key, size_t key_sz,
                          const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                          enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_atomic_or(struct hyperclient* client,
                               const char* space,
                               const char* key, size_t key_sz,
                               const struct hyperclient_attribute_check* checks, size_t checks_sz,
                               const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                               enum hyperclient_returncode* status);

int64_t
hyperclient_map_atomic_xor(struct hyperclient* client,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_atomic_xor(struct hyperclient* client,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status);

int64_t
hyperclient_map_string_prepend(struct hyperclient* client,
                               const char* space,
                               const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                               enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_string_prepend(struct hyperclient* client,
                                    const char* space,
                                    const char* key, size_t key_sz,
                                    const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                    const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                    enum hyperclient_returncode* status);

int64_t
hyperclient_map_string_append(struct hyperclient* client,
                              const char* space,
                              const char* key, size_t key_sz,
                              const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                              enum hyperclient_returncode* status);

int64_t
hyperclient_cond_map_string_append(struct hyperclient* client,
                                   const char* space,
                                   const char* key, size_t key_sz,
                                   const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                   const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                   enum hyperclient_returncode* status);

int64_t
hyperclient_search(struct hyperclient* client,
                   const char* space,
                   const struct hyperclient_attribute_check* checks, size_t checks_sz,
                   enum hyperclient_returncode* status,
                   struct hyperclient_attribute** attrs, size_t* attrs_sz);

int64_t
hyperclient_search_describe(struct hyperclient* client,
                            const char* space,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            enum hyperclient_returncode* status,
                            const char** str);

int64_t
hyperclient_sorted_search(struct hyperclient* client,
                          const char* space,
                          const struct hyperclient_attribute_check* checks, size_t checks_sz,
                          const char* sort_by, uint64_t limit, int maximize,
                          enum hyperclient_returncode* status,
                          struct hyperclient_attribute** attrs, size_t* attrs_sz);

int64_t
hyperclient_group_del(struct hyperclient* client,
                      const char* space,
                      const struct hyperclient_attribute_check* checks, size_t checks_sz,
                      enum hyperclient_returncode* status);

int64_t
hyperclient_count(struct hyperclient* client,
                  const char* space,
                  const struct hyperclient_attribute_check* checks, size_t checks_sz,
                  enum hyperclient_returncode* status, uint64_t* result);

int64_t
hyperclient_loop(struct hyperclient* client, int timeout,
                 enum hyperclient_returncode* status);

enum hyperdatatype
hyperclient_attribute_type(struct hyperclient* client,
                           const char* space, const char* name,
                           enum hyperclient_returncode* status);

void
hyperclient_destroy_attrs(struct hyperclient_attribute* attrs, size_t attrs_sz);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* hyperclient_h_ */
