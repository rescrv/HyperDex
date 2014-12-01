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

#ifndef hyperdex_datastructures_h_
#define hyperdex_datastructures_h_

/* HyperDex */
#include <hyperdex/client.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/* types */
struct hyperdex_ds_arena;

struct hyperdex_ds_list;
struct hyperdex_ds_set;
struct hyperdex_ds_map;

struct hyperdex_ds_iterator
{
    enum hyperdatatype datatype;
    const char* value;
    const char* value_end;
    const char* progress;
};

/* errors */
enum hyperdex_ds_returncode
{
    HYPERDEX_DS_SUCCESS,
    HYPERDEX_DS_NOMEM,
    HYPERDEX_DS_MIXED_TYPES,
    HYPERDEX_DS_WRONG_STATE,
    HYPERDEX_DS_STRING_TOO_LONG
};

/* arena manipulation */
struct hyperdex_ds_arena*
hyperdex_ds_arena_create();

void
hyperdex_ds_arena_destroy(struct hyperdex_ds_arena* arena);

void*
hyperdex_ds_malloc(struct hyperdex_ds_arena* arena, size_t sz);

/* client parameters */
struct hyperdex_client_attribute*
hyperdex_ds_allocate_attribute(struct hyperdex_ds_arena* arena, size_t sz);

struct hyperdex_client_attribute_check*
hyperdex_ds_allocate_attribute_check(struct hyperdex_ds_arena* arena, size_t sz);

struct hyperdex_client_map_attribute*
hyperdex_ds_allocate_map_attribute(struct hyperdex_ds_arena* arena, size_t sz);

/* pack/unpack ints/floats */
void
hyperdex_ds_pack_int(int64_t num, char* buf);
int
hyperdex_ds_unpack_int(const char* buf, size_t buf_sz, int64_t* num);

void
hyperdex_ds_pack_float(double num, char* buf);
int
hyperdex_ds_unpack_float(const char* buf, size_t buf_sz, double* num);

/* copy strings/ints/floats */
int
hyperdex_ds_copy_string(struct hyperdex_ds_arena* arena,
                        const char* str, size_t str_sz,
                        enum hyperdex_ds_returncode* status,
                        const char** value, size_t* value_sz);
int
hyperdex_ds_copy_int(struct hyperdex_ds_arena* arena, int64_t num,
                     enum hyperdex_ds_returncode* status,
                     const char** value, size_t* value_sz);
int
hyperdex_ds_copy_float(struct hyperdex_ds_arena* arena, double num,
                       enum hyperdex_ds_returncode* status,
                       const char** value, size_t* value_sz);

/* pack lists */
struct hyperdex_ds_list*
hyperdex_ds_allocate_list(struct hyperdex_ds_arena* arena);
int
hyperdex_ds_list_append_string(struct hyperdex_ds_list* list,
                               const char* str, size_t str_sz,
                               enum hyperdex_ds_returncode* status);
int
hyperdex_ds_list_append_int(struct hyperdex_ds_list* list,
                            int64_t num,
                            enum hyperdex_ds_returncode* status);
int
hyperdex_ds_list_append_float(struct hyperdex_ds_list* list,
                              double num,
                              enum hyperdex_ds_returncode* status);
int
hyperdex_ds_list_finalize(struct hyperdex_ds_list* list,
                          enum hyperdex_ds_returncode* status,
                          const char** value, size_t* value_sz,
                          enum hyperdatatype* datatype);

/* pack sets */
struct hyperdex_ds_set*
hyperdex_ds_allocate_set(struct hyperdex_ds_arena* arena);
int
hyperdex_ds_set_insert_string(struct hyperdex_ds_set* set, const char* str, size_t str_sz,
                              enum hyperdex_ds_returncode* status);
int
hyperdex_ds_set_insert_int(struct hyperdex_ds_set* set, int64_t num,
                           enum hyperdex_ds_returncode* status);
int
hyperdex_ds_set_insert_float(struct hyperdex_ds_set* set, double num,
                             enum hyperdex_ds_returncode* status);
int
hyperdex_ds_set_finalize(struct hyperdex_ds_set*,
                         enum hyperdex_ds_returncode* status,
                         const char** value, size_t* value_sz,
                         enum hyperdatatype* datatype);

/* pack maps */
struct hyperdex_ds_map*
hyperdex_ds_allocate_map(struct hyperdex_ds_arena* arena);
int
hyperdex_ds_map_insert_key_string(struct hyperdex_ds_map* map,
                                  const char* str, size_t str_sz,
                                  enum hyperdex_ds_returncode* status);
int
hyperdex_ds_map_insert_val_string(struct hyperdex_ds_map* map,
                                  const char* str, size_t str_sz,
                                  enum hyperdex_ds_returncode* status);
int
hyperdex_ds_map_insert_key_int(struct hyperdex_ds_map* map, int64_t num,
                               enum hyperdex_ds_returncode* status);
int
hyperdex_ds_map_insert_val_int(struct hyperdex_ds_map* map, int64_t num,
                               enum hyperdex_ds_returncode* status);
int
hyperdex_ds_map_insert_key_float(struct hyperdex_ds_map* map, double num,
                                 enum hyperdex_ds_returncode* status);
int
hyperdex_ds_map_insert_val_float(struct hyperdex_ds_map* map, double num,
                                 enum hyperdex_ds_returncode* status);
int
hyperdex_ds_map_finalize(struct hyperdex_ds_map*,
                         enum hyperdex_ds_returncode* status,
                         const char** value, size_t* value_sz,
                         enum hyperdatatype* datatype);

/* iterate datatypes */
void
hyperdex_ds_iterator_init(struct hyperdex_ds_iterator* iter,
                          enum hyperdatatype datatype,
                          const char* value,
                          size_t value_sz);
/* list */
int
hyperdex_ds_iterate_list_string_next(struct hyperdex_ds_iterator* iter,
                                     const char** str, size_t* str_sz);
int
hyperdex_ds_iterate_list_int_next(struct hyperdex_ds_iterator* iter, int64_t* num);
int
hyperdex_ds_iterate_list_float_next(struct hyperdex_ds_iterator* iter, double* num);
/* set */
int
hyperdex_ds_iterate_set_string_next(struct hyperdex_ds_iterator* iter,
                                    const char** str, size_t* str_sz);
int
hyperdex_ds_iterate_set_int_next(struct hyperdex_ds_iterator* iter, int64_t* num);
int
hyperdex_ds_iterate_set_float_next(struct hyperdex_ds_iterator* iter, double* num);
/* map(string, *) */
int
hyperdex_ds_iterate_map_string_string_next(struct hyperdex_ds_iterator* iter,
                                           const char** key, size_t* key_sz,
                                           const char** val, size_t* val_sz);
int
hyperdex_ds_iterate_map_string_int_next(struct hyperdex_ds_iterator* iter,
                                        const char** key, size_t* key_sz,
                                        int64_t* val);
int
hyperdex_ds_iterate_map_string_float_next(struct hyperdex_ds_iterator* iter,
                                          const char** key, size_t* key_sz,
                                          double* val);
/* map(int, *) */
int
hyperdex_ds_iterate_map_int_string_next(struct hyperdex_ds_iterator* iter,
                                        int64_t* key,
                                        const char** val, size_t* val_sz);
int
hyperdex_ds_iterate_map_int_int_next(struct hyperdex_ds_iterator* iter,
                                     int64_t* key,
                                     int64_t* val);
int
hyperdex_ds_iterate_map_int_float_next(struct hyperdex_ds_iterator* iter,
                                       int64_t* key,
                                       double* val);
/* map(float, *) */
int
hyperdex_ds_iterate_map_float_string_next(struct hyperdex_ds_iterator* iter,
                                          double* key,
                                          const char** val, size_t* val_sz);
int
hyperdex_ds_iterate_map_float_int_next(struct hyperdex_ds_iterator* iter,
                                       double* key,
                                       int64_t* val);
int
hyperdex_ds_iterate_map_float_float_next(struct hyperdex_ds_iterator* iter,
                                         double* key,
                                         double* val);

#ifdef __cplusplus
} /* extern "C" */

/* C++ */
#include <iostream>

std::ostream&
operator << (std::ostream& lhs, hyperdex_ds_returncode rhs);

#endif /* __cplusplus */
#endif /* hyperdex_datastructures_h_ */
