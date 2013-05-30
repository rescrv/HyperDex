// Copyright (c) 2013, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of HyperDex nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

// GENERATED!  Do not modify this file directly

#ifndef hyperclient_hpp_
#define hyperclient_hpp_

// HyperDex
#include <hyperclient.h>

class HyperClient
{
    public:
        HyperClient(const char* coordinator, uint16_t port)
            : m_cl(hyperclient_create(coordinator, port))
        {
            if (!m_cl)
            {
                throw std::bad_alloc();
            }
        }
        ~HyperClient() throw ()
        {
            hyperclient_destroy(m_cl);
        }

    public:
        enum hyperclient_returncode add_space(const char* str)
            { return hyperclient_add_space(m_cl, str); }
        enum hyperclient_returncode rm_space(const char* str)
            { return hyperclient_rm_space(m_cl, str); }
        int64_t get(const char* space, const char* key, size_t key_sz,
                    hyperclient_returncode* status,
                    struct hyperclient_attribute** attrs, size_t* attrs_sz)
            { return hyperclient_get(m_cl, space, key, key_sz, status, attrs, attrs_sz); }
        int64_t put(const char* space, const char* key, size_t key_sz,
                    const struct hyperclient_attribute* attrs, size_t attrs_sz,
                    hyperclient_returncode* status)
            { return hyperclient_put(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_put(const char* space, const char* key, size_t key_sz,
                         const struct hyperclient_attribute_check* checks, size_t checks_sz,
                         const struct hyperclient_attribute* attrs, size_t attrs_sz,
                         hyperclient_returncode* status)
            { return hyperclient_cond_put(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t put_if_not_exist(const char* space, const char* key, size_t key_sz,
                                 const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                 hyperclient_returncode* status)
            { return hyperclient_put_if_not_exist(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t del(const char* space, const char* key, size_t key_sz,
                    hyperclient_returncode* status)
            { return hyperclient_del(m_cl, space, key, key_sz, status); }
        int64_t cond_del(const char* space, const char* key, size_t key_sz,
                         const struct hyperclient_attribute_check* checks, size_t checks_sz,
                         hyperclient_returncode* status)
            { return hyperclient_cond_del(m_cl, space, key, key_sz, checks, checks_sz, status); }
        int64_t atomic_add(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status)
            { return hyperclient_atomic_add(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_add(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
            { return hyperclient_cond_atomic_add(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t atomic_sub(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status)
            { return hyperclient_atomic_sub(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_sub(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
            { return hyperclient_cond_atomic_sub(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t atomic_mul(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status)
            { return hyperclient_atomic_mul(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_mul(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
            { return hyperclient_cond_atomic_mul(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t atomic_div(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status)
            { return hyperclient_atomic_div(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_div(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
            { return hyperclient_cond_atomic_div(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t atomic_mod(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status)
            { return hyperclient_atomic_mod(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_mod(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
            { return hyperclient_cond_atomic_mod(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t atomic_and(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status)
            { return hyperclient_atomic_and(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_and(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
            { return hyperclient_cond_atomic_and(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t atomic_or(const char* space, const char* key, size_t key_sz,
                          const struct hyperclient_attribute* attrs, size_t attrs_sz,
                          hyperclient_returncode* status)
            { return hyperclient_atomic_or(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_or(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_attribute_check* checks, size_t checks_sz,
                               const struct hyperclient_attribute* attrs, size_t attrs_sz,
                               hyperclient_returncode* status)
            { return hyperclient_cond_atomic_or(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t atomic_xor(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status)
            { return hyperclient_atomic_xor(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_xor(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
            { return hyperclient_cond_atomic_xor(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t string_prepend(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_attribute* attrs, size_t attrs_sz,
                               hyperclient_returncode* status)
            { return hyperclient_string_prepend(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_string_prepend(const char* space, const char* key, size_t key_sz,
                                    const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                    const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                    hyperclient_returncode* status)
            { return hyperclient_cond_string_prepend(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t string_append(const char* space, const char* key, size_t key_sz,
                              const struct hyperclient_attribute* attrs, size_t attrs_sz,
                              hyperclient_returncode* status)
            { return hyperclient_string_append(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_string_append(const char* space, const char* key, size_t key_sz,
                                   const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                   const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                   hyperclient_returncode* status)
            { return hyperclient_cond_string_append(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t list_lpush(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status)
            { return hyperclient_list_lpush(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_list_lpush(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
            { return hyperclient_cond_list_lpush(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t list_rpush(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status)
            { return hyperclient_list_rpush(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_list_rpush(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
            { return hyperclient_cond_list_rpush(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t set_add(const char* space, const char* key, size_t key_sz,
                        const struct hyperclient_attribute* attrs, size_t attrs_sz,
                        hyperclient_returncode* status)
            { return hyperclient_set_add(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_set_add(const char* space, const char* key, size_t key_sz,
                             const struct hyperclient_attribute_check* checks, size_t checks_sz,
                             const struct hyperclient_attribute* attrs, size_t attrs_sz,
                             hyperclient_returncode* status)
            { return hyperclient_cond_set_add(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t set_remove(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status)
            { return hyperclient_set_remove(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_set_remove(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
            { return hyperclient_cond_set_remove(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t set_intersect(const char* space, const char* key, size_t key_sz,
                              const struct hyperclient_attribute* attrs, size_t attrs_sz,
                              hyperclient_returncode* status)
            { return hyperclient_set_intersect(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_set_intersect(const char* space, const char* key, size_t key_sz,
                                   const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                   const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                   hyperclient_returncode* status)
            { return hyperclient_cond_set_intersect(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t set_union(const char* space, const char* key, size_t key_sz,
                          const struct hyperclient_attribute* attrs, size_t attrs_sz,
                          hyperclient_returncode* status)
            { return hyperclient_set_union(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_set_union(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_attribute_check* checks, size_t checks_sz,
                               const struct hyperclient_attribute* attrs, size_t attrs_sz,
                               hyperclient_returncode* status)
            { return hyperclient_cond_set_union(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t map_add(const char* space, const char* key, size_t key_sz,
                        const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                        hyperclient_returncode* status)
            { return hyperclient_map_add(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_add(const char* space, const char* key, size_t key_sz,
                             const struct hyperclient_attribute_check* checks, size_t checks_sz,
                             const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                             hyperclient_returncode* status)
            { return hyperclient_cond_map_add(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t map_remove(const char* space, const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status)
            { return hyperclient_map_remove(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_map_remove(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
            { return hyperclient_cond_map_remove(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t map_atomic_add(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperclient_returncode* status)
            { return hyperclient_map_atomic_add(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_add(const char* space, const char* key, size_t key_sz,
                                    const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                    const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperclient_returncode* status)
            { return hyperclient_cond_map_atomic_add(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t map_atomic_sub(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperclient_returncode* status)
            { return hyperclient_map_atomic_sub(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_sub(const char* space, const char* key, size_t key_sz,
                                    const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                    const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperclient_returncode* status)
            { return hyperclient_cond_map_atomic_sub(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t map_atomic_mul(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperclient_returncode* status)
            { return hyperclient_map_atomic_mul(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_mul(const char* space, const char* key, size_t key_sz,
                                    const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                    const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperclient_returncode* status)
            { return hyperclient_cond_map_atomic_mul(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t map_atomic_div(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperclient_returncode* status)
            { return hyperclient_map_atomic_div(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_div(const char* space, const char* key, size_t key_sz,
                                    const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                    const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperclient_returncode* status)
            { return hyperclient_cond_map_atomic_div(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t map_atomic_mod(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperclient_returncode* status)
            { return hyperclient_map_atomic_mod(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_mod(const char* space, const char* key, size_t key_sz,
                                    const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                    const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperclient_returncode* status)
            { return hyperclient_cond_map_atomic_mod(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t map_atomic_and(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperclient_returncode* status)
            { return hyperclient_map_atomic_and(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_and(const char* space, const char* key, size_t key_sz,
                                    const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                    const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperclient_returncode* status)
            { return hyperclient_cond_map_atomic_and(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t map_atomic_or(const char* space, const char* key, size_t key_sz,
                              const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                              hyperclient_returncode* status)
            { return hyperclient_map_atomic_or(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_or(const char* space, const char* key, size_t key_sz,
                                   const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                   const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                   hyperclient_returncode* status)
            { return hyperclient_cond_map_atomic_or(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t map_atomic_xor(const char* space, const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperclient_returncode* status)
            { return hyperclient_map_atomic_xor(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_xor(const char* space, const char* key, size_t key_sz,
                                    const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                    const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperclient_returncode* status)
            { return hyperclient_cond_map_atomic_xor(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t map_string_prepend(const char* space, const char* key, size_t key_sz,
                                   const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                   hyperclient_returncode* status)
            { return hyperclient_map_string_prepend(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_string_prepend(const char* space, const char* key, size_t key_sz,
                                        const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                        const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                        hyperclient_returncode* status)
            { return hyperclient_cond_map_string_prepend(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t map_string_append(const char* space, const char* key, size_t key_sz,
                                  const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                  hyperclient_returncode* status)
            { return hyperclient_map_string_append(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_string_append(const char* space, const char* key, size_t key_sz,
                                       const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                       const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                       hyperclient_returncode* status)
            { return hyperclient_cond_map_string_append(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t search(const char* space,
                       const struct hyperclient_attribute_check* checks, size_t checks_sz,
                       enum hyperclient_returncode* status,
                       struct hyperclient_attribute** attrs, size_t* attrs_sz)
            { return hyperclient_search(m_cl, space, checks, checks_sz, status, attrs, attrs_sz); }
        int64_t search_describe(const char* space,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                enum hyperclient_returncode* status, const char** str)
            { return hyperclient_search_describe(m_cl, space, checks, checks_sz, status, str); }
        int64_t sorted_search(const char* space,
                              const struct hyperclient_attribute_check* checks, size_t checks_sz,
                              const char* sort_by, uint64_t limit, int maximize,
                              enum hyperclient_returncode* status,
                              struct hyperclient_attribute** attrs, size_t* attrs_sz)
            { return hyperclient_sorted_search(m_cl, space, checks, checks_sz, sort_by, limit, maximize, status, attrs, attrs_sz); }
        int64_t group_del(const char* space,
                          const struct hyperclient_attribute_check* checks, size_t checks_sz,
                          enum hyperclient_returncode* status)
            { return hyperclient_group_del(m_cl, space, checks, checks_sz, status); }
        int64_t count(const char* space,
                      const struct hyperclient_attribute_check* checks, size_t checks_sz,
                      enum hyperclient_returncode* status, uint64_t* result)
            { return hyperclient_count(m_cl, space, checks, checks_sz, status, result); }

    public:
        int64_t loop(int timeout, hyperclient_returncode* status)
            { return hyperclient_loop(m_cl, timeout, status); }

    private:
        HyperClient(const HyperClient&);
        HyperClient& operator = (const HyperClient&);

    private:
        hyperclient* m_cl;
};

std::ostream&
operator << (std::ostream& lhs, hyperclient_returncode rhs);

#endif // hyperclient_hpp_
