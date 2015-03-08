// Copyright (c) 2013-2015, Cornell University
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
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef hyperdex_client_hpp_
#define hyperdex_client_hpp_

// C++
#include <iostream>

// HyperDex
#include <hyperdex/client.h>

namespace hyperdex
{

class Client
{
    public:
        Client(const char* coordinator, uint16_t port)
            : m_cl(hyperdex_client_create(coordinator, port))
        {
            if (!m_cl)
            {
                throw std::bad_alloc();
            }
        }
        Client(const char* conn_str)
            : m_cl(hyperdex_client_create_conn_str(conn_str))
        {
            if (!m_cl)
            {
                throw std::bad_alloc();
            }
        }
        ~Client() throw ()
        {
            hyperdex_client_destroy(m_cl);
        }

    public:
        int64_t get(const char* space,
                    const char* key, size_t key_sz,
                    hyperdex_client_returncode* status,
                    const hyperdex_client_attribute** attrs, size_t* attrs_sz)
            { return hyperdex_client_get(m_cl, space, key, key_sz, status, attrs, attrs_sz); }
        int64_t get_partial(const char* space,
                            const char* key, size_t key_sz,
                            const char** attrnames, size_t attrnames_sz,
                            hyperdex_client_returncode* status,
                            const hyperdex_client_attribute** attrs, size_t* attrs_sz)
            { return hyperdex_client_get_partial(m_cl, space, key, key_sz, attrnames, attrnames_sz, status, attrs, attrs_sz); }
        int64_t put(const char* space,
                    const char* key, size_t key_sz,
                    const hyperdex_client_attribute* attrs, size_t attrs_sz,
                    hyperdex_client_returncode* status)
            { return hyperdex_client_put(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_put(const char* space,
                         const char* key, size_t key_sz,
                         const hyperdex_client_attribute_check* checks, size_t checks_sz,
                         const hyperdex_client_attribute* attrs, size_t attrs_sz,
                         hyperdex_client_returncode* status)
            { return hyperdex_client_cond_put(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t cond_put_or_create(const char* space,
                                   const char* key, size_t key_sz,
                                   const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                   const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                   hyperdex_client_returncode* status)
            { return hyperdex_client_cond_put_or_create(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_put(const char* space,
                          const hyperdex_client_attribute_check* checks, size_t checks_sz,
                          const hyperdex_client_attribute* attrs, size_t attrs_sz,
                          hyperdex_client_returncode* status,
                          uint64_t* count)
            { return hyperdex_client_group_put(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t put_if_not_exist(const char* space,
                                 const char* key, size_t key_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status)
            { return hyperdex_client_put_if_not_exist(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t del(const char* space,
                    const char* key, size_t key_sz,
                    hyperdex_client_returncode* status)
            { return hyperdex_client_del(m_cl, space, key, key_sz, status); }
        int64_t cond_del(const char* space,
                         const char* key, size_t key_sz,
                         const hyperdex_client_attribute_check* checks, size_t checks_sz,
                         hyperdex_client_returncode* status)
            { return hyperdex_client_cond_del(m_cl, space, key, key_sz, checks, checks_sz, status); }
        int64_t group_del(const char* space,
                          const hyperdex_client_attribute_check* checks, size_t checks_sz,
                          hyperdex_client_returncode* status,
                          uint64_t* count)
            { return hyperdex_client_group_del(m_cl, space, checks, checks_sz, status, count); }
        int64_t atomic_add(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_atomic_add(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_add(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_atomic_add(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_atomic_add(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_atomic_add(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t atomic_sub(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_atomic_sub(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_sub(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_atomic_sub(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_atomic_sub(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_atomic_sub(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t atomic_mul(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_atomic_mul(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_mul(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_atomic_mul(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_atomic_mul(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_atomic_mul(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t atomic_div(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_atomic_div(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_div(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_atomic_div(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_atomic_div(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_atomic_div(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t atomic_mod(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_atomic_mod(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_mod(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_atomic_mod(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_atomic_mod(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_atomic_mod(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t atomic_and(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_atomic_and(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_and(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_atomic_and(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_atomic_and(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_atomic_and(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t atomic_or(const char* space,
                          const char* key, size_t key_sz,
                          const hyperdex_client_attribute* attrs, size_t attrs_sz,
                          hyperdex_client_returncode* status)
            { return hyperdex_client_atomic_or(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_or(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_attribute_check* checks, size_t checks_sz,
                               const hyperdex_client_attribute* attrs, size_t attrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_cond_atomic_or(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_atomic_or(const char* space,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status,
                                uint64_t* count)
            { return hyperdex_client_group_atomic_or(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t atomic_xor(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_atomic_xor(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_xor(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_atomic_xor(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_atomic_xor(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_atomic_xor(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t atomic_min(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_atomic_min(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_min(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_atomic_min(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_atomic_min(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_atomic_min(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t atomic_max(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_atomic_max(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_atomic_max(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_atomic_max(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_atomic_max(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_atomic_max(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t string_prepend(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_attribute* attrs, size_t attrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_string_prepend(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_string_prepend(const char* space,
                                    const char* key, size_t key_sz,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                    hyperdex_client_returncode* status)
            { return hyperdex_client_cond_string_prepend(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_string_prepend(const char* space,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                     hyperdex_client_returncode* status,
                                     uint64_t* count)
            { return hyperdex_client_group_string_prepend(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t string_append(const char* space,
                              const char* key, size_t key_sz,
                              const hyperdex_client_attribute* attrs, size_t attrs_sz,
                              hyperdex_client_returncode* status)
            { return hyperdex_client_string_append(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_string_append(const char* space,
                                   const char* key, size_t key_sz,
                                   const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                   const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                   hyperdex_client_returncode* status)
            { return hyperdex_client_cond_string_append(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_string_append(const char* space,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                    hyperdex_client_returncode* status,
                                    uint64_t* count)
            { return hyperdex_client_group_string_append(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t list_lpush(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_list_lpush(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_list_lpush(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_list_lpush(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_list_lpush(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_list_lpush(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t list_rpush(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_list_rpush(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_list_rpush(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_list_rpush(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_list_rpush(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_list_rpush(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t set_add(const char* space,
                        const char* key, size_t key_sz,
                        const hyperdex_client_attribute* attrs, size_t attrs_sz,
                        hyperdex_client_returncode* status)
            { return hyperdex_client_set_add(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_set_add(const char* space,
                             const char* key, size_t key_sz,
                             const hyperdex_client_attribute_check* checks, size_t checks_sz,
                             const hyperdex_client_attribute* attrs, size_t attrs_sz,
                             hyperdex_client_returncode* status)
            { return hyperdex_client_cond_set_add(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_set_add(const char* space,
                              const hyperdex_client_attribute_check* checks, size_t checks_sz,
                              const hyperdex_client_attribute* attrs, size_t attrs_sz,
                              hyperdex_client_returncode* status,
                              uint64_t* count)
            { return hyperdex_client_group_set_add(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t set_remove(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_set_remove(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_set_remove(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_set_remove(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_set_remove(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_set_remove(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t set_intersect(const char* space,
                              const char* key, size_t key_sz,
                              const hyperdex_client_attribute* attrs, size_t attrs_sz,
                              hyperdex_client_returncode* status)
            { return hyperdex_client_set_intersect(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_set_intersect(const char* space,
                                   const char* key, size_t key_sz,
                                   const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                   const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                   hyperdex_client_returncode* status)
            { return hyperdex_client_cond_set_intersect(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_set_intersect(const char* space,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                    hyperdex_client_returncode* status,
                                    uint64_t* count)
            { return hyperdex_client_group_set_intersect(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t set_union(const char* space,
                          const char* key, size_t key_sz,
                          const hyperdex_client_attribute* attrs, size_t attrs_sz,
                          hyperdex_client_returncode* status)
            { return hyperdex_client_set_union(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_set_union(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_attribute_check* checks, size_t checks_sz,
                               const hyperdex_client_attribute* attrs, size_t attrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_cond_set_union(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_set_union(const char* space,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status,
                                uint64_t* count)
            { return hyperdex_client_group_set_union(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t document_rename(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_document_rename(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_document_rename(const char* space,
                                     const char* key, size_t key_sz,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                     hyperdex_client_returncode* status)
            { return hyperdex_client_cond_document_rename(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_document_rename(const char* space,
                                      const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      hyperdex_client_returncode* status,
                                      uint64_t* count)
            { return hyperdex_client_group_document_rename(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t document_unset(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_attribute* attrs, size_t attrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_document_unset(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_document_unset(const char* space,
                                    const char* key, size_t key_sz,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                    hyperdex_client_returncode* status)
            { return hyperdex_client_cond_document_unset(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_document_unset(const char* space,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                     hyperdex_client_returncode* status,
                                     uint64_t* count)
            { return hyperdex_client_group_document_unset(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t map_add(const char* space,
                        const char* key, size_t key_sz,
                        const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                        hyperdex_client_returncode* status)
            { return hyperdex_client_map_add(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_add(const char* space,
                             const char* key, size_t key_sz,
                             const hyperdex_client_attribute_check* checks, size_t checks_sz,
                             const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                             hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_add(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_add(const char* space,
                              const hyperdex_client_attribute_check* checks, size_t checks_sz,
                              const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                              hyperdex_client_returncode* status,
                              uint64_t* count)
            { return hyperdex_client_group_map_add(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_remove(const char* space,
                           const char* key, size_t key_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           hyperdex_client_returncode* status)
            { return hyperdex_client_map_remove(m_cl, space, key, key_sz, attrs, attrs_sz, status); }
        int64_t cond_map_remove(const char* space,
                                const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_remove(m_cl, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); }
        int64_t group_map_remove(const char* space,
                                 const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                 const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 hyperdex_client_returncode* status,
                                 uint64_t* count)
            { return hyperdex_client_group_map_remove(m_cl, space, checks, checks_sz, attrs, attrs_sz, status, count); }
        int64_t map_atomic_add(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_map_atomic_add(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_add(const char* space,
                                    const char* key, size_t key_sz,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_atomic_add(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_atomic_add(const char* space,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     hyperdex_client_returncode* status,
                                     uint64_t* count)
            { return hyperdex_client_group_map_atomic_add(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_atomic_sub(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_map_atomic_sub(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_sub(const char* space,
                                    const char* key, size_t key_sz,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_atomic_sub(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_atomic_sub(const char* space,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     hyperdex_client_returncode* status,
                                     uint64_t* count)
            { return hyperdex_client_group_map_atomic_sub(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_atomic_mul(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_map_atomic_mul(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_mul(const char* space,
                                    const char* key, size_t key_sz,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_atomic_mul(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_atomic_mul(const char* space,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     hyperdex_client_returncode* status,
                                     uint64_t* count)
            { return hyperdex_client_group_map_atomic_mul(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_atomic_div(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_map_atomic_div(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_div(const char* space,
                                    const char* key, size_t key_sz,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_atomic_div(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_atomic_div(const char* space,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     hyperdex_client_returncode* status,
                                     uint64_t* count)
            { return hyperdex_client_group_map_atomic_div(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_atomic_mod(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_map_atomic_mod(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_mod(const char* space,
                                    const char* key, size_t key_sz,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_atomic_mod(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_atomic_mod(const char* space,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     hyperdex_client_returncode* status,
                                     uint64_t* count)
            { return hyperdex_client_group_map_atomic_mod(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_atomic_and(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_map_atomic_and(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_and(const char* space,
                                    const char* key, size_t key_sz,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_atomic_and(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_atomic_and(const char* space,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     hyperdex_client_returncode* status,
                                     uint64_t* count)
            { return hyperdex_client_group_map_atomic_and(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_atomic_or(const char* space,
                              const char* key, size_t key_sz,
                              const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                              hyperdex_client_returncode* status)
            { return hyperdex_client_map_atomic_or(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_or(const char* space,
                                   const char* key, size_t key_sz,
                                   const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                   const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                   hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_atomic_or(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_atomic_or(const char* space,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperdex_client_returncode* status,
                                    uint64_t* count)
            { return hyperdex_client_group_map_atomic_or(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_atomic_xor(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_map_atomic_xor(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_xor(const char* space,
                                    const char* key, size_t key_sz,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_atomic_xor(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_atomic_xor(const char* space,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     hyperdex_client_returncode* status,
                                     uint64_t* count)
            { return hyperdex_client_group_map_atomic_xor(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_string_prepend(const char* space,
                                   const char* key, size_t key_sz,
                                   const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                   hyperdex_client_returncode* status)
            { return hyperdex_client_map_string_prepend(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_string_prepend(const char* space,
                                        const char* key, size_t key_sz,
                                        const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                        const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                        hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_string_prepend(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_string_prepend(const char* space,
                                         const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                         const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                         hyperdex_client_returncode* status,
                                         uint64_t* count)
            { return hyperdex_client_group_map_string_prepend(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_string_append(const char* space,
                                  const char* key, size_t key_sz,
                                  const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                  hyperdex_client_returncode* status)
            { return hyperdex_client_map_string_append(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_string_append(const char* space,
                                       const char* key, size_t key_sz,
                                       const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                       const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                       hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_string_append(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_string_append(const char* space,
                                        const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                        const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                        hyperdex_client_returncode* status,
                                        uint64_t* count)
            { return hyperdex_client_group_map_string_append(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_atomic_min(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_map_atomic_min(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_min(const char* space,
                                    const char* key, size_t key_sz,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_atomic_min(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_atomic_min(const char* space,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     hyperdex_client_returncode* status,
                                     uint64_t* count)
            { return hyperdex_client_group_map_atomic_min(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t map_atomic_max(const char* space,
                               const char* key, size_t key_sz,
                               const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                               hyperdex_client_returncode* status)
            { return hyperdex_client_map_atomic_max(m_cl, space, key, key_sz, mapattrs, mapattrs_sz, status); }
        int64_t cond_map_atomic_max(const char* space,
                                    const char* key, size_t key_sz,
                                    const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                    hyperdex_client_returncode* status)
            { return hyperdex_client_cond_map_atomic_max(m_cl, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status); }
        int64_t group_map_atomic_max(const char* space,
                                     const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     hyperdex_client_returncode* status,
                                     uint64_t* count)
            { return hyperdex_client_group_map_atomic_max(m_cl, space, checks, checks_sz, mapattrs, mapattrs_sz, status, count); }
        int64_t search(const char* space,
                       const hyperdex_client_attribute_check* checks, size_t checks_sz,
                       hyperdex_client_returncode* status,
                       const hyperdex_client_attribute** attrs, size_t* attrs_sz)
            { return hyperdex_client_search(m_cl, space, checks, checks_sz, status, attrs, attrs_sz); }
        int64_t search_describe(const char* space,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                hyperdex_client_returncode* status,
                                const char** description)
            { return hyperdex_client_search_describe(m_cl, space, checks, checks_sz, status, description); }
        int64_t sorted_search(const char* space,
                              const hyperdex_client_attribute_check* checks, size_t checks_sz,
                              const char* sort_by,
                              uint64_t limit,
                              int maxmin,
                              hyperdex_client_returncode* status,
                              const hyperdex_client_attribute** attrs, size_t* attrs_sz)
            { return hyperdex_client_sorted_search(m_cl, space, checks, checks_sz, sort_by, limit, maxmin, status, attrs, attrs_sz); }
        int64_t count(const char* space,
                      const hyperdex_client_attribute_check* checks, size_t checks_sz,
                      hyperdex_client_returncode* status,
                      uint64_t* count)
            { return hyperdex_client_count(m_cl, space, checks, checks_sz, status, count); }

    public:
        void clear_auth_context()
            { return hyperdex_client_clear_auth_context(m_cl); }
        void set_auth_context(const char** macaroons, size_t macaroons_sz)
            { return hyperdex_client_set_auth_context(m_cl, macaroons, macaroons_sz); }

    public:
        int64_t loop(int timeout, hyperdex_client_returncode* status)
            { return hyperdex_client_loop(m_cl, timeout, status); }
        int poll_fd()
            { return hyperdex_client_poll_fd(m_cl); }
        int block(int timeout)
            { return hyperdex_client_block(m_cl, timeout); }
        std::string error_message()
            { return hyperdex_client_error_message(m_cl); }
        std::string error_location()
            { return hyperdex_client_error_location(m_cl); }

    private:
        Client(const Client&);
        Client& operator = (const Client&);

    private:
        hyperdex_client* m_cl;
};

} // namespace hyperdex

std::ostream&
operator << (std::ostream& lhs, hyperdex_client_returncode rhs);

#endif // hyperdex_client_hpp_
