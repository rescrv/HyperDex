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

// HyperDex
#include "common/macros.h"
#include "client/client.h"
#include "client/hyperclient.h"

#define C_WRAP_EXCEPT(X) \
    try \
    { \
        X \
    } \
    catch (po6::error& e) \
    { \
        errno = e; \
        *status = HYPERCLIENT_EXCEPTION; \
        return -1; \
    } \
    catch (std::bad_alloc& ba) \
    { \
        errno = ENOMEM; \
        *status = HYPERCLIENT_NOMEM; \
        return -1; \
    } \
    catch (...) \
    { \
        *status = HYPERCLIENT_EXCEPTION; \
        return -1; \
    }

#ifdef __cplusplus
extern "C"
{
#endif // __cplusplus

struct hyperclient*
hyperclient_create(const char* coordinator, uint16_t port)
{
    try
    {
        return reinterpret_cast<struct hyperclient*>(new hyperdex::client(coordinator, port));
    }
    catch (po6::error& e)
    {
        errno = e;
        return NULL;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        return NULL;
    }
}

void
hyperclient_destroy(struct hyperclient* client)
{
    delete reinterpret_cast<hyperdex::client*>(client);
}

void
hyperclient_destroy_attrs(struct hyperclient_attribute* attrs, size_t /*attrs_sz*/)
{
    free(attrs);
}

enum hyperclient_returncode
hyperclient_add_space(struct hyperclient* _cl,
                      const char* str)
{
    try
    {
        hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
        return cl->add_space(str);
    }
    catch (po6::error& e)
    {
        errno = e;
        return HYPERCLIENT_EXCEPTION;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return HYPERCLIENT_NOMEM;
    }
    catch (...)
    {
        return HYPERCLIENT_EXCEPTION;
    }
}

enum hyperclient_returncode
hyperclient_rm_space(struct hyperclient* _cl,
                     const char* str)
{
    try
    {
        hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
        return cl->rm_space(str);
    }
    catch (po6::error& e)
    {
        errno = e;
        return HYPERCLIENT_EXCEPTION;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return HYPERCLIENT_NOMEM;
    }
    catch (...)
    {
        return HYPERCLIENT_EXCEPTION;
    }
}

int64_t
hyperclient_get(struct hyperclient* _cl,
                const char* space,
                const char* key, size_t key_sz,
                enum hyperclient_returncode* status,
                struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->get(space, key, key_sz, status, attrs, attrs_sz);
    );
}

int64_t
hyperclient_put(struct hyperclient* _cl,
                const char* space,
                const char* key, size_t key_sz,
                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(put), strlen(XSTR(put)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_put(struct hyperclient* _cl,
                     const char* space,
                     const char* key, size_t key_sz,
                     const struct hyperclient_attribute_check* checks, size_t checks_sz,
                     const struct hyperclient_attribute* attrs, size_t attrs_sz,
                     enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_put), strlen(XSTR(cond_put)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_put_if_not_exist(struct hyperclient* _cl,
                             const char* space,
                             const char* key, size_t key_sz,
                             const struct hyperclient_attribute* attrs, size_t attrs_sz,
                             enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(put_if_not_exist), strlen(XSTR(put_if_not_exist)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_del(struct hyperclient* _cl,
                const char* space,
                const char* key, size_t key_sz,
                enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(del), strlen(XSTR(del)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_del(struct hyperclient* _cl,
                     const char* space,
                     const char* key, size_t key_sz,
                     const struct hyperclient_attribute_check* checks, size_t checks_sz,
                     enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_del), strlen(XSTR(cond_del)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, NULL, 0, status);
    );
}

int64_t
hyperclient_atomic_add(struct hyperclient* _cl,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(atomic_add), strlen(XSTR(atomic_add)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_atomic_add(struct hyperclient* _cl,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_atomic_add), strlen(XSTR(cond_atomic_add)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_atomic_sub(struct hyperclient* _cl,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(atomic_sub), strlen(XSTR(atomic_sub)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_atomic_sub(struct hyperclient* _cl,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_atomic_sub), strlen(XSTR(cond_atomic_sub)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_atomic_mul(struct hyperclient* _cl,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(atomic_mul), strlen(XSTR(atomic_mul)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_atomic_mul(struct hyperclient* _cl,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_atomic_mul), strlen(XSTR(cond_atomic_mul)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_atomic_div(struct hyperclient* _cl,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(atomic_div), strlen(XSTR(atomic_div)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_atomic_div(struct hyperclient* _cl,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_atomic_div), strlen(XSTR(cond_atomic_div)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_atomic_mod(struct hyperclient* _cl,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(atomic_mod), strlen(XSTR(atomic_mod)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_atomic_mod(struct hyperclient* _cl,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_atomic_mod), strlen(XSTR(cond_atomic_mod)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_atomic_and(struct hyperclient* _cl,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(atomic_and), strlen(XSTR(atomic_and)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_atomic_and(struct hyperclient* _cl,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_atomic_and), strlen(XSTR(cond_atomic_and)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_atomic_or(struct hyperclient* _cl,
                      const char* space,
                      const char* key, size_t key_sz,
                      const struct hyperclient_attribute* attrs, size_t attrs_sz,
                      enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(atomic_or), strlen(XSTR(atomic_or)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_atomic_or(struct hyperclient* _cl,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_attribute_check* checks, size_t checks_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_atomic_or), strlen(XSTR(cond_atomic_or)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_atomic_xor(struct hyperclient* _cl,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(atomic_xor), strlen(XSTR(atomic_xor)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_atomic_xor(struct hyperclient* _cl,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_atomic_xor), strlen(XSTR(cond_atomic_xor)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_string_prepend(struct hyperclient* _cl,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(string_prepend), strlen(XSTR(string_prepend)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_string_prepend(struct hyperclient* _cl,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_string_prepend), strlen(XSTR(cond_string_prepend)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_string_append(struct hyperclient* _cl,
                          const char* space,
                          const char* key, size_t key_sz,
                          const struct hyperclient_attribute* attrs, size_t attrs_sz,
                          enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(string_append), strlen(XSTR(string_append)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_string_append(struct hyperclient* _cl,
                               const char* space,
                               const char* key, size_t key_sz,
                               const struct hyperclient_attribute_check* checks, size_t checks_sz,
                               const struct hyperclient_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_string_append), strlen(XSTR(cond_string_append)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_list_lpush(struct hyperclient* _cl,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(list_lpush), strlen(XSTR(list_lpush)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_list_lpush(struct hyperclient* _cl,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_list_lpush), strlen(XSTR(cond_list_lpush)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_list_rpush(struct hyperclient* _cl,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(list_rpush), strlen(XSTR(list_rpush)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_list_rpush(struct hyperclient* _cl,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_list_rpush), strlen(XSTR(cond_list_rpush)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_set_add(struct hyperclient* _cl,
                    const char* space,
                    const char* key, size_t key_sz,
                    const struct hyperclient_attribute* attrs, size_t attrs_sz,
                    enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(set_add), strlen(XSTR(set_add)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_set_add(struct hyperclient* _cl,
                         const char* space,
                         const char* key, size_t key_sz,
                         const struct hyperclient_attribute_check* checks, size_t checks_sz,
                         const struct hyperclient_attribute* attrs, size_t attrs_sz,
                         enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_set_add), strlen(XSTR(cond_set_add)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_set_remove(struct hyperclient* _cl,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(set_remove), strlen(XSTR(set_remove)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_set_remove(struct hyperclient* _cl,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_set_remove), strlen(XSTR(cond_set_remove)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_set_intersect(struct hyperclient* _cl,
                          const char* space,
                          const char* key, size_t key_sz,
                          const struct hyperclient_attribute* attrs, size_t attrs_sz,
                          enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(set_intersect), strlen(XSTR(set_intersect)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_set_intersect(struct hyperclient* _cl,
                               const char* space,
                               const char* key, size_t key_sz,
                               const struct hyperclient_attribute_check* checks, size_t checks_sz,
                               const struct hyperclient_attribute* attrs, size_t attrs_sz,
                               enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_set_intersect), strlen(XSTR(cond_set_intersect)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_set_union(struct hyperclient* _cl,
                      const char* space,
                      const char* key, size_t key_sz,
                      const struct hyperclient_attribute* attrs, size_t attrs_sz,
                      enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(set_union), strlen(XSTR(set_union)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_set_union(struct hyperclient* _cl,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_attribute_check* checks, size_t checks_sz,
                           const struct hyperclient_attribute* attrs, size_t attrs_sz,
                           enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_set_union), strlen(XSTR(cond_set_union)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_map_add(struct hyperclient* _cl,
                    const char* space,
                    const char* key, size_t key_sz,
                    const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                    enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_add), strlen(XSTR(map_add)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_cond_map_add(struct hyperclient* _cl,
                         const char* space,
                         const char* key, size_t key_sz,
                         const struct hyperclient_attribute_check* checks, size_t checks_sz,
                         const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                         enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_add), strlen(XSTR(cond_map_add)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_map_remove(struct hyperclient* _cl,
                       const char* space,
                       const char* key, size_t key_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_remove), strlen(XSTR(map_remove)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_cond_map_remove(struct hyperclient* _cl,
                            const char* space,
                            const char* key, size_t key_sz,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            const struct hyperclient_attribute* attrs, size_t attrs_sz,
                            enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_remove), strlen(XSTR(cond_map_remove)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, NULL, 0, status);
    );
}

int64_t
hyperclient_map_atomic_add(struct hyperclient* _cl,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_atomic_add), strlen(XSTR(map_atomic_add)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_cond_map_atomic_add(struct hyperclient* _cl,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_atomic_add), strlen(XSTR(cond_map_atomic_add)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_map_atomic_sub(struct hyperclient* _cl,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_atomic_sub), strlen(XSTR(map_atomic_sub)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_cond_map_atomic_sub(struct hyperclient* _cl,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_atomic_sub), strlen(XSTR(cond_map_atomic_sub)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_map_atomic_mul(struct hyperclient* _cl,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_atomic_mul), strlen(XSTR(map_atomic_mul)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_cond_map_atomic_mul(struct hyperclient* _cl,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_atomic_mul), strlen(XSTR(cond_map_atomic_mul)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_map_atomic_div(struct hyperclient* _cl,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_atomic_div), strlen(XSTR(map_atomic_div)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_cond_map_atomic_div(struct hyperclient* _cl,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_atomic_div), strlen(XSTR(cond_map_atomic_div)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_map_atomic_mod(struct hyperclient* _cl,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_atomic_mod), strlen(XSTR(map_atomic_mod)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_cond_map_atomic_mod(struct hyperclient* _cl,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_atomic_mod), strlen(XSTR(cond_map_atomic_mod)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_map_atomic_and(struct hyperclient* _cl,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_atomic_and), strlen(XSTR(map_atomic_and)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_cond_map_atomic_and(struct hyperclient* _cl,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_atomic_and), strlen(XSTR(cond_map_atomic_and)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_map_atomic_or(struct hyperclient* _cl,
                          const char* space,
                          const char* key, size_t key_sz,
                          const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                          enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_atomic_or), strlen(XSTR(map_atomic_or)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_cond_map_atomic_or(struct hyperclient* _cl,
                               const char* space,
                               const char* key, size_t key_sz,
                               const struct hyperclient_attribute_check* checks, size_t checks_sz,
                               const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                               enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_atomic_or), strlen(XSTR(cond_map_atomic_or)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_map_atomic_xor(struct hyperclient* _cl,
                           const char* space,
                           const char* key, size_t key_sz,
                           const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                           enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_atomic_xor), strlen(XSTR(map_atomic_xor)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_cond_map_atomic_xor(struct hyperclient* _cl,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_atomic_xor), strlen(XSTR(cond_map_atomic_xor)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_map_string_prepend(struct hyperclient* _cl,
                               const char* space,
                               const char* key, size_t key_sz,
                               const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                               enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_string_prepend), strlen(XSTR(map_string_prepend)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_cond_map_string_prepend(struct hyperclient* _cl,
                                    const char* space,
                                    const char* key, size_t key_sz,
                                    const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                    const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                    enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_string_prepend), strlen(XSTR(cond_map_string_prepend)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_map_string_append(struct hyperclient* _cl,
                              const char* space,
                              const char* key, size_t key_sz,
                              const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                              enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(map_string_append), strlen(XSTR(map_string_append)));
    return cl->perform_funcall(opinfo, space, key, key_sz, NULL, 0, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_cond_map_string_append(struct hyperclient* _cl,
                                   const char* space,
                                   const char* key, size_t key_sz,
                                   const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                   const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                   enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(cond_map_string_append), strlen(XSTR(cond_map_string_append)));
    return cl->perform_funcall(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, mapattrs, mapattrs_sz, status);
    );
}

int64_t
hyperclient_search(struct hyperclient* _cl,
                   const char* space,
                   const struct hyperclient_attribute_check* checks, size_t checks_sz,
                   enum hyperclient_returncode* status,
                   struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->search(space, checks, checks_sz, status, attrs, attrs_sz);
    );
}

int64_t
hyperclient_search_describe(struct hyperclient* _cl,
                            const char* space,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            enum hyperclient_returncode* status,
                            const char** str)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->search_describe(space, checks, checks_sz, status, str);
    );
}

int64_t
hyperclient_sorted_search(struct hyperclient* _cl,
                          const char* space,
                          const struct hyperclient_attribute_check* checks, size_t checks_sz,
                          const char* sort_by, uint64_t limit, int maximize,
                          enum hyperclient_returncode* status,
                          struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->sorted_search(space, checks, checks_sz, sort_by, limit, maximize, status, attrs, attrs_sz);
    );
}

int64_t
hyperclient_group_del(struct hyperclient* _cl,
                      const char* space,
                      const struct hyperclient_attribute_check* checks, size_t checks_sz,
                      enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->group_del(space, checks, checks_sz, status);
    );
}

int64_t
hyperclient_count(struct hyperclient* _cl,
                  const char* space,
                  const struct hyperclient_attribute_check* checks, size_t checks_sz,
                  enum hyperclient_returncode* status, uint64_t* result)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(_cl);
    return cl->count(space, checks, checks_sz, status, result);
    );
}

int64_t
hyperclient_loop(struct hyperclient* client, int timeout,
                 enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::client* cl = reinterpret_cast<hyperdex::client*>(client);
    return cl->loop(timeout, status);
    );
}

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus
