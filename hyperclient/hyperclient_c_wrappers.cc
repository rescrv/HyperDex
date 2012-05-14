// Copyright (c) 2012, Cornell University
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

// HyperClient
#include "hyperclient/hyperclient.h"

extern "C"
{

struct hyperclient*
hyperclient_create(const char* coordinator, in_port_t port)
{
    try
    {
        std::auto_ptr<hyperclient> ret(new hyperclient(coordinator, port));
        return ret.release();
    }
    catch (po6::error& e)
    {
        errno = e;
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
    delete client;
}

int64_t
hyperclient_get(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, hyperclient_returncode* status,
                struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    try
    {
        return client->get(space, key, key_sz, status, attrs, attrs_sz);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

int64_t
hyperclient_put(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, const struct hyperclient_attribute* attrs,
                size_t attrs_sz, hyperclient_returncode* status)
{
    try
    {
        return client->put(space, key, key_sz, attrs, attrs_sz, status);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

int64_t
hyperclient_condput(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, const struct hyperclient_attribute* condattrs,
                size_t condattrs_sz, const struct hyperclient_attribute* attrs,
                size_t attrs_sz, hyperclient_returncode* status)
{
    try
    {
        return client->condput(space, key, key_sz, condattrs, condattrs_sz, attrs, attrs_sz, status);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

int64_t
hyperclient_del(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, hyperclient_returncode* status)
{
    try
    {
        return client->del(space, key, key_sz, status);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

#define HYPERCLIENT_CDEF_CUSTOM_CALL(OPNAME, ATTRPREFIX) \
    int64_t \
    hyperclient_ ## OPNAME(struct hyperclient* client, const char* space, const char* key, \
                  size_t key_sz, const struct hyperclient ## ATTRPREFIX ## attribute* attrs, \
                  size_t attrs_sz, hyperclient_returncode* status) \
    { \
        try \
        { \
            return client->OPNAME(space, key, key_sz, attrs, attrs_sz, status); \
        } \
        catch (po6::error& e) \
        { \
            errno = e; \
            *status = HYPERCLIENT_EXCEPTION; \
            return -1; \
        } \
        catch (...) \
        { \
            *status = HYPERCLIENT_EXCEPTION; \
            return -1; \
        } \
    }

#define HYPERCLIENT_CDEF_STANDARD_CALL(OPNAME) \
    HYPERCLIENT_CDEF_CUSTOM_CALL(OPNAME, _)
#define HYPERCLIENT_CDEF_MAP_CALL(OPNAME) \
    HYPERCLIENT_CDEF_CUSTOM_CALL(OPNAME, _map_)

HYPERCLIENT_CDEF_STANDARD_CALL(atomic_add)
HYPERCLIENT_CDEF_STANDARD_CALL(atomic_sub)
HYPERCLIENT_CDEF_STANDARD_CALL(atomic_mul)
HYPERCLIENT_CDEF_STANDARD_CALL(atomic_div)
HYPERCLIENT_CDEF_STANDARD_CALL(atomic_mod)
HYPERCLIENT_CDEF_STANDARD_CALL(atomic_and)
HYPERCLIENT_CDEF_STANDARD_CALL(atomic_or)
HYPERCLIENT_CDEF_STANDARD_CALL(atomic_xor)
HYPERCLIENT_CDEF_STANDARD_CALL(string_prepend)
HYPERCLIENT_CDEF_STANDARD_CALL(string_append)
HYPERCLIENT_CDEF_STANDARD_CALL(list_lpush)
HYPERCLIENT_CDEF_STANDARD_CALL(list_rpush)
HYPERCLIENT_CDEF_STANDARD_CALL(set_add)
HYPERCLIENT_CDEF_STANDARD_CALL(set_remove)
HYPERCLIENT_CDEF_STANDARD_CALL(set_intersect)
HYPERCLIENT_CDEF_STANDARD_CALL(set_union)
HYPERCLIENT_CDEF_MAP_CALL(map_add)
HYPERCLIENT_CDEF_MAP_CALL(map_remove)
HYPERCLIENT_CDEF_MAP_CALL(map_atomic_add)
HYPERCLIENT_CDEF_MAP_CALL(map_atomic_sub)
HYPERCLIENT_CDEF_MAP_CALL(map_atomic_mul)
HYPERCLIENT_CDEF_MAP_CALL(map_atomic_div)
HYPERCLIENT_CDEF_MAP_CALL(map_atomic_mod)
HYPERCLIENT_CDEF_MAP_CALL(map_atomic_and)
HYPERCLIENT_CDEF_MAP_CALL(map_atomic_or)
HYPERCLIENT_CDEF_MAP_CALL(map_atomic_xor)
HYPERCLIENT_CDEF_MAP_CALL(map_string_prepend)
HYPERCLIENT_CDEF_MAP_CALL(map_string_append)

int64_t
hyperclient_search(struct hyperclient* client, const char* space,
                   const struct hyperclient_attribute* eq, size_t eq_sz,
                   const struct hyperclient_range_query* rn, size_t rn_sz,
                   enum hyperclient_returncode* status,
                   struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    try
    {
        return client->search(space, eq, eq_sz, rn, rn_sz, status, attrs, attrs_sz);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

int64_t
hyperclient_loop(struct hyperclient* client, int timeout, hyperclient_returncode* status)
{
    try
    {
        return client->loop(timeout, status);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

void
hyperclient_destroy_attrs(struct hyperclient_attribute* attrs, size_t /*attrs_sz*/)
{
    free(attrs);
}

} // extern "C"
