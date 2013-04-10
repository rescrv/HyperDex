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

// C
#include <errno.h>

// e
#include <po6/error.h>

// HyperDex
#include "client/hyperclient.h"
#include "client/wrap.h"

extern "C"
{

struct hyperclient*
hyperclient_create(const char* coordinator, uint16_t port)
{
    try
    {
        return new hyperclient(coordinator, port);
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
    delete client;
}

enum hyperclient_returncode
hyperclient_add_space(struct hyperclient* client, const char* description)
{
    try
    {
        return client->add_space(description);
    }
    catch (po6::error& e)
    {
        errno = e;
        return HYPERCLIENT_EXCEPTION;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return HYPERCLIENT_EXCEPTION;
    }
    catch (...)
    {
        return HYPERCLIENT_EXCEPTION;
    }
}

enum hyperclient_returncode
hyperclient_rm_space(struct hyperclient* client, const char* space)
{
    try
    {
        return client->rm_space(space);
    }
    catch (po6::error& e)
    {
        errno = e;
        return HYPERCLIENT_EXCEPTION;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return HYPERCLIENT_EXCEPTION;
    }
    catch (...)
    {
        return HYPERCLIENT_EXCEPTION;
    }
}

int64_t
hyperclient_get(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, hyperclient_returncode* status,
                struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    C_WRAP_EXCEPT(client->get(space, key, key_sz, status, attrs, attrs_sz));
}

int64_t
hyperclient_cond_put(struct hyperclient* client, const char* space,
                     const char* key, size_t key_sz,
                     const struct hyperclient_attribute_check* checks, size_t checks_sz,
                     const struct hyperclient_attribute* attrs,
                     size_t attrs_sz, hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(client->cond_put(space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status));
}

int64_t
hyperclient_del(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(client->del(space, key, key_sz, status));
}

int64_t
hyperclient_search(struct hyperclient* client, const char* space,
                   const struct hyperclient_attribute_check* checks, size_t checks_sz,
                   enum hyperclient_returncode* status,
                   struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    C_WRAP_EXCEPT(client->search(space, checks, checks_sz, status, attrs, attrs_sz));
}

int64_t
hyperclient_search_describe(struct hyperclient* client, const char* space,
                            const struct hyperclient_attribute_check* checks, size_t checks_sz,
                            enum hyperclient_returncode* status,
                            const char** description)
{
    C_WRAP_EXCEPT(client->search_describe(space, checks, checks_sz, status, description));
}

int64_t
hyperclient_sorted_search(struct hyperclient* client, const char* space,
                          const struct hyperclient_attribute_check* checks, size_t checks_sz,
                          const char* sort_by, uint64_t limit, int maximize,
                          enum hyperclient_returncode* status,
                          struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    C_WRAP_EXCEPT(client->sorted_search(space, checks, checks_sz, sort_by, limit, maximize != 0, status, attrs, attrs_sz));
}

int64_t
hyperclient_group_del(struct hyperclient* client, const char* space,
                      const struct hyperclient_attribute_check* checks, size_t checks_sz,
                      enum hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(client->group_del(space, checks, checks_sz, status));
}

int64_t
hyperclient_count(struct hyperclient* client, const char* space,
                  const struct hyperclient_attribute_check* checks, size_t checks_sz,
                  enum hyperclient_returncode* status, uint64_t* result)
{
    C_WRAP_EXCEPT(client->count(space, checks, checks_sz, status, result));
}

int64_t
hyperclient_loop(struct hyperclient* client, int timeout, hyperclient_returncode* status)
{
    C_WRAP_EXCEPT(client->loop(timeout, status));
}

enum hyperdatatype
hyperclient_attribute_type(struct hyperclient* client,
                           const char* space, const char* name,
                           enum hyperclient_returncode* status)
{
    try
    {
        return client->attribute_type(space, name, status);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_EXCEPTION;
        return HYPERDATATYPE_GARBAGE;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERCLIENT_NOMEM;
        return HYPERDATATYPE_GARBAGE;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return HYPERDATATYPE_GARBAGE;
    }
}

void
hyperclient_destroy_attrs(struct hyperclient_attribute* attrs, size_t /*attrs_sz*/)
{
    free(attrs);
}

} // extern "C"
