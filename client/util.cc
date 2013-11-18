// Copyright (c) 2011-2012, Cornell University
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

// e
#include <e/endian.h>
#include <e/guard.h>

// HyperDex
#include "common/schema.h"
#include "client/util.h"

#define UTIL_ERROR(CODE) \
    *op_status = HYPERDEX_CLIENT_ ## CODE; \
    op_error->set_loc(__FILE__, __LINE__); \
    op_error->set_msg()

bool
hyperdex :: value_to_attributes(const configuration& config,
                                const region_id& rid,
                                const uint8_t* key,
                                size_t key_sz,
                                const std::vector<e::slice>& value,
                                hyperdex_client_returncode* op_status,
                                e::error* op_error,
                                const hyperdex_client_attribute** attrs,
                                size_t* attrs_sz)
{
    const schema* sc = config.get_schema(rid);

    if (value.size() + 1 != sc->attrs_sz)
    {
        UTIL_ERROR(SERVERERROR) << "received object with " << value.size()
                                << " attributes instead of "
                                << sc->attrs_sz - 1 << " attributes";
        return false;
    }

    size_t sz = sizeof(hyperdex_client_attribute) * sc->attrs_sz + key_sz
              + strlen(sc->attrs[0].name) + 1;

    for (size_t i = 0; i < value.size(); ++i)
    {
        sz += strlen(sc->attrs[i + 1].name) + 1 + value[i].size();
    }

    std::vector<hyperdex_client_attribute> ha;
    ha.reserve(sc->attrs_sz);
    char* ret = static_cast<char*>(malloc(sz));

    if (!ret)
    {
        UTIL_ERROR(NOMEM) << "out of memory";
        return false;
    }

    e::guard g = e::makeguard(free, ret);
    char* data = ret + sizeof(hyperdex_client_attribute) * value.size();

    if (key)
    {
        data += sizeof(hyperdex_client_attribute);
        ha.push_back(hyperdex_client_attribute());
        size_t attr_sz = strlen(sc->attrs[0].name) + 1;
        ha.back().attr = data;
        memmove(data, sc->attrs[0].name, attr_sz);
        data += attr_sz;
        ha.back().value = data;
        memmove(data, key, key_sz);
        data += key_sz;
        ha.back().value_sz = key_sz;
        ha.back().datatype = sc->attrs[0].type;
    }

    for (size_t i = 0; i < value.size(); ++i)
    {
        ha.push_back(hyperdex_client_attribute());
        size_t attr_sz = strlen(sc->attrs[i + 1].name) + 1;
        ha.back().attr = data;
        memmove(data, sc->attrs[i + 1].name, attr_sz);
        data += attr_sz;
        ha.back().value = data;
        memmove(data, value[i].data(), value[i].size());
        data += value[i].size();
        ha.back().value_sz = value[i].size();
        ha.back().datatype = sc->attrs[i + 1].type;
    }

    memmove(ret, &ha.front(), sizeof(hyperdex_client_attribute) * ha.size());
    *op_status = HYPERDEX_CLIENT_SUCCESS;
    *op_error = e::error();
    *attrs = reinterpret_cast<hyperdex_client_attribute*>(ret);
    *attrs_sz = ha.size();
    g.dismiss();
    return true;
}
