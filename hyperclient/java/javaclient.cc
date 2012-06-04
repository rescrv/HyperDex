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

// STL
#include <map>
#include <string>

// e
#include <e/guard.h>

// HyperType's and HyperClient
#include "hyperclient/hyperclient.h"
#include "hyperclient/java/javaclient.h"

#include <iostream>
#include <sstream>

HyperClient :: HyperClient(const char* coordinator, in_port_t port)
    : m_client(coordinator, port)
{
}

HyperClient :: ~HyperClient() throw ()
{
}

std::string
HyperClient :: read_attr_name(hyperclient_attribute *ha)
{
    return std::string(ha->attr);
}

int
HyperClient :: read(const char *memb, int memb_sz, char *ret, int ret_sz)
{
    if ( ret_sz == 0 )
    {
        return (int)(memb_sz); // XXX throw if memb_sz > MAX_INT
    }
    else
    {
        int smallest_sz = (int)(ret_sz<memb_sz?ret_sz:memb_sz); // XXX
        memcpy(ret,memb,smallest_sz);
        return smallest_sz;
    }
}

int
HyperClient :: read_value(hyperclient_attribute *ha, char *value, int value_sz)
{
    read(ha->value,ha->value_sz,value,value_sz);
}

void
HyperClient :: set_attribute(hyperclient_attribute *ha, char *attr,
                                   char *value, int value_sz,
                                   hyperdatatype type)
{
}

hyperclient_attribute *
HyperClient :: get_attribute(hyperclient_attribute *ha, int i)
{
    return ha + i;
}

hyperclient_returncode
HyperClient :: get(const std::string& space,
                   const std::string& key,
                   hyperclient_attribute **attrs, int *attrs_sz)
{
    int64_t id;
    hyperclient_returncode stat1 = HYPERCLIENT_A;
    hyperclient_returncode stat2 = HYPERCLIENT_B;
    *attrs = NULL;
    *attrs_sz = 0;

    id = m_client.get(space.c_str(),
                      key.data(),
                      key.size(),
                      &stat1,
                      attrs,
                      (size_t *)attrs_sz);

    if (id < 0)
    {
        assert(static_cast<unsigned>(stat1) >= 8448);
        assert(static_cast<unsigned>(stat1) < 8576);
        return stat1;
    }

    int64_t lid = m_client.loop(-1, &stat2);

    if (lid < 0)
    {
        assert(static_cast<unsigned>(stat2) >= 8448);
        assert(static_cast<unsigned>(stat2) < 8576);
        return stat2;
    }

    assert(id == lid);
    //e::guard g = e::makeguard(free, *attrs); g.use_variable(); XXX do this in java

    assert(static_cast<unsigned>(stat1) >= 8448);
    assert(static_cast<unsigned>(stat1) < 8576);
    return stat1;
}

hyperclient_returncode
HyperClient :: put(const std::string& space,
                       const std::string& key,
                       const hyperclient_attribute *attrs, int attrs_sz)

{
    return HYPERCLIENT_SUCCESS;
}
