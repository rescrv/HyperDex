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

#include <limits.h>

#include <iostream>
#include <sstream>

#include <cstdlib>

HyperClient :: HyperClient(const char* coordinator, in_port_t port)
{
    m_client = hyperclient_create(coordinator,port);
}

HyperClient :: ~HyperClient() throw ()
{
    if (m_client)
        hyperclient_destroy(m_client);
}

std::string
HyperClient :: read_attr_name(hyperclient_attribute *ha)
{
    std::string str = std::string(ha->attr);

    if (str.length() > INT_MAX)
    {
        return str.substr(0,INT_MAX);
    }

    return str;
}

size_t
HyperClient :: read(const char *memb, size_t memb_sz, char *ret, size_t ret_sz)
{
    if ( ret_sz == 0 )
    {
        return memb_sz;
    }
    else
    {
        size_t smallest_sz = ret_sz<memb_sz?ret_sz:memb_sz;
        memcpy(ret,memb,smallest_sz);
        return smallest_sz;
    }
}

size_t
HyperClient :: read_attr_value(hyperclient_attribute *ha, char *value, size_t value_sz)
{
    return read(ha->value,ha->value_sz,value,value_sz);
}

int64_t
HyperClient :: loop(int *i_rc)
{
    hyperclient_returncode rc;
    int64_t ret;

    ret = hyperclient_loop(m_client, -1, &rc);
    *i_rc = (int)rc;
    std::cout << "c++: the int is: " << *i_rc << std::endl;
    std::cout << "c++: the enum is: " << rc << std::endl;
    return ret;
}

hyperclient_attribute*
HyperClient :: alloc_attrs(size_t attrs_sz)
{
    return (hyperclient_attribute *)calloc(attrs_sz,sizeof(hyperclient_attribute));
}

void
HyperClient :: destroy_attrs(hyperclient_attribute *attrs, size_t attrs_sz)
{
    std::cout << "About to destroy_attrs" << std::endl;
    for (int i=0; i<attrs_sz; i++)
    {
        if (attrs[i].attr) free((void*)(attrs[i].attr));
        if (attrs[i].value) destroy_attr_value(&(attrs[i]));
    }

    std::cout << "Freed members" << std::endl;
    free(attrs);
    std::cout << "Freed attrs" << std::endl;
}

void
HyperClient :: destroy_attr_value(hyperclient_attribute *ha)
{
    if (ha->datatype == HYPERDATATYPE_STRING || ha->datatype == HYPERDATATYPE_INT64)
        free((void*)(ha->value));    
}

int
HyperClient :: write_attr_name(hyperclient_attribute *ha,
                               const char *attr, size_t attr_sz,
                               hyperdatatype type)
{
    char *buf;

    if ((buf = (char *)calloc(attr_sz+1,sizeof(char))) == NULL) return 0;
    memcpy(buf,attr,attr_sz);
    ha->attr = buf;
    ha->datatype = type;
    return 1;
}

int
HyperClient :: write_attr_value(hyperclient_attribute *ha,
                                const char *value, size_t value_sz)
{
    char *buf;
    if ((buf = (char *)calloc(value_sz,sizeof(char))) == NULL) return 0;
    memcpy(buf,value,value_sz);
    ha->value = buf;
    ha->value_sz = value_sz;
    return 1;
}

hyperclient_attribute *
HyperClient :: get_attr(hyperclient_attribute *ha, size_t i)
{
    return ha + i;
}

int64_t
HyperClient :: get(const std::string& space,
                   const std::string& key,
                   int *i_rc,
                   hyperclient_attribute **attrs, size_t *attrs_sz)
{
    hyperclient_returncode rc;

    int64_t id =  hyperclient_get(m_client,
                                  space.c_str(),
                                  key.data(),
                                  key.size(),
                                  &rc,
                                  attrs,
                                  attrs_sz);
    *i_rc = (int)rc;
    std::cout << "c++: the id is: " << id << std::endl;
    std::cout << "c++: the rc is: " << (int)rc << std::endl;
    std::cout << "c++: the space is: " << space.c_str() << std::endl;
    std::cout << "c++: the key is: " << key << std::endl;
    return id;
}

int64_t
HyperClient :: put(const std::string& space,
                       const std::string& key,
                       const hyperclient_attribute *attrs, size_t attrs_sz, int *i_rc)

{
    hyperclient_returncode rc;

    std::cout << m_client << std::endl
    << space << std::endl
    << key << std::endl
    << attrs[0].attr << std::endl
    << attrs_sz << std::endl
    << &rc << std::endl;

    int64_t id = hyperclient_put(m_client,
                                 space.c_str(),
                                 key.data(),
                                 key.size(),
                                 attrs,
                                 attrs_sz,
                                 &rc);
    

    *i_rc = (int)rc;
    return id;
}
