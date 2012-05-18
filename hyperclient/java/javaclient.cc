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

// Attributes and HyperClient
#include "hyperclient/hyperclient.h"
#include "hyperclient/java/javaclient.h"

#include <iostream>
#include <sstream>

Attribute :: Attribute()
{
    std::cout << "Attribute Constructed" << std::endl;
}

Attribute :: ~Attribute() throw ()
{
    std::cout << "Attribute Destructed" << std::endl;
}

hyperdatatype 
Attribute :: type() const
{
    return HYPERDATATYPE_GARBAGE;
}

void 
Attribute :: serialize(std::string& value) const
{
    std::cout << "Attribute Serialized" << std::endl;
}

Attribute* 
Attribute :: deserialize(const hyperclient_attribute& attr)
{
    if ( attr.datatype == HYPERDATATYPE_STRING )
    {
        return new StringAttribute(attr.value, attr.value_sz);
    }
    else if ( attr.datatype == HYPERDATATYPE_INT64 )
    {
        return new IntegerAttribute(le64toh(*reinterpret_cast<const int64_t*>(attr.value)));
    }

    std::string ex_str;
    ex_str += "Type '";
    ex_str += attr.datatype;
    ex_str += "' not supported.";

    throw ex_str;
}

int
Attribute :: data(char *bytes, int len)
{
    return 0;
}

std::string
Attribute :: toString()
{
    return "";
}

StringAttribute :: StringAttribute(const std::string& attr_value)
                                   : m_attr_value(attr_value)
{
    std::cout << "StringAttribute Constructed" << std::endl;
}

StringAttribute :: StringAttribute(const char *s, size_t n)
                                   : m_attr_value(s,n)
{
    std::cout << "StringAttribute Constructed" << std::endl;
}

StringAttribute :: ~StringAttribute() throw ()
{
    std::cout << "StringAttribute Destructed" << std::endl;
}

hyperdatatype 
StringAttribute :: type() const
{
    return HYPERDATATYPE_STRING;
}

void
StringAttribute :: serialize(std::string& value) const
{
    std::cout << "StringAttribute Serialized" << std::endl;
    value.assign(std::string(m_attr_value));
}

int
StringAttribute :: data(char *bytes, int len)
{
    size_t size = m_attr_value.size();

    if ( len == 0 ) return size;
    
    int ret_len = len<size?len:size;
    m_attr_value.copy(bytes,ret_len);
    return ret_len;
}

std::string
StringAttribute :: toString()
{
    return m_attr_value;
}

IntegerAttribute :: IntegerAttribute(int64_t attr_value)
                                     : m_attr_value(attr_value)
{
   std::cout << "IntegerAttribute Constructed" << std::endl;
}

IntegerAttribute :: ~IntegerAttribute() throw ()
{
   std::cout << "IntegerAttribute Destructed" << std::endl;
}

hyperdatatype
IntegerAttribute :: type() const
{
    return HYPERDATATYPE_INT64;
}

void
IntegerAttribute :: serialize(std::string& value) const
{
    std::cout << "IntegerAttribute Serialized" << std::endl;
    value.assign(
        reinterpret_cast<const char *>(&htole64(m_attr_value)),
        sizeof(int64_t));
}

int
IntegerAttribute :: data(char *bytes, int len)
{
    size_t size = sizeof(int64_t);

    if ( len == 0 ) return size;
    
    int ret_len = len<size?len:size;
    int64_t intbe64 = htobe64(m_attr_value);
    std::string buf = std::string((const char *)&intbe64,ret_len);
    buf.copy(bytes,ret_len);
    return ret_len;
}

std::string
IntegerAttribute :: toString()
{
    std::stringstream ss;
    ss << m_attr_value;
    return ss.str();
}

HyperClient :: HyperClient(const char* coordinator, in_port_t port)
    : m_client(coordinator, port)
{
}

HyperClient :: ~HyperClient() throw ()
{
}

hyperclient_returncode
HyperClient :: get(const std::string& space,
                   const std::string& key,
                   std::map<std::string, Attribute*>* values)
{
    int64_t id;
    hyperclient_returncode stat1 = HYPERCLIENT_A;
    hyperclient_returncode stat2 = HYPERCLIENT_B;
    hyperclient_attribute* attrs = NULL;
    size_t attrs_sz = 0;

    id = m_client.get(space.c_str(),
                      key.data(),
                      key.size(),
                      &stat1,
                      &attrs,
                      &attrs_sz);

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
    e::guard g = e::makeguard(free, attrs); g.use_variable();

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        values->insert(std::make_pair(std::string(attrs[i].attr),
                                      Attribute::deserialize(attrs[i])));
    }

    assert(static_cast<unsigned>(stat1) >= 8448);
    assert(static_cast<unsigned>(stat1) < 8576);
    return stat1;
}

hyperclient_returncode
HyperClient :: put(const std::string& space,
                   const std::string& key,
                   const std::map<std::string, Attribute*>& attributes)
{
    int64_t id;
    hyperclient_returncode stat1 = HYPERCLIENT_A;
    hyperclient_returncode stat2 = HYPERCLIENT_B;
    std::vector<hyperclient_attribute> attrs;
    std::vector<std::string> values;
    values.reserve(attributes.size());

    for (std::map<std::string, Attribute*>::const_iterator ci = attributes.begin();
            ci != attributes.end(); ++ci)
    {
        hyperclient_attribute at;
        at.attr = ci->first.c_str();
        std::string value;
        ci->second->serialize(value);
        values.push_back(value);
        at.value = values.back().data();
        at.value_sz = value.size();
        at.datatype = ci->second->type();
        attrs.push_back(at);
        std::cout << "value type = " << ci->second->type() << std::endl;
        std::cout << "value = " << value << std::endl;
        std::cout << "value size = " << value.size() << std::endl;
    }

    id = m_client.put(space.c_str(),
                      key.data(),
                      key.size(),
                      &attrs.front(),
                      attrs.size(),
                      &stat1);

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

    assert(lid == id);
    assert(static_cast<unsigned>(stat1) >= 8448);
    assert(static_cast<unsigned>(stat1) < 8576);
    return stat1;
}

hyperclient_returncode
HyperClient :: del(const std::string& space,
                   const std::string& key)
{
    int64_t id;
    hyperclient_returncode stat1 = HYPERCLIENT_A;
    hyperclient_returncode stat2 = HYPERCLIENT_B;

    id = m_client.del(space.c_str(), key.data(), key.size(), &stat1);

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

    assert(lid == id);
    assert(static_cast<unsigned>(stat1) >= 8448);
    assert(static_cast<unsigned>(stat1) < 8576);
    return stat1;
}

hyperclient_returncode
HyperClient :: range_search(const std::string& space,
                            const std::string& attr,
                            int64_t lower,
                            int64_t upper,
                            std::vector<std::map<std::string, Attribute*> >* results)
{
    hyperclient_range_query rn;
    rn.attr = attr.c_str();
    rn.lower = lower;
    rn.upper = upper;

    int64_t id;
    hyperclient_returncode status = HYPERCLIENT_A;
    hyperclient_attribute* attrs = NULL;
    size_t attrs_sz = 0;

    id = m_client.search(space.c_str(), NULL, 0, &rn, 1, &status, &attrs, &attrs_sz);

    if (id < 0)
    {
        return status;
    }

    int64_t lid;
    hyperclient_returncode lstatus = HYPERCLIENT_B;

    //size_t pre_insert_size = 0; // Keep track of the number of "n-tuples" returned

    while ((lid = m_client.loop(-1, &lstatus)) == id)
    {
        if (lstatus == HYPERCLIENT_SEARCHDONE)
        {
            return HYPERCLIENT_SUCCESS;
        }

        if (status != HYPERCLIENT_SUCCESS)
        {
            break;
        }

        /*
        for (size_t i = 0; i < attrs_sz; ++i)
        {
            if ( attrs[i].datatype == HYPERDATATYPE_STRING )
            {
                if (sresults->empty() || sresults->size() == pre_insert_size)
                {
                    sresults->push_back(std::map<std::string, std::string>());
                }

                sresults->back().insert(std::make_pair(attrs[i].attr, std::string(attrs[i].value, attrs[i].value_sz)));
            }
            else if ( attrs[i].datatype == HYPERDATATYPE_INT64 )
            {
                if (nresults->empty() || nresults->size() == pre_insert_size)
                {
                    nresults->push_back(std::map<std::string, int64_t>());
                }

                nresults->back().insert(std::make_pair(attrs[i].attr, le64toh(*((int64_t *)(attrs[i].value)))));
            }
        }

        pre_insert_size++;
        */

        results->push_back(std::map<std::string, Attribute*>());

        for (size_t i = 0; i < attrs_sz; ++i)
        {
            results->back().insert(std::make_pair(attrs[i].attr,
                                                  Attribute::deserialize(attrs[i])));
        }

        if (attrs)
        {
            hyperclient_destroy_attrs(attrs, attrs_sz);
        }

        attrs = NULL;
        attrs_sz = 0;
    }

    if (lid < 0)
    {
        assert(static_cast<unsigned>(lstatus) >= 8448);
        assert(static_cast<unsigned>(lstatus) < 8576);
        return lstatus;
    }

    assert(lid == id);
    assert(static_cast<unsigned>(status) >= 8448);
    assert(static_cast<unsigned>(status) < 8576);
    return status;
}
