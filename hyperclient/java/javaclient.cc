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

#include <cstdlib>

HyperClient :: HyperClient(const char* coordinator, in_port_t port)
{
    _hyperclient = new hyperclient(coordinator,port);
}

HyperClient :: ~HyperClient() throw ()
{
    if (_hyperclient)
        delete _hyperclient;
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

void
HyperClient :: read_attr_value(hyperclient_attribute *ha,
                               char *value, size_t value_sz,
                               size_t pos)
{
    size_t available = ha->value_sz - pos;
    memcpy(value, ha->value+pos, value_sz<available?value_sz:available);
}

std::string
HyperClient :: read_map_attr_name(hyperclient_map_attribute *hma)
{
    std::string str = std::string(hma->attr);

    if (str.length() > INT_MAX)
    {
        return str.substr(0,INT_MAX);
    }

    return str;
}

std::string
HyperClient :: read_range_query_attr_name(hyperclient_range_query *rq)
{
    std::string str = std::string(rq->attr);

    if (str.length() > INT_MAX)
    {
        return str.substr(0,INT_MAX);
    }

    return str;
}

int64_t
HyperClient :: loop(hyperclient_returncode *rc)
{

    return _hyperclient->loop(-1, rc);
}

hyperclient_attribute*
HyperClient :: alloc_attrs(size_t attrs_sz)
{
    return (hyperclient_attribute *)calloc(attrs_sz,sizeof(hyperclient_attribute));
}

void
HyperClient :: destroy_attrs(hyperclient_attribute *attrs, size_t attrs_sz)
{
    for (size_t i=0; i<attrs_sz; i++)
    {
        if (attrs[i].attr) free((void*)(attrs[i].attr));
        if (attrs[i].value) free((void*)(attrs[i].value));
    }

    free(attrs);
}

hyperclient_map_attribute*
HyperClient :: alloc_map_attrs(size_t attrs_sz)
{
    return (hyperclient_map_attribute *)calloc(attrs_sz,
                                               sizeof(hyperclient_map_attribute));
}

void
HyperClient :: destroy_map_attrs(hyperclient_map_attribute *attrs, size_t attrs_sz)
{
    for (size_t i=0; i<attrs_sz; i++)
    {
        if (attrs[i].attr) free((void*)(attrs[i].attr));
        if (attrs[i].map_key) free((void*)(attrs[i].map_key));
        if (attrs[i].value) free((void*)(attrs[i].value));
    }

    free(attrs);
}

hyperclient_range_query*
HyperClient :: alloc_range_queries(size_t rqs_sz)
{
    return (hyperclient_range_query *)calloc(rqs_sz,
                                               sizeof(hyperclient_range_query));
}

void
HyperClient :: destroy_range_queries(hyperclient_range_query *rqs, size_t rqs_sz)
{
    for (size_t i=0; i<rqs_sz; i++)
    {
        if (rqs[i].attr) free((void*)(rqs[i].attr));
    }

    free(rqs);
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
    char *buf = NULL;
    // Note: Since hyperclient_attribute array was calloc'ed
    //       ha->value = NULL and ha->value_sz = 0 initially
    if ((buf = (char *)realloc((void *)(ha->value), ha->value_sz + value_sz))
                                                                    == NULL) return 0;
    memcpy(buf + ha->value_sz, value, value_sz);
    ha->value = buf;
    ha->value_sz += value_sz;
    return 1;
}

int
HyperClient :: write_map_attr_name(hyperclient_map_attribute *hma,
                               const char *attr, size_t attr_sz,
                               hyperdatatype type)
{
    char *buf;

    if ((buf = (char *)calloc(attr_sz+1,sizeof(char))) == NULL) return 0;
    memcpy(buf,attr,attr_sz);
    hma->attr = buf;
    hma->datatype = type;
    return 1;
}

int
HyperClient :: write_map_attr_map_key(hyperclient_map_attribute *hma,
                                      const char *map_key, size_t map_key_sz)
{
    char *buf = NULL;
    // Note: Since hyperclient_map_attribute array was calloc'ed
    //       hma->map_key = NULL and hma->map_key_sz = 0 initially
    if ((buf = (char *)realloc((void *)(hma->map_key), hma->map_key_sz + map_key_sz))
                                                                    == NULL) return 0;
    memcpy(buf + hma->map_key_sz, map_key, map_key_sz);
    hma->map_key = buf;
    hma->map_key_sz += map_key_sz;
    return 1;
}

int
HyperClient :: write_map_attr_value(hyperclient_map_attribute *hma,
                                const char *value, size_t value_sz)
{
    char *buf = NULL;
    // Note: Since hyperclient_map_attribute array was calloc'ed
    //       hma->value = NULL and hma->value_sz = 0 initially
    if ((buf = (char *)realloc((void *)(hma->value), hma->value_sz + value_sz))
                                                                    == NULL) return 0;
    memcpy(buf + hma->value_sz, value, value_sz);
    hma->value = buf;
    hma->value_sz += value_sz;
    return 1;
}

int
HyperClient :: write_range_query(hyperclient_range_query *rq,
                               const char *attr, size_t attr_sz,
                               int64_t lower,
                               int64_t upper)
{
    char *buf;

    if ((buf = (char *)calloc(attr_sz+1,sizeof(char))) == NULL) return 0;
    memcpy(buf,attr,attr_sz);
    rq->attr = buf;
    rq->lower = lower;
    rq->upper = upper;
    return 1;
}

hyperclient_attribute *
HyperClient :: get_attr(hyperclient_attribute *ha, size_t i)
{
    return ha + i;
}

hyperclient_map_attribute *
HyperClient :: get_map_attr(hyperclient_map_attribute *hma, size_t i)
{
    return hma + i;
}

hyperclient_range_query *
HyperClient :: get_range_query(hyperclient_range_query *rqs, size_t i)
{
    return rqs + i;
}
