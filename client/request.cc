
// Copyright (c) 2014, Cornell University
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

#include <e/strescape.h>

#include "client/request.h"
#include "client/util.h"
#include "client/constants.h"

#include "common/attribute_check.h"
#include "common/datatype_info.h"
#include "common/funcall.h"
#include "common/macros.h"
#include "common/serialization.h"

#define ERROR(CODE) \
    status = HYPERDEX_CLIENT_ ## CODE; \
    cl.set_last_error(__FILE__, __LINE__)

BEGIN_HYPERDEX_NAMESPACE

request::request(client& cl_, const coordinator_link& coord_, const std::string& space_)
    : cl(cl_), coord(coord_), space(space_), allocate()
{
}

size_t
request :: prepare_checks(const schema& sc,
                         const hyperdex_client_attribute_check* chks, size_t chks_sz,
                         hyperdex_client_returncode& status,
                         std::vector<attribute_check>* checks)
{
    checks->reserve(checks->size() + chks_sz);

    for (size_t i = 0; i < chks_sz; ++i)
    {
        std::string _attr;
        const char* attr;
        const char* dot;

        if ((dot = strchr(chks[i].attr, '.')))
        {
            _attr.assign(chks[i].attr, dot);
            ++dot;
        }
        else
        {
            _attr.assign(chks[i].attr, strlen(chks[i].attr));
            dot = NULL;
        }

        attr = _attr.c_str();
        uint16_t attrnum = sc.lookup_attr(attr);

        if (attrnum >= sc.attrs_sz)
        {
            ERROR(UNKNOWNATTR) << "\"" << e::strescape(attr)
                               << "\" is not an attribute of space \""
                               << e::strescape(space) << "\"";
            return i;
        }

        hyperdatatype datatype = chks[i].datatype;

        if (datatype == CONTAINER_TYPE(datatype) &&
            CONTAINER_TYPE(datatype) == CONTAINER_TYPE(sc.attrs[attrnum].type) &&
            (chks[i].value_sz == 0 || datatype == HYPERDATATYPE_TIMESTAMP_GENERIC))
        {
            datatype = sc.attrs[attrnum].type;
        }

        attribute_check c;
        c.attr = attrnum;
        c.value = e::slice(chks[i].value, chks[i].value_sz);
        c.datatype = datatype;
        c.predicate = chks[i].predicate;

        if (dot)
        {
            size_t dot_sz = strlen(dot) + 1;
            allocate.push_back(std::string());
            std::string& s(allocate.back());
            s.append(dot, dot_sz);
            s.append(chks[i].value, chks[i].value_sz);
            c.value = e::slice(s);
        }

        if (!validate_attribute_check(sc.attrs[attrnum].type, c))
        {
            ERROR(WRONGTYPE) << "invalid predicate on \""
                             << e::strescape(attr) << "\"";
            return i;
        }

        checks->push_back(c);
    }

    return chks_sz;
}

const schema& request::get_schema() const throw(std::runtime_error)
{
    const schema *sc = coord.config()->get_schema(space.c_str());

    if (sc == NULL)
    {
        throw std::runtime_error("space \"" + e::strescape(space.c_str()) + "\" does not exist");
    }

    return *sc;
}

END_HYPERDEX_NAMESPACE
