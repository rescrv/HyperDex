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

// e
#include <e/strescape.h>

#include "client/atomic_request.h"
#include "client/util.h"
#include "client/constants.h"

#include "common/attribute_check.h"
#include "common/datatype_info.h"
#include "common/funcall.h"
#include "common/macros.h"
#include "common/serialization.h"
#include "common/auth_wallet.h"

#include <document/document.h>
using namespace document;

#define ERROR(CODE) \
    status = HYPERDEX_CLIENT_ ## CODE; \
    req.cl.m_last_error.set_loc(__FILE__, __LINE__); \
    req.cl.m_last_error.set_msg()

BEGIN_HYPERDEX_NAMESPACE

hyperdex_client_returncode atomic_request::validate_key(const e::slice& key) const
{
    hyperdex_client_returncode status = HYPERDEX_CLIENT_SUCCESS;

    try
    {
        const schema& sc = req.get_schema();

        datatype_info* di = datatype_info::lookup(sc.attrs[0].type);
        assert(di);

        if (!di->validate(key))
        {
            // This will update the status on error
            ERROR(WRONGTYPE) << "key must be type " << sc.attrs[0].type;
        }

        return status;
    }
    catch(std::exception& e)
    {
        ERROR(UNKNOWNSPACE) << e.what();
        return HYPERDEX_CLIENT_UNKNOWNSPACE;
    }
}

int atomic_request::prepare(const hyperdex_client_keyop_info& opinfo,
                           const hyperdex_client_attribute_check* chks, size_t chks_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                           hyperdex_client_returncode& status)
{
    try
    {
        const schema& sc = req.get_schema();
        size_t idx = 0;

        // Prepare the checks
        idx = req.prepare_checks(sc, chks, chks_sz, status, &checks);

        if (idx < chks_sz)
        {
            return -2 - idx;
        }

        // Prepare the attrs
        idx = prepare_funcs(sc, opinfo, attrs, attrs_sz, status);

        if (idx < attrs_sz)
        {
            return -2 - chks_sz - idx;
        }

        // Prepare the mapattrs
        idx = prepare_funcs(sc, opinfo, mapattrs, mapattrs_sz, status);

        if (idx < mapattrs_sz)
        {
            return -2 - chks_sz - attrs_sz - idx;
        }

        return 0;
    }
    catch(std::exception& e)
    {
        ERROR(UNKNOWNSPACE) << e.what();
        return HYPERDEX_CLIENT_UNKNOWNSPACE;
    }
}

e::buffer* atomic_request::create_message(const hyperdex_client_keyop_info& opinfo, const e::slice& key)
{
    std::stable_sort(checks.begin(), checks.end());
    std::stable_sort(funcs.begin(), funcs.end());
    size_t sz = HYPERDEX_CLIENT_HEADER_SIZE_REQ
              + sizeof(uint8_t)
              + pack_size(key)
              + pack_size(checks)
              + pack_size(funcs);

    auth_wallet aw = req.cl.get_macaroons();

    if (aw.size())
    {
        sz += pack_size(aw);
    }

    e::buffer *msg = e::buffer::create(sz);
    uint8_t flags = (opinfo.fail_if_not_found ? 1 : 0)
                  | (opinfo.fail_if_found ? 2 : 0)
                  | (opinfo.erase ? 0 : 128)
                  | (!aw.empty() ? 64 : 0);

    e::buffer::packer pa = msg->pack_at(HYPERDEX_CLIENT_HEADER_SIZE_REQ);
    pa << key << flags << checks << funcs;

    if (!aw.empty())
    {
        pa = pa << aw;
    }

    return msg;
}


size_t
atomic_request :: prepare_funcs(const schema& sc,
                        const hyperdex_client_keyop_info& opinfo,
                        const hyperdex_client_attribute* attrs, size_t attrs_sz,
                        hyperdex_client_returncode& status)
{
    funcs.reserve(funcs.size() + attrs_sz);

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        // In case we need to allocate memory for a second string
        std::string attr_buf;

        uint16_t attrnum = 0;
        const char* attr = attrs[i].attr;
        const char* path = strstr(attrs[i].attr, ".");

        // This is a path to a document value
        if (path != NULL)
        {
            // Remove the dot
            path = path+1;

            // Set attribute name to only the first part
            std::string orig(attrs[i].attr);
            size_t pos = orig.find('.');

            attr_buf = orig.substr(0, pos);
            attr = attr_buf.c_str();
        }

        attrnum = sc.lookup_attr(attr);

        if (attrnum == sc.attrs_sz)
        {
            ERROR(UNKNOWNATTR) << "\"" << e::strescape(attr)
                               << "\" is not an attribute of space \""
                               << e::strescape(req.space) << "\"";
            return i;
        }

        if (attrnum == 0)
        {
            ERROR(DONTUSEKEY) << "attribute \""
                              << e::strescape(attr)
                              << "\" is the key and cannot be changed";
            return i;
        }

        hyperdatatype datatype = attrs[i].datatype;

        if (datatype == CONTAINER_TYPE(datatype) &&
            CONTAINER_TYPE(datatype) == CONTAINER_TYPE(sc.attrs[attrnum].type) &&
            (attrs[i].value_sz == 0 || datatype == HYPERDATATYPE_TIMESTAMP_GENERIC))
        {
            datatype = sc.attrs[attrnum].type;
        }

        if (sc.attrs[attrnum].type == HYPERDATATYPE_MACAROON_SECRET)
        {
            datatype = HYPERDATATYPE_MACAROON_SECRET;
        }

        funcall o;
        o.attr = attrnum;
        o.name = opinfo.fname;
        o.arg1_datatype = datatype;

        if (datatype == HYPERDATATYPE_DOCUMENT)
        {
            std::auto_ptr<document::document> doc( create_document_from_json(document_type_bson, reinterpret_cast<const uint8_t*>(attrs[i].value), attrs[i].value_sz));

            if(!doc->is_valid())
            {
                ERROR(WRONGTYPE) << "invalid document for attribute \"" << e::strescape(attr) << "\": " << doc->get_last_error();
                return i;
            }

            size_t len = doc->size();
            const char* data = reinterpret_cast<const char*>(doc->data());

            req.allocate.push_back(std::string());
            std::string& s(req.allocate.back());
            s.append(data, len);

            o.arg1 = e::slice(s.data(), len);
        }
        else
        {
            o.arg1 = e::slice(attrs[i].value, attrs[i].value_sz);
        }

        if(path != NULL)
        {
            o.arg2 = e::slice(path, strlen(path)+1);
            o.arg2_datatype = HYPERDATATYPE_STRING;
        }

        datatype_info* type = datatype_info::lookup(sc.attrs[attrnum].type);

        if (!type->check_args(o))
        {
            ERROR(WRONGTYPE) << "invalid attribute \""
                             << e::strescape(attr)
                             << "\": attribute has the wrong type";
            return i;
        }

        funcs.push_back(o);
    }

    return attrs_sz;
}

size_t
atomic_request :: prepare_funcs(const schema& sc,
                        const hyperdex_client_keyop_info& opinfo,
                        const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                        hyperdex_client_returncode& status)
{
    funcs.reserve(funcs.size() + mapattrs_sz);

    for (size_t i = 0; i < mapattrs_sz; ++i)
    {
        uint16_t attrnum = sc.lookup_attr(mapattrs[i].attr);

        if (attrnum == sc.attrs_sz)
        {
            ERROR(UNKNOWNATTR) << "\"" << e::strescape(mapattrs[i].attr)
                               << "\" is not an attribute of space \""
                               << e::strescape(req.space) << "\"";
            return i;
        }

        if (attrnum == 0)
        {
            ERROR(DONTUSEKEY) << "attribute \""
                              << e::strescape(mapattrs[i].attr)
                              << "\" is the key and cannot be changed";
            return i;
        }

        hyperdatatype k_datatype = mapattrs[i].map_key_datatype;

        if (k_datatype == CONTAINER_TYPE(k_datatype) &&
            CONTAINER_TYPE(k_datatype) == CONTAINER_TYPE(sc.attrs[attrnum].type) &&
            mapattrs[i].value_sz == 0)
        {
            k_datatype = sc.attrs[attrnum].type;
        }

        hyperdatatype v_datatype = mapattrs[i].value_datatype;

        if (v_datatype == CONTAINER_TYPE(v_datatype) &&
            CONTAINER_TYPE(v_datatype) == CONTAINER_TYPE(sc.attrs[attrnum].type) &&
            mapattrs[i].value_sz == 0)
        {
            v_datatype = sc.attrs[attrnum].type;
        }

        funcall o;
        o.attr = attrnum;
        o.name = opinfo.fname;
        o.arg2 = e::slice(mapattrs[i].map_key, mapattrs[i].map_key_sz);
        o.arg2_datatype = k_datatype;
        o.arg1 = e::slice(mapattrs[i].value, mapattrs[i].value_sz);
        o.arg1_datatype = v_datatype;
        datatype_info* type = datatype_info::lookup(sc.attrs[attrnum].type);

        if (!type->check_args(o))
        {
            ERROR(WRONGTYPE) << "invalid attribute \""
                             << e::strescape(mapattrs[i].attr)
                             << "\": attribute has the wrong type";
            return i;
        }

        funcs.push_back(o);
    }

    return mapattrs_sz;
}

END_HYPERDEX_NAMESPACE
