// Copyright (c) 2011-2013, Cornell University
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

#define __STDC_LIMIT_MACROS

// POSIX
#include <poll.h>

// STL
#include <algorithm>

// e
#include <e/intrusive_ptr.h>
#include <e/strescape.h>

// BusyBee
#include <busybee_utils.h>

// BSON
#include <bson.h>

// HyperDex
#include "visibility.h"
#include "common/attribute_check.h"
#include "common/datatype_info.h"
#include "common/funcall.h"
#include "common/macros.h"
#include "common/network_msgtype.h"
#include "common/serialization.h"
#include "client/atomic_request.h"
#include "client/atomic_group_request.h"
#include "client/group_del_request.h"
#include "client/sorted_search_request.h"
#include "client/search_describe_request.h"
#include "client/search_request.h"
#include "client/count_request.h"
#include "client/client.h"
#include "client/constants.h"
#include "client/pending_atomic.h"
#include "client/pending_atomic_group.h"
#include "client/pending_count.h"
#include "client/pending_get.h"
#include "client/pending_get_partial.h"
#include "client/pending_group_del.h"
#include "client/pending_search.h"
#include "client/pending_search_describe.h"
#include "client/pending_sorted_search.h"

#define ERROR(CODE) \
    status = HYPERDEX_CLIENT_ ## CODE; \
    m_last_error.set_loc(__FILE__, __LINE__); \
    m_last_error.set_msg()

#define _BUSYBEE_ERROR(BBRC) \
    case BUSYBEE_ ## BBRC: \
        ERROR(INTERNAL) << "internal error: BusyBee unexpectedly returned " XSTR(BBRC) << ": please file a bug"

#define BUSYBEE_ERROR_CASE(BBRC) \
    _BUSYBEE_ERROR(BBRC); \
    return -1;

#define BUSYBEE_ERROR_CASE_FALSE(BBRC) \
    _BUSYBEE_ERROR(BBRC); \
    return false;

using hyperdex::client;

client :: client(const char* coordinator, uint16_t port)
    : m_coord(coordinator, port)
    , m_gc()
    , m_gc_ts()
    , m_busybee_mapper(m_coord.config())
    , m_busybee(&m_gc, &m_busybee_mapper, 0)
    , m_next_client_id(1)
    , m_next_server_nonce(1)
    , m_pending_ops()
    , m_failed()
    , m_yieldable()
    , m_yielding()
    , m_yielded()
    , m_last_error()
{
    m_gc.register_thread(&m_gc_ts);
}

client :: client(const char* conn_str)
    : m_coord(conn_str)
    , m_gc()
    , m_gc_ts()
    , m_busybee_mapper(m_coord.config())
    , m_busybee(&m_gc, &m_busybee_mapper, 0)
    , m_next_client_id(1)
    , m_next_server_nonce(1)
    , m_pending_ops()
    , m_failed()
    , m_yieldable()
    , m_yielding()
    , m_yielded()
    , m_last_error()
{
    m_gc.register_thread(&m_gc_ts);
}

client :: ~client() throw ()
{
    m_gc.deregister_thread(&m_gc_ts);
}

int64_t
client :: get(const char* space, const char* _key, size_t _key_sz,
              hyperdex_client_returncode& status,
              const hyperdex_client_attribute** attrs, size_t* attrs_sz)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    const schema* sc = m_coord.config()->get_schema(space);

    if (!sc)
    {
        ERROR(UNKNOWNSPACE) << "space \"" << e::strescape(space) << "\" does not exist";
        return -1;
    }

    datatype_info* di = datatype_info::lookup(sc->attrs[0].type);
    assert(di);
    e::slice key(_key, _key_sz);

    if (!di->validate(key))
    {
        ERROR(WRONGTYPE) << "key must be type " << sc->attrs[0].type;
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_get(m_next_client_id++, status, attrs, attrs_sz);
    size_t sz = HYPERDEX_CLIENT_HEADER_SIZE_REQ + sizeof(uint32_t) + key.size();
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_CLIENT_HEADER_SIZE_REQ) << key;
    return send_keyop(space, key, REQ_GET, msg, op, status);
}

int64_t
client :: get_partial(const char* space, const char* _key, size_t _key_sz,
                      const char** attrnames, size_t attrnames_sz,
                      hyperdex_client_returncode& status,
                      const hyperdex_client_attribute** attrs, size_t* attrs_sz)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    const schema* sc = m_coord.config()->get_schema(space);

    if (!sc)
    {
        ERROR(UNKNOWNSPACE) << "space \"" << e::strescape(space) << "\" does not exist";
        return -1;
    }

    std::vector<uint16_t> attrnums;

    for (size_t i = 0; i < attrnames_sz; ++i)
    {
        uint16_t attr = sc->lookup_attr(attrnames[i]);

        if (attr == UINT16_MAX)
        {
            ERROR(UNKNOWNATTR) << "attribute \"" << e::strescape(attrnames[i])
                               << "\" is not an attribute in space \""
                               << e::strescape(space) << "\"";
            return -1;
        }

        if (attr == 0)
        {
            ERROR(DONTUSEKEY) << "don't specify the key (\"" << e::strescape(attrnames[i])
                              << "\") when doing get_partial on space \""
                               << e::strescape(space) << "\"";
            return -1;
        }

        attrnums.push_back(attr);
    }

    datatype_info* di = datatype_info::lookup(sc->attrs[0].type);
    assert(di);
    e::slice key(_key, _key_sz);

    if (!di->validate(key))
    {
        ERROR(WRONGTYPE) << "key must be type " << sc->attrs[0].type;
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_get_partial(m_next_client_id++, status, attrs, attrs_sz);
    size_t sz = HYPERDEX_CLIENT_HEADER_SIZE_REQ
              + sizeof(uint32_t) + key.size()
              + sizeof(uint32_t) + attrnums.size() * sizeof(uint16_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(HYPERDEX_CLIENT_HEADER_SIZE_REQ);
    pa = pa << key << uint32_t(attrnums.size());

    for (size_t i = 0; i < attrnums.size(); ++i)
    {
        pa = pa << attrnums[i];
    }

    return send_keyop(space, key, REQ_GET_PARTIAL, msg, op, status);
}

int64_t
client :: search(const char* space,
                 const hyperdex_client_attribute_check* selection, size_t selection_sz,
                 hyperdex_client_returncode& status,
                 const hyperdex_client_attribute** attrs, size_t* attrs_sz)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    search_request request(*this, m_coord, space);
    int res = request.prepare(selection, selection_sz, status);

    if(res < 0)
    {
        return res;
    }

    int64_t client_id = m_next_client_id++;
    std::auto_ptr<e::buffer> msg(request.create_message(client_id));

    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_search(client_id, status, attrs, attrs_sz);

    return perform_aggregation(request.get_servers(), op, REQ_SEARCH_START, msg, status);
}

int64_t
client :: search_describe(const char* space,
                          const hyperdex_client_attribute_check* selection, size_t selection_sz,
                          hyperdex_client_returncode& status, const char** description)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    search_describe_request request(*this, m_coord, space);
    int res = request.prepare(selection, selection_sz, status);

    if(res < 0)
    {
        return res;
    }

    int64_t client_id = m_next_client_id++;
    std::auto_ptr<e::buffer> msg(request.create_message());

    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_search_describe(client_id, status, description);

    return perform_aggregation(request.get_servers(), op, REQ_SEARCH_DESCRIBE, msg, status);
}

int64_t
client :: sorted_search(const char* space,
                        const hyperdex_client_attribute_check* selection, size_t selection_sz,
                        const char* sort_by,
                        uint64_t limit,
                        bool maximize,
                        hyperdex_client_returncode& status,
                        const hyperdex_client_attribute** attrs, size_t* attrs_sz)
{
    sorted_search_request request(*this, m_coord, space);

    int res = request.prepare(selection, selection_sz, status, sort_by);

    if(res < 0)
    {
        return res;
    }

    int64_t client_id = m_next_client_id++;
    std::auto_ptr<e::buffer> msg(request.create_message(limit, maximize));

    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_sorted_search(this, client_id, maximize, limit,
                request.get_sort_by_index(), request.get_sort_di(), status, attrs, attrs_sz);
    return perform_aggregation(request.get_servers(), op, REQ_SORTED_SEARCH, msg, status);
}

int64_t
client :: group_del(const char* space,
                    const hyperdex_client_attribute_check* selection, size_t selection_sz,
                    hyperdex_client_returncode& status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    group_del_request request(*this, m_coord, space);
    int res = request.prepare(selection, selection_sz, status);

    if(res < 0)
    {
        return res;
    }

    std::auto_ptr<e::buffer> msg(request.create_message());

    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_group_del(m_next_client_id++, status);

    return perform_aggregation(request.get_servers(), op, REQ_GROUP_DEL, msg, status);
}

int64_t
client :: count(const char* space,
                const hyperdex_client_attribute_check* selection, size_t selection_sz,
                hyperdex_client_returncode& status,
                uint64_t& result)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    count_request request(*this, m_coord, space);
    int res = request.prepare(selection, selection_sz, status);

    if(res < 0)
    {
        return res;
    }

    std::auto_ptr<e::buffer> msg(request.create_message());

    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_count(m_next_client_id++, status, result);

    return perform_aggregation(request.get_servers(), op, REQ_COUNT, msg, status);
}

int64_t
client :: perform_group_funcall(const hyperdex_client_keyop_info* opinfo,
                          const char* space, const hyperdex_client_attribute_check* selection, size_t selection_sz,
                          const hyperdex_client_attribute* attrs, size_t attrs_sz,
                          const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                          hyperdex_client_returncode& status,
                          uint64_t &update_count)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    atomic_group_request request(*this, m_coord, space);
    int res = request.prepare(*opinfo, selection, selection_sz, attrs, attrs_sz, mapattrs, mapattrs_sz, status);

    if(res < 0)
    {
        return res;
    }

    std::auto_ptr<e::buffer> msg(request.create_message(*opinfo));

    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_atomic_group(m_next_client_id++, status, update_count);

    return perform_aggregation(request.get_servers(), op, REQ_GROUP_ATOMIC, msg, status);
}

int64_t
client :: perform_funcall(const hyperdex_client_keyop_info* opinfo,
                          const char* space, const char* _key, size_t _key_sz,
                          const hyperdex_client_attribute_check* chks, size_t chks_sz,
                          const hyperdex_client_attribute* attrs, size_t attrs_sz,
                          const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                          hyperdex_client_returncode& status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    atomic_request request(*this, m_coord, space);

    e::slice key(_key, _key_sz);

    status = request.validate_key(key);
    if(status != HYPERDEX_CLIENT_SUCCESS)
    {
        return -1;
    }

    int res = request.prepare(*opinfo, chks, chks_sz, attrs, attrs_sz, mapattrs, mapattrs_sz, status);

    if(res < 0)
    {
        return res;
    }

    std::auto_ptr<e::buffer> msg(request.create_message(*opinfo, key));

    e::intrusive_ptr<pending> op;
    op = new pending_atomic(m_next_client_id++, status);

    return send_keyop(space, key, REQ_ATOMIC, msg, op, status);
}

int64_t
client :: loop(int timeout, hyperdex_client_returncode& status)
{
    status = HYPERDEX_CLIENT_SUCCESS;
    m_last_error = e::error();

    while (m_yielding ||
           !m_failed.empty() ||
           !m_yieldable.empty() ||
           !m_pending_ops.empty())
    {
        m_gc.quiescent_state(&m_gc_ts);

        if (m_yielding)
        {
            if (!m_yielding->can_yield())
            {
                m_yielding = NULL;
                continue;
            }

            if (!m_yielding->yield(status, m_last_error))
            {
                return -1;
            }

            int64_t client_id = m_yielding->client_visible_id();
            m_last_error = m_yielding->error();

            if (!m_yielding->can_yield())
            {
                m_yielded = m_yielding;
                m_yielding = NULL;
            }

            return client_id;
        }
        else if (!m_yieldable.empty())
        {
            m_yielding = m_yieldable.front();
            m_yieldable.pop_front();
            continue;
        }
        else if (!m_failed.empty())
        {
            const pending_server_pair& psp(m_failed.front());
            psp.op->handle_failure(psp.si, psp.vsi);
            m_yielding = psp.op;
            m_failed.pop_front();
            continue;
        }

        m_yielded = NULL;
        assert(!m_pending_ops.empty());

        if (!maintain_coord_connection(status))
        {
            return -1;
        }

        uint64_t sid_num;
        std::auto_ptr<e::buffer> msg;
        m_busybee.set_timeout(timeout);
        busybee_returncode rc = m_busybee.recv(&sid_num, &msg);
        server_id id(sid_num);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_INTERRUPTED:
                ERROR(INTERRUPTED) << "signal received";
                return -1;
            case BUSYBEE_TIMEOUT:
                ERROR(TIMEOUT) << "operation timed out";
                return -1;
            case BUSYBEE_DISRUPTED:
                handle_disruption(id);
                continue;
            case BUSYBEE_EXTERNAL:
                continue;
            BUSYBEE_ERROR_CASE(POLLFAILED);
            BUSYBEE_ERROR_CASE(ADDFDFAIL);
            BUSYBEE_ERROR_CASE(SHUTDOWN);
            default:
                ERROR(INTERNAL) << "internal error: BusyBee unexpectedly returned "
                                << (unsigned) rc << ": please file a bug";
                return -1;
        }

        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        uint8_t mt;
        virtual_server_id vfrom;
        int64_t nonce;
        up = up >> mt >> vfrom >> nonce;

        if (up.error())
        {
            ERROR(SERVERERROR) << "communication error: server "
                               << sid_num << " sent message="
                               << msg->as_slice().hex()
                               << " with invalid header";
            return -1;
        }

        network_msgtype msg_type = static_cast<network_msgtype>(mt);
        pending_map_t::iterator it = m_pending_ops.find(nonce);

        if (it == m_pending_ops.end())
        {
            continue;
        }

        const pending_server_pair psp(it->second);
        e::intrusive_ptr<pending> op = psp.op;
        m_pending_ops.erase(it);

        if (msg_type == CONFIGMISMATCH)
        {
            m_failed.push_back(psp);
            continue;
        }

        if (vfrom == psp.vsi &&
            id == psp.si &&
            m_coord.config()->get_server_id(vfrom) == id)
        {
            if (!op->handle_message(this, id, vfrom, msg_type, msg, up, status, m_last_error))
            {
                return -1;
            }

            m_yielding = psp.op;
        }
        else
        {
            ERROR(SERVERERROR) << "wrong server replied for nonce=" << nonce
                               << ": expected it to come from "
                               << psp.vsi << "/" << psp.si
                               << "; it came from "
                               << vfrom << "/" << id
                               << "; our config says that virtual_id should map to "
                               << m_coord.config()->get_server_id(vfrom);
            return -1;
        }
    }

    uint64_t sid_num;
    std::auto_ptr<e::buffer> msg;
    m_busybee.set_timeout(timeout);
    busybee_returncode rc = m_busybee.recv(&sid_num, &msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
        case BUSYBEE_TIMEOUT:
            break;
        case BUSYBEE_INTERRUPTED:
            ERROR(INTERRUPTED) << "signal received";
            return -1;
        case BUSYBEE_DISRUPTED:
            handle_disruption(server_id(sid_num));
            break;
        case BUSYBEE_EXTERNAL:
            if (!maintain_coord_connection(status))
            {
                return -1;
            }
            break;
        BUSYBEE_ERROR_CASE(POLLFAILED);
        BUSYBEE_ERROR_CASE(ADDFDFAIL);
        BUSYBEE_ERROR_CASE(SHUTDOWN);
        default:
            ERROR(INTERNAL) << "internal error: BusyBee unexpectedly returned "
                            << (unsigned) rc << ": please file a bug";
            return -1;
    }

    ERROR(NONEPENDING) << "no outstanding operations to process";
    return -1;
}

int
client :: poll()
{
    return m_busybee.poll_fd();
}

int
client :: block(int timeout)
{
    pollfd pfd;
    pfd.fd = m_busybee.poll_fd();
    pfd.events = POLLIN|POLLHUP;
    pfd.revents = 0;
    return ::poll(&pfd, 1, timeout) >= 0 ? 0 : -1;
}

const char*
client :: error_message()
{
    return m_last_error.msg();
}

const char*
client :: error_location()
{
    return m_last_error.loc();
}

void
client :: set_error_message(const char* msg)
{
    m_last_error = e::error();
    m_last_error.set_loc(__FILE__, __LINE__);
    m_last_error.set_msg() << msg;
}

hyperdatatype
client :: attribute_type(const char* space, const char* name,
                         hyperdex_client_returncode& status)
{
    if (maintain_coord_connection(status) < 0)
    {
        return HYPERDATATYPE_GARBAGE;
    }

    const hyperdex::schema* sc = m_coord.config()->get_schema(space);

    if (!sc)
    {
        ERROR(UNKNOWNSPACE) << "space \"" << e::strescape(space) << "\" does not exist";
        return HYPERDATATYPE_GARBAGE;
    }

    uint16_t attrnum = sc->lookup_attr(name);

    if (attrnum == sc->attrs_sz)
    {
        ERROR(UNKNOWNATTR) << "\"" << e::strescape(name)
                           << "\" is not an attribute of space \""
                           << e::strescape(space) << "\"";
        return HYPERDATATYPE_GARBAGE;
    }

    return sc->attrs[attrnum].type;
}

size_t
client :: prepare_funcs(const char* space, const schema& sc,
                        const hyperdex_client_keyop_info& opinfo,
                        const hyperdex_client_attribute* attrs, size_t attrs_sz,
                        arena_t* allocate,
                        hyperdex_client_returncode& status,
                        std::vector<funcall>* funcs)
{
    funcs->reserve(funcs->size() + attrs_sz);

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        const char* attr = attrs[i].attr;
        const char* path = strstr(attrs[i].attr, ".");

        // This is a path to a document value
        if (path != NULL)
        {
            // Remove the dot
            path = path+1;

            // Set attribute name to only the first part
            std::string orig(attrs[i].attr);
            attr = orig.substr(0, orig.find('.')).c_str();
        }

        uint16_t attrnum = sc.lookup_attr(attr);

        if (attrnum == sc.attrs_sz)
        {
            ERROR(UNKNOWNATTR) << "\"" << e::strescape(attr)
                               << "\" is not an attribute of space \""
                               << e::strescape(space) << "\"";
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
            attrs[i].value_sz == 0)
        {
            datatype = sc.attrs[attrnum].type;
        }

        funcall o;
        o.attr = attrnum;
        o.name = opinfo.fname;
        o.arg1_datatype = datatype;

        if (datatype == HYPERDATATYPE_DOCUMENT)
        {
            bson_error_t error;
            bson_t *bson = bson_new_from_json(reinterpret_cast<const uint8_t*>(attrs[i].value), attrs[i].value_sz, &error);

            if(!bson)
            {
                //TODO parse error
                ERROR(WRONGTYPE) << "invalid document \"" << e::strescape(attr) << "\"";
            }

            size_t len = bson->len;
            const char* data = reinterpret_cast<const char*>(bson_get_data(bson));

            //TODO make arena store smart pointers instead of copying to strings
            allocate->push_back(std::string());
            std::string& s(allocate->back());
            s.append(data, len);

            o.arg1 = e::slice(s.data(), len);
            bson_destroy(bson);
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

        funcs->push_back(o);
    }

    return attrs_sz;
}

size_t
client :: prepare_funcs(const char* space, const schema& sc,
                        const hyperdex_client_keyop_info& opinfo,
                        const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                        arena_t*,
                        hyperdex_client_returncode& status,
                        std::vector<funcall>* funcs)
{
    funcs->reserve(funcs->size() + mapattrs_sz);

    for (size_t i = 0; i < mapattrs_sz; ++i)
    {
        uint16_t attrnum = sc.lookup_attr(mapattrs[i].attr);

        if (attrnum == sc.attrs_sz)
        {
            ERROR(UNKNOWNATTR) << "\"" << e::strescape(mapattrs[i].attr)
                               << "\" is not an attribute of space \""
                               << e::strescape(space) << "\"";
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

        funcs->push_back(o);
    }

    return mapattrs_sz;
}

int64_t
client :: perform_aggregation(const std::vector<virtual_server_id>& servers,
                              e::intrusive_ptr<pending_aggregation> _op,
                              network_msgtype mt,
                              std::auto_ptr<e::buffer> msg,
                              hyperdex_client_returncode& status)
{
    e::intrusive_ptr<pending> op(_op.get());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        uint64_t nonce = m_next_server_nonce++;
        pending_server_pair psp(m_coord.config()->get_server_id(servers[i]), servers[i], op);
        std::auto_ptr<e::buffer> msg_copy(msg->copy());

        if (!send(mt, psp.vsi, nonce, msg_copy, op, status))
        {
            m_failed.push_back(psp);
        }
    }

    return op->client_visible_id();
}

bool
client :: maintain_coord_connection(hyperdex_client_returncode& status)
{
    replicant_returncode rc;
    uint64_t old_version = m_coord.config()->version();

    if (!m_coord.ensure_configuration(&rc))
    {
        if (rc == REPLICANT_INTERRUPTED)
        {
            ERROR(INTERRUPTED) << "signal received";
        }
        else if (rc == REPLICANT_TIMEOUT)
        {
            ERROR(TIMEOUT) << "operation timed out";
        }
        else
        {
            ERROR(COORDFAIL) << "coordinator failure: " << m_coord.error().msg();
        }

        return false;
    }

    if (m_busybee.set_external_fd(m_coord.poll_fd()) != BUSYBEE_SUCCESS)
    {
        status = HYPERDEX_CLIENT_POLLFAILED;
        return false;
    }

    uint64_t new_version = m_coord.config()->version();

    if (old_version < new_version)
    {
        pending_map_t::iterator it = m_pending_ops.begin();

        while (it != m_pending_ops.end())
        {
            // If the mapping that was true when the operation started is no
            // longer true, we fail the operation with a RECONFIGURE.
            if (m_coord.config()->get_server_id(it->second.vsi) != it->second.si)
            {
                m_failed.push_back(it->second);
                pending_map_t::iterator tmp = it;
                ++it;
                m_pending_ops.erase(tmp);
            }
            else
            {
                ++it;
            }
        }
    }

    return true;
}

bool
client :: send(network_msgtype mt,
               const virtual_server_id& to,
               uint64_t nonce,
               std::auto_ptr<e::buffer> msg,
               e::intrusive_ptr<pending> op,
               hyperdex_client_returncode& status)
{
    const uint8_t type = static_cast<uint8_t>(mt);
    const uint8_t flags = 0;
    const uint64_t version = m_coord.config()->version();
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << type << flags << version << to << nonce;
    server_id id = m_coord.config()->get_server_id(to);
    m_busybee.set_timeout(-1);
    busybee_returncode rc = m_busybee.send(id.get(), msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            op->handle_sent_to(id, to);
            m_pending_ops.insert(std::make_pair(nonce, pending_server_pair(id, to, op)));
            return true;
        case BUSYBEE_DISRUPTED:
            handle_disruption(id);
            ERROR(SERVERERROR) << "server " << id.get() << " had a communication disruption";
            return false;
        BUSYBEE_ERROR_CASE_FALSE(SHUTDOWN);
        BUSYBEE_ERROR_CASE_FALSE(POLLFAILED);
        BUSYBEE_ERROR_CASE_FALSE(ADDFDFAIL);
        BUSYBEE_ERROR_CASE_FALSE(TIMEOUT);
        BUSYBEE_ERROR_CASE_FALSE(EXTERNAL);
        BUSYBEE_ERROR_CASE_FALSE(INTERRUPTED);
        default:
            ERROR(INTERNAL) << "internal error: BusyBee unexpectedly returned "
                                << (unsigned) rc << ": please file a bug";
            return false;
    }
}

int64_t
client :: send_keyop(const char* space,
                     const e::slice& key,
                     network_msgtype mt,
                     std::auto_ptr<e::buffer> msg,
                     e::intrusive_ptr<pending> op,
                     hyperdex_client_returncode& status)
{
    virtual_server_id vsi = m_coord.config()->point_leader(space, key);

    if (vsi == virtual_server_id())
    {
        ERROR(OFFLINE) << "all servers for key \""
                       << e::strescape(std::string(reinterpret_cast<const char*>(key.data()), key.size()))
                       << "\" in space \"" << e::strescape(space)
                       << "\" are offline: bring one or more online to remedy the issue";
        return -1;
    }

    int64_t nonce = m_next_server_nonce++;

    if (send(mt, vsi, nonce, msg, op, status))
    {
        return op->client_visible_id();
    }
    else
    {
        ERROR(RECONFIGURE) << "could not send " << mt << " to " << vsi;
        return -1;
    }
}

void
client :: handle_disruption(const server_id& si)
{
    pending_map_t::iterator it = m_pending_ops.begin();

    while (it != m_pending_ops.end())
    {
        if (it->second.si == si)
        {
            m_failed.push_back(it->second);
            pending_map_t::iterator tmp = it;
            ++it;
            m_pending_ops.erase(tmp);
        }
        else
        {
            ++it;
        }
    }

    m_busybee.drop(si.get());
}

HYPERDEX_API std::ostream&
operator << (std::ostream& lhs, hyperdex_client_returncode rhs)
{
    switch (rhs)
    {
        STRINGIFY(HYPERDEX_CLIENT_SUCCESS);
        STRINGIFY(HYPERDEX_CLIENT_NOTFOUND);
        STRINGIFY(HYPERDEX_CLIENT_SEARCHDONE);
        STRINGIFY(HYPERDEX_CLIENT_CMPFAIL);
        STRINGIFY(HYPERDEX_CLIENT_READONLY);
        STRINGIFY(HYPERDEX_CLIENT_UNKNOWNSPACE);
        STRINGIFY(HYPERDEX_CLIENT_COORDFAIL);
        STRINGIFY(HYPERDEX_CLIENT_SERVERERROR);
        STRINGIFY(HYPERDEX_CLIENT_POLLFAILED);
        STRINGIFY(HYPERDEX_CLIENT_OVERFLOW);
        STRINGIFY(HYPERDEX_CLIENT_RECONFIGURE);
        STRINGIFY(HYPERDEX_CLIENT_TIMEOUT);
        STRINGIFY(HYPERDEX_CLIENT_UNKNOWNATTR);
        STRINGIFY(HYPERDEX_CLIENT_DUPEATTR);
        STRINGIFY(HYPERDEX_CLIENT_NONEPENDING);
        STRINGIFY(HYPERDEX_CLIENT_DONTUSEKEY);
        STRINGIFY(HYPERDEX_CLIENT_WRONGTYPE);
        STRINGIFY(HYPERDEX_CLIENT_NOMEM);
        STRINGIFY(HYPERDEX_CLIENT_INTERRUPTED);
        STRINGIFY(HYPERDEX_CLIENT_CLUSTER_JUMP);
        STRINGIFY(HYPERDEX_CLIENT_OFFLINE);
        STRINGIFY(HYPERDEX_CLIENT_INTERNAL);
        STRINGIFY(HYPERDEX_CLIENT_EXCEPTION);
        STRINGIFY(HYPERDEX_CLIENT_GARBAGE);
        default:
            lhs << "unknown hyperdex_client_returncode";
            return lhs;
    }

    return lhs;
}
