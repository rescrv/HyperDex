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

// STL
#include <algorithm>

// e
#include <e/intrusive_ptr.h>

// BusyBee
#include <busybee_utils.h>

// HyperDex
#include "common/attribute_check.h"
#include "common/datatypes.h"
#include "common/funcall.h"
#include "common/macros.h"
#include "common/network_msgtype.h"
#include "common/serialization.h"
#include "client/client.h"
#include "client/constants.h"
#include "client/hyperspace_builder.h"
#include "client/pending_atomic.h"
#include "client/pending_count.h"
#include "client/pending_get.h"
#include "client/pending_group_del.h"
#include "client/pending_search.h"
#include "client/pending_search_describe.h"
#include "client/pending_sorted_search.h"

using hyperdex::client;

client :: client(const char* coordinator, uint16_t port)
    : m_config()
    , m_have_seen_config(false)
    , m_busybee_mapper(&m_config)
    , m_busybee(&m_busybee_mapper, busybee_generate_id())
    , m_coord(po6::net::hostname(coordinator, port))
    , m_next_client_id(1)
    , m_next_server_nonce(1)
    , m_pending_ops()
    , m_failed()
    , m_yieldable()
    , m_yielding()
{
}

client :: ~client() throw ()
{
}

hyperclient_returncode
client :: add_space(const char* description)
{
    hyperspace* space = hyperspace_parse(description);

    if (hyperspace_error(space))
    {
        if (space)
        {
            hyperspace_destroy(space);
        }

        return HYPERCLIENT_BADSPACE;
    }

    return m_coord.add_space(space);
}

hyperclient_returncode
client :: rm_space(const char* space)
{
    return m_coord.rm_space(space);
}

int64_t
client :: get(const char* space, const char* _key, size_t _key_sz,
              hyperclient_returncode* status,
              struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    const schema* sc = m_config.get_schema(space);

    if (!sc)
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return -1;
    }

    datatype_info* di = datatype_info::lookup(sc->attrs[0].type);
    assert(di);
    e::slice key(_key, _key_sz);

    if (!di->validate(key))
    {
        *status = HYPERCLIENT_WRONGTYPE;
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_get(m_next_client_id++, status, attrs, attrs_sz);
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ + sizeof(uint32_t) + key.size();
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << key;
    return send_keyop(space, key, REQ_GET, msg, op, status);
}

#define SEARCH_BOILERPLATE \
    if (!maintain_coord_connection(status)) \
    { \
        return -1; \
    } \
    const schema* sc = m_config.get_schema(space); \
    if (!sc) \
    { \
        *status = HYPERCLIENT_UNKNOWNSPACE; \
        return -1; \
    } \
    std::vector<attribute_check> checks; \
    std::vector<virtual_server_id> servers; \
    int64_t ret = prepare_searchop(*sc, space, chks, chks_sz, status, &checks, &servers); \
    if (ret < 0) \
    { \
        return ret; \
    }

int64_t
client :: search(const char* space,
                 const struct hyperclient_attribute_check* chks, size_t chks_sz,
                 enum hyperclient_returncode* status,
                 struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    SEARCH_BOILERPLATE
    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_search(client_id, status, attrs, attrs_sz);
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + sizeof(uint64_t)
              + pack_size(checks);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << client_id << checks;
    return perform_aggregation(servers, op, REQ_SEARCH_START, msg, status);
}

int64_t
client :: search_describe(const char* space,
                          const struct hyperclient_attribute_check* chks, size_t chks_sz,
                          enum hyperclient_returncode* status, const char** description)
{
    SEARCH_BOILERPLATE
    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_search_describe(client_id, status, description);
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + pack_size(checks);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << checks;
    return perform_aggregation(servers, op, REQ_SEARCH_DESCRIBE, msg, status);
}

int64_t
client :: sorted_search(const char* space,
                        const struct hyperclient_attribute_check* chks, size_t chks_sz,
                        const char* sort_by,
                        uint64_t limit,
                        bool maximize,
                        enum hyperclient_returncode* status,
                        struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    SEARCH_BOILERPLATE
    uint16_t sort_by_num = sc->lookup_attr(sort_by);

    if (sort_by_num == sc->attrs_sz)
    {
        *status = HYPERCLIENT_UNKNOWNATTR;
        return -1 - chks_sz;
    }

    datatype_info* di = datatype_info::lookup(sc->attrs[sort_by_num].type);

    if (!di->comparable())
    {
        *status = HYPERCLIENT_WRONGTYPE;
        return -1 - chks_sz;
    }

    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_sorted_search(this, client_id, maximize, limit, sort_by_num, di, status, attrs, attrs_sz);
    int8_t max = maximize ? 1 : 0;
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + pack_size(checks)
              + sizeof(limit)
              + sizeof(sort_by_num)
              + sizeof(max);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << checks << limit << sort_by_num << max;
    return perform_aggregation(servers, op, REQ_SORTED_SEARCH, msg, status);
}

int64_t
client :: group_del(const char* space,
                    const struct hyperclient_attribute_check* chks, size_t chks_sz,
                    enum hyperclient_returncode* status)
{
    SEARCH_BOILERPLATE
    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_group_del(client_id, status);
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + pack_size(checks);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << checks;
    return perform_aggregation(servers, op, REQ_GROUP_DEL, msg, status);
}

int64_t
client :: count(const char* space,
                const struct hyperclient_attribute_check* chks, size_t chks_sz,
                enum hyperclient_returncode* status,
                uint64_t* result)
{
    SEARCH_BOILERPLATE
    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_count(client_id, status, result);
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + pack_size(checks);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << checks;
    return perform_aggregation(servers, op, REQ_COUNT, msg, status);
}

int64_t
client :: perform_funcall(const struct hyperclient_keyop_info* opinfo,
                          const char* space, const char* _key, size_t _key_sz,
                          const struct hyperclient_attribute_check* chks, size_t chks_sz,
                          const struct hyperclient_attribute* attrs, size_t attrs_sz,
                          const struct hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                          hyperclient_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    const schema* sc = m_config.get_schema(space);

    if (!sc)
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return -1;
    }

    datatype_info* di = datatype_info::lookup(sc->attrs[0].type);
    assert(di);
    e::slice key(_key, _key_sz);

    if (!di->validate(key))
    {
        *status = HYPERCLIENT_WRONGTYPE;
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_atomic(m_next_client_id++, status);
    std::vector<attribute_check> checks;
    std::vector<funcall> funcs;
    size_t idx = 0;

    // Prepare the checks
    idx = prepare_checks(*sc, chks, chks_sz, status, &checks);

    if (idx < chks_sz)
    {
        return -2 - idx;
    }

    // Prepare the attrs
    idx = prepare_funcs(*sc, opinfo, attrs, attrs_sz, status, &funcs);

    if (idx < attrs_sz)
    {
        return -2 - chks_sz - idx;
    }

    // Prepare the mapattrs
    idx = prepare_funcs(*sc, opinfo, mapattrs, mapattrs_sz, status, &funcs);

    if (idx < mapattrs_sz)
    {
        return -2 - chks_sz - attrs_sz - idx;
    }

    std::stable_sort(checks.begin(), checks.end());
    std::stable_sort(funcs.begin(), funcs.end());
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + sizeof(uint8_t)
              + pack_size(key)
              + pack_size(checks)
              + pack_size(funcs);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    uint8_t flags = (opinfo->fail_if_not_found ? 1 : 0)
                  | (opinfo->fail_if_found ? 2 : 0)
                  | (opinfo->erase ? 0 : 128);
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ)
        << key << flags << checks << funcs;
    return send_keyop(space, key, REQ_ATOMIC, msg, op, status);
}

int64_t
client :: loop(int timeout, hyperclient_returncode* status)
{
    *status = HYPERCLIENT_SUCCESS;

    while (m_yielding ||
           !m_failed.empty() ||
           !m_yieldable.empty() ||
           !m_pending_ops.empty())
    {
        if (m_yielding)
        {
            if (!m_yielding->can_yield())
            {
                m_yielding = NULL;
                continue;
            }

            if (!m_yielding->yield(status))
            {
                return -1;
            }

            int64_t client_id = m_yielding->client_visible_id();

            if (!m_yielding->can_yield())
            {
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
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
                *status = HYPERCLIENT_POLLFAILED;
                return -1;
            case BUSYBEE_DISRUPTED:
                handle_disruption(id);
                continue;
            case BUSYBEE_TIMEOUT:
                *status = HYPERCLIENT_TIMEOUT;
                return -1;
            case BUSYBEE_INTERRUPTED:
                *status = HYPERCLIENT_INTERRUPTED;
                return -1;
            case BUSYBEE_EXTERNAL:
                continue;
            case BUSYBEE_SHUTDOWN:
            default:
                abort();
        }

        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        uint8_t mt;
        virtual_server_id vfrom;
        int64_t nonce;
        up = up >> mt >> vfrom >> nonce;

        if (up.error())
        {
            *status = HYPERCLIENT_SERVERERROR;
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

        if (vfrom == it->second.vsi &&
            id == it->second.si &&
            m_config.get_server_id(vfrom) == id)
        {
            if (!op->handle_message(this, id, vfrom, msg_type, msg, up, status))
            {
                return -1;
            }

            m_yielding = psp.op;
        }
        else
        {
            *status = HYPERCLIENT_SERVERERROR;
            return -1;
        }
    }

    *status = HYPERCLIENT_NONEPENDING;
    return -1;
}

size_t
client :: prepare_checks(const schema& sc,
                         const hyperclient_attribute_check* chks, size_t chks_sz,
                         hyperclient_returncode* status,
                         std::vector<attribute_check>* checks)
{
    checks->reserve(checks->size() + chks_sz);

    for (size_t i = 0; i < chks_sz; ++i)
    {
        uint16_t attrnum = sc.lookup_attr(chks[i].attr);

        if (attrnum >= sc.attrs_sz)
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return i;
        }

        hyperdatatype datatype = chks[i].datatype;

        if (datatype == CONTAINER_TYPE(datatype) &&
            CONTAINER_TYPE(datatype) == CONTAINER_TYPE(sc.attrs[attrnum].type) &&
            chks[i].value_sz == 0)
        {
            datatype = sc.attrs[attrnum].type;
        }

        attribute_check c;
        c.attr = attrnum;
        c.value = e::slice(chks[i].value, chks[i].value_sz);
        c.datatype = datatype;
        c.predicate = chks[i].predicate;

        if (!validate_attribute_check(sc, c))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return i;
        }

        checks->push_back(c);
    }

    return chks_sz;
}

size_t
client :: prepare_funcs(const schema& sc,
                        const hyperclient_keyop_info* opinfo,
                        const hyperclient_attribute* attrs, size_t attrs_sz,
                        hyperclient_returncode* status,
                        std::vector<funcall>* funcs)
{
    funcs->reserve(funcs->size() + attrs_sz);

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        uint16_t attrnum = sc.lookup_attr(attrs[i].attr);

        if (attrnum == sc.attrs_sz)
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return i;
        }

        if (attrnum == 0)
        {
            *status = HYPERCLIENT_DONTUSEKEY;
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
        o.name = opinfo->fname;
        o.arg1 = e::slice(attrs[i].value, attrs[i].value_sz);
        o.arg1_datatype = datatype;
        datatype_info* type = datatype_info::lookup(sc.attrs[attrnum].type);

        if (!type->check_args(o))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return i;
        }

        funcs->push_back(o);
    }

    return attrs_sz;
}

size_t
client :: prepare_funcs(const schema& sc,
                        const hyperclient_keyop_info* opinfo,
                        const hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                        hyperclient_returncode* status,
                        std::vector<funcall>* funcs)
{
    funcs->reserve(funcs->size() + mapattrs_sz);

    for (size_t i = 0; i < mapattrs_sz; ++i)
    {
        uint16_t attrnum = sc.lookup_attr(mapattrs[i].attr);

        if (attrnum == sc.attrs_sz)
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return i;
        }

        if (attrnum == 0)
        {
            *status = HYPERCLIENT_DONTUSEKEY;
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
        o.name = opinfo->fname;
        o.arg2 = e::slice(mapattrs[i].map_key, mapattrs[i].map_key_sz);
        o.arg2_datatype = k_datatype;
        o.arg1 = e::slice(mapattrs[i].value, mapattrs[i].value_sz);
        o.arg1_datatype = v_datatype;
        datatype_info* type = datatype_info::lookup(sc.attrs[attrnum].type);

        if (!type->check_args(o))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return i;
        }

        funcs->push_back(o);
    }

    return mapattrs_sz;
}

size_t
client :: prepare_searchop(const schema& sc,
                           const char* space,
                           const struct hyperclient_attribute_check* chks, size_t chks_sz,
                           hyperclient_returncode* status,
                           std::vector<attribute_check>* checks,
                           std::vector<virtual_server_id>* servers)
{
    size_t num_checks = prepare_checks(sc, chks, chks_sz, status, checks);

    if (num_checks != chks_sz)
    {
        return -1 - num_checks;
    }

    std::stable_sort(checks->begin(), checks->end());
    m_config.lookup_search(space, *checks, servers); // XXX search guaranteed empty vs. search encounters offline server

    if (servers->empty())
    {
        *status = HYPERCLIENT_INTERNAL;
        return -1;
    }

    return 0;
}

int64_t
client :: perform_aggregation(const std::vector<virtual_server_id>& servers,
                              e::intrusive_ptr<pending_aggregation> op,
                              network_msgtype mt,
                              std::auto_ptr<e::buffer> msg,
                              hyperclient_returncode* status)
{
    for (size_t i = 0; i < servers.size(); ++i)
    {
        uint64_t nonce = m_next_server_nonce++;
        pending_server_pair psp(m_config.get_server_id(servers[i]), servers[i], op.get());
        std::pair<pending_map_t::iterator, bool> inserted;
        inserted = m_pending_ops.insert(std::make_pair(nonce, psp));
        assert(inserted.second);
        std::auto_ptr<e::buffer> msg_copy(msg->copy());

        if (!send(mt, psp.vsi, nonce, msg_copy, op.get(), status))
        {
            m_pending_ops.erase(inserted.first);
            psp.op->handle_sent_to(psp.si, psp.vsi);
            m_failed.push_back(psp);
        }
    }

    return op->client_visible_id();
}

bool
client :: maintain_coord_connection(hyperclient_returncode* status)
{
    if (!m_have_seen_config)
    {
        if (!m_coord.wait_for_config(status))
        {
            return false;
        }

        if (m_busybee.set_external_fd(m_coord.poll_fd()) < 0)
        {
            *status = HYPERCLIENT_POLLFAILED;
            return false;
        }
    }
    else
    {
        if (!m_coord.poll_for_config(status))
        {
            return true;
        }
    }

    if (m_config.version() < m_coord.config().version())
    {
        m_config = m_coord.config();
        pending_map_t::iterator it = m_pending_ops.begin();

        while (it != m_pending_ops.end())
        {
            // If the mapping that was true when the operation started is no
            // longer true, we fail the operation with a RECONFIGURE.
            if (m_config.get_server_id(it->second.vsi) != it->second.si)
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

        m_have_seen_config = true;
    }

    return true;
}

bool
client :: send(network_msgtype mt,
               const virtual_server_id& to,
               uint64_t nonce,
               std::auto_ptr<e::buffer> msg,
               e::intrusive_ptr<pending> op,
               hyperclient_returncode* status)
{
    const uint8_t type = static_cast<uint8_t>(mt);
    const uint8_t flags = 0;
    const uint64_t version = m_config.version();
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << type << flags << version << to << nonce;
    server_id id = m_config.get_server_id(to);
    m_busybee.set_timeout(-1);

    switch (m_busybee.send(id.get(), msg))
    {
        case BUSYBEE_SUCCESS:
            op->handle_sent_to(id, to);
            m_pending_ops.insert(std::make_pair(nonce, pending_server_pair(id, to, op)));
            return true;
        case BUSYBEE_DISRUPTED:
            handle_disruption(id);
            return false;
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
            *status = HYPERCLIENT_POLLFAILED;
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        case BUSYBEE_INTERRUPTED:
        default:
            abort();
    }
}

int64_t
client :: send_keyop(const char* space,
                     const e::slice& key,
                     network_msgtype mt,
                     std::auto_ptr<e::buffer> msg,
                     e::intrusive_ptr<pending> op,
                     hyperclient_returncode* status)
{
    virtual_server_id vsi = m_config.point_leader(space, key);

    if (vsi == virtual_server_id())
    {
        op->set_status(HYPERCLIENT_OFFLINE);
        return -1;
    }

    int64_t nonce = m_next_server_nonce++;

    if (send(mt, vsi, nonce, msg, op, status))
    {
        return op->client_visible_id();
    }
    else
    {
        op->set_status(HYPERCLIENT_RECONFIGURE);
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

std::ostream&
operator << (std::ostream& lhs, hyperclient_returncode rhs)
{
    switch (rhs)
    {
        STRINGIFY(HYPERCLIENT_SUCCESS);
        STRINGIFY(HYPERCLIENT_NOTFOUND);
        STRINGIFY(HYPERCLIENT_SEARCHDONE);
        STRINGIFY(HYPERCLIENT_CMPFAIL);
        STRINGIFY(HYPERCLIENT_READONLY);
        STRINGIFY(HYPERCLIENT_UNKNOWNSPACE);
        STRINGIFY(HYPERCLIENT_COORDFAIL);
        STRINGIFY(HYPERCLIENT_SERVERERROR);
        STRINGIFY(HYPERCLIENT_POLLFAILED);
        STRINGIFY(HYPERCLIENT_OVERFLOW);
        STRINGIFY(HYPERCLIENT_RECONFIGURE);
        STRINGIFY(HYPERCLIENT_TIMEOUT);
        STRINGIFY(HYPERCLIENT_UNKNOWNATTR);
        STRINGIFY(HYPERCLIENT_DUPEATTR);
        STRINGIFY(HYPERCLIENT_NONEPENDING);
        STRINGIFY(HYPERCLIENT_DONTUSEKEY);
        STRINGIFY(HYPERCLIENT_WRONGTYPE);
        STRINGIFY(HYPERCLIENT_NOMEM);
        STRINGIFY(HYPERCLIENT_BADCONFIG);
        STRINGIFY(HYPERCLIENT_BADSPACE);
        STRINGIFY(HYPERCLIENT_DUPLICATE);
        STRINGIFY(HYPERCLIENT_INTERRUPTED);
        STRINGIFY(HYPERCLIENT_CLUSTER_JUMP);
        STRINGIFY(HYPERCLIENT_COORD_LOGGED);
        STRINGIFY(HYPERCLIENT_OFFLINE);
        STRINGIFY(HYPERCLIENT_INTERNAL);
        STRINGIFY(HYPERCLIENT_EXCEPTION);
        STRINGIFY(HYPERCLIENT_GARBAGE);
        default:
            lhs << "unknown returncode";
            break;
    }

    return lhs;
}
