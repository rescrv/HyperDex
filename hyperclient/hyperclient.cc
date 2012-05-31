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

// C++
#include <iostream>

// STL
#include <algorithm>

// po6
#include <po6/net/location.h>

// BusyBee
#include <busybee_returncode.h>
#include <busybee_st.h>

// HyperDex
#include "hyperdex/hyperdex/configuration.h"
#include "hyperdex/hyperdex/coordinatorlink.h"
#include "hyperdex/hyperdex/microop.h"

// HyperClient
#include "hyperclient/constants.h"
#include "hyperclient/hyperclient.h"
#include "hyperclient/hyperclient_completedop.h"
#include "hyperclient/hyperclient_pending.h"
#include "hyperclient/hyperclient_pending_get.h"
#include "hyperclient/hyperclient_pending_search.h"
#include "hyperclient/hyperclient_pending_statusonly.h"
#include "hyperclient/util.h"

#define MICROOP_BASE_SIZE \
    (sizeof(uint16_t) + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint32_t) * 2)

hyperclient :: hyperclient(const char* coordinator, in_port_t port)
    : m_coord(new hyperdex::coordinatorlink(po6::net::location(coordinator, port)))
    , m_config(new hyperdex::configuration())
    , m_busybee(new busybee_st())
    , m_incomplete()
    , m_complete()
    , m_server_nonce(1)
    , m_client_id(1)
    , m_old_coord_fd(-1)
    , m_have_seen_config(false)
{
    m_coord->set_announce("client");
}

hyperclient :: ~hyperclient() throw ()
{
}

int64_t
hyperclient :: get(const char* space, const char* key, size_t key_sz,
                   hyperclient_returncode* status,
                   struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_get(status, attrs, attrs_sz);
    size_t sz = HYPERCLIENT_HEADER_SIZE
              + sizeof(uint32_t)
              + key_sz;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer p = msg->pack_at(HYPERCLIENT_HEADER_SIZE);
    p = p << e::slice(key, key_sz);
    assert(!p.error());
    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: put(const char* space, const char* key, size_t key_sz,
                   const struct hyperclient_attribute* attrs, size_t attrs_sz,
                   hyperclient_returncode* status)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_PUT, hyperdex::RESP_PUT, status);
    size_t sz = HYPERCLIENT_HEADER_SIZE
              + sizeof(uint32_t)
              + key_sz
              + pack_attrs_sz(attrs, attrs_sz);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer p = msg->pack_at(HYPERCLIENT_HEADER_SIZE);
    p = p << e::slice(key, key_sz);

    int64_t ret = pack_attrs(space, &p, attrs, attrs_sz, status);

    if (ret < 0)
    {
        return ret;
    }

    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: condput(const char* space, const char* key, size_t key_sz,
                       const struct hyperclient_attribute* condattrs, size_t condattrs_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       hyperclient_returncode* status)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_CONDPUT, hyperdex::RESP_CONDPUT, status);
    size_t sz = HYPERCLIENT_HEADER_SIZE
              + sizeof(uint32_t)
              + key_sz
              + pack_attrs_sz(condattrs, condattrs_sz)
              + pack_attrs_sz(attrs, attrs_sz);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer p = msg->pack_at(HYPERCLIENT_HEADER_SIZE);
    p = p << e::slice(key, key_sz);

    int64_t ret = pack_attrs(space, &p, condattrs, condattrs_sz, status);

    if (ret < 0)
    {
        return ret;
    }

    ret = pack_attrs(space, &p, attrs, attrs_sz, status);

    if (ret < 0)
    {
      return ret - condattrs_sz;
    }

    assert(!p.error());
    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: del(const char* space, const char* key, size_t key_sz,
                   hyperclient_returncode* status)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_DEL, hyperdex::RESP_DEL, status);
    size_t sz = HYPERCLIENT_HEADER_SIZE
              + sizeof(uint32_t)
              + key_sz;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer p = msg->pack_at(HYPERCLIENT_HEADER_SIZE);
    p = p << e::slice(key, key_sz);
    assert(!p.error());
    return add_keyop(space, key, key_sz, msg, op);
}

static hyperdatatype
coerce_identity(hyperdatatype, hyperdatatype provided)
{
    return provided;
}

static hyperdatatype
coerce_generic(hyperdatatype expected, hyperdatatype provided)
{
    if (provided == HYPERDATATYPE_LIST_GENERIC)
    {
        if (expected == HYPERDATATYPE_LIST_INT64 ||
            expected == HYPERDATATYPE_LIST_STRING)
        {
            return expected;
        }

        return HYPERDATATYPE_GARBAGE;
    }
    else if (provided == HYPERDATATYPE_SET_GENERIC)
    {
        if (expected == HYPERDATATYPE_SET_INT64 ||
            expected == HYPERDATATYPE_SET_STRING)
        {
            return expected;
        }

        return HYPERDATATYPE_GARBAGE;
    }
    else if (provided == HYPERDATATYPE_MAP_GENERIC)
    {
        if (expected == HYPERDATATYPE_MAP_STRING_STRING ||
            expected == HYPERDATATYPE_MAP_STRING_INT64 ||
            expected == HYPERDATATYPE_MAP_INT64_STRING ||
            expected == HYPERDATATYPE_MAP_INT64_INT64)
        {
            return expected;
        }

        return HYPERDATATYPE_GARBAGE;
    }

    return provided;
}

static hyperdatatype
coerce_list(hyperdatatype, hyperdatatype provided)
{
    if (provided == HYPERDATATYPE_INT64)
    {
        return HYPERDATATYPE_LIST_INT64;
    }

    if (provided == HYPERDATATYPE_STRING)
    {
        return HYPERDATATYPE_LIST_STRING;
    }

    return HYPERDATATYPE_GARBAGE;
}

static hyperdatatype
coerce_set(hyperdatatype, hyperdatatype provided)
{
    if (provided == HYPERDATATYPE_INT64)
    {
        return HYPERDATATYPE_SET_INT64;
    }

    if (provided == HYPERDATATYPE_STRING)
    {
        return HYPERDATATYPE_SET_STRING;
    }

    return HYPERDATATYPE_GARBAGE;
}

static hyperdatatype
coerce_set_generic(hyperdatatype expected, hyperdatatype provided)
{
    if (provided == HYPERDATATYPE_INT64)
    {
        return HYPERDATATYPE_SET_INT64;
    }
    else if (provided == HYPERDATATYPE_STRING)
    {
        return HYPERDATATYPE_SET_STRING;
    }
    else if (provided == HYPERDATATYPE_SET_GENERIC)
    {
        if (expected == HYPERDATATYPE_SET_INT64 ||
            expected == HYPERDATATYPE_SET_STRING)
        {
            return expected;
        }
    }

    return HYPERDATATYPE_GARBAGE;
}

#define HYPERCLIENT_CPPDEF(PREFIX, OPNAME, OPNAMECAPS, CONVERT) \
    int64_t \
    hyperclient :: PREFIX ## _ ## OPNAME(const char* space, const char* key, size_t key_sz, \
                              const struct hyperclient_attribute* attrs, size_t attrs_sz, \
                              enum hyperclient_returncode* status) \
    { \
        return attributes_to_microops(CONVERT, \
                                      hyperdex::OP_ ## OPNAMECAPS, space, \
                                      key, key_sz, attrs, attrs_sz, status); \
    }

HYPERCLIENT_CPPDEF(atomic, add, INT64_ADD, coerce_identity)
HYPERCLIENT_CPPDEF(atomic, sub, INT64_SUB, coerce_identity)
HYPERCLIENT_CPPDEF(atomic, mul, INT64_MUL, coerce_identity)
HYPERCLIENT_CPPDEF(atomic, div, INT64_DIV, coerce_identity)
HYPERCLIENT_CPPDEF(atomic, mod, INT64_MOD, coerce_identity)
HYPERCLIENT_CPPDEF(atomic, and, INT64_AND, coerce_identity)
HYPERCLIENT_CPPDEF(atomic, or, INT64_OR, coerce_identity)
HYPERCLIENT_CPPDEF(atomic, xor, INT64_XOR, coerce_identity)
HYPERCLIENT_CPPDEF(string, prepend, STRING_PREPEND, coerce_identity)
HYPERCLIENT_CPPDEF(string, append, STRING_APPEND, coerce_identity)
HYPERCLIENT_CPPDEF(list, lpush, LIST_LPUSH, coerce_list)
HYPERCLIENT_CPPDEF(list, rpush, LIST_RPUSH, coerce_list)
HYPERCLIENT_CPPDEF(set, add, SET_ADD, coerce_set)
HYPERCLIENT_CPPDEF(set, remove, SET_REMOVE, coerce_set)
HYPERCLIENT_CPPDEF(set, intersect, SET_INTERSECT, coerce_set_generic)
HYPERCLIENT_CPPDEF(set, union, SET_UNION, coerce_set_generic)

static hyperdatatype
coerce_map_remove(hyperdatatype expected, hyperdatatype provided)
{
    if (provided == HYPERDATATYPE_MAP_STRING_KEYONLY &&
        (expected == HYPERDATATYPE_MAP_STRING_STRING ||
         expected == HYPERDATATYPE_MAP_STRING_INT64))
    {
        return expected;
    }

    if (provided == HYPERDATATYPE_MAP_INT64_KEYONLY &&
        (expected == HYPERDATATYPE_MAP_INT64_STRING ||
         expected == HYPERDATATYPE_MAP_INT64_INT64))
    {
        return expected;
    }

    return HYPERDATATYPE_GARBAGE;
}

static hyperdatatype
coerce_numeric_value(hyperdatatype, hyperdatatype provided)
{
    if (provided == HYPERDATATYPE_MAP_STRING_INT64 ||
        provided == HYPERDATATYPE_MAP_INT64_INT64)
    {
        return provided;
    }

    return HYPERDATATYPE_GARBAGE;
}

static hyperdatatype
coerce_string_value(hyperdatatype, hyperdatatype provided)
{
    if (provided == HYPERDATATYPE_MAP_STRING_STRING ||
        provided == HYPERDATATYPE_MAP_INT64_STRING)
    {
        return provided;
    }

    return HYPERDATATYPE_GARBAGE;
}

#define HYPERCLIENT_MAP_CPPDEF(PREFIX, OPNAME, OPNAMECAPS, CONVERT) \
    int64_t \
    hyperclient :: PREFIX ## _ ## OPNAME(const char* space, const char* key, size_t key_sz, \
                              const struct hyperclient_map_attribute* attrs, size_t attrs_sz, \
                              enum hyperclient_returncode* status) \
    { \
        return map_attributes_to_microops(CONVERT, \
                                          hyperdex::OP_ ## OPNAMECAPS, space, \
                                          key, key_sz, attrs, attrs_sz, status); \
    }

HYPERCLIENT_MAP_CPPDEF(map, add, MAP_ADD, coerce_identity)
HYPERCLIENT_MAP_CPPDEF(map, remove, MAP_REMOVE, coerce_map_remove)
HYPERCLIENT_MAP_CPPDEF(map, atomic_add, INT64_ADD, coerce_numeric_value)
HYPERCLIENT_MAP_CPPDEF(map, atomic_sub, INT64_SUB, coerce_numeric_value)
HYPERCLIENT_MAP_CPPDEF(map, atomic_mul, INT64_MUL, coerce_numeric_value)
HYPERCLIENT_MAP_CPPDEF(map, atomic_div, INT64_DIV, coerce_numeric_value)
HYPERCLIENT_MAP_CPPDEF(map, atomic_mod, INT64_MOD, coerce_numeric_value)
HYPERCLIENT_MAP_CPPDEF(map, atomic_and, INT64_AND, coerce_numeric_value)
HYPERCLIENT_MAP_CPPDEF(map, atomic_or, INT64_OR, coerce_numeric_value)
HYPERCLIENT_MAP_CPPDEF(map, atomic_xor, INT64_XOR, coerce_numeric_value)
HYPERCLIENT_MAP_CPPDEF(map, string_prepend, STRING_PREPEND, coerce_string_value)
HYPERCLIENT_MAP_CPPDEF(map, string_append, STRING_APPEND, coerce_string_value)

int64_t
hyperclient :: search(const char* space,
                      const struct hyperclient_attribute* eq, size_t eq_sz,
                      const struct hyperclient_range_query* rn, size_t rn_sz,
                      enum hyperclient_returncode* status,
                      struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    hyperdex::spaceid si = m_config->space(space);

    if (si == hyperdex::spaceid())
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return -1;
    }

    std::vector<hyperdex::attribute> dims = m_config->dimension_names(si);
    assert(dims.size() > 0);
    e::bitfield dims_seen(dims.size());

    // Populate the search object.
    hyperspacehashing::search s(dims.size());

    // Check the equality conditions.
    for (size_t i = 0; i < eq_sz; ++i)
    {
        hyperdatatype coerced = eq[i].datatype;
        int idx = validate_attr(coerce_identity, dims, &dims_seen, eq[i].attr, &coerced, status);

        if (idx < 0)
        {
            return -1 - i;
        }

        assert(coerced == eq[i].datatype);
        s.equality_set(idx, e::slice(eq[i].value, eq[i].value_sz));
    }

    // Check the range conditions.
    for (size_t i = 0; i < rn_sz; ++i)
    {
        hyperdatatype coerced = HYPERDATATYPE_INT64;
        int idx = validate_attr(coerce_identity, dims, &dims_seen, rn[i].attr, &coerced, status);

        if (idx < 0)
        {
            return -1 - eq_sz - i;
        }

        assert(coerced == HYPERDATATYPE_INT64);
        s.range_set(idx, rn[i].lower, rn[i].upper);
    }

    // Get the hosts that match our search terms.
    std::map<hyperdex::entityid, hyperdex::instance> search_entities;
    search_entities = m_config->search_entities(si, s);

    // Send a search query to each matching host.
    int64_t searchid = m_client_id;
    ++m_client_id;

    // Pack the message to send
    std::auto_ptr<e::buffer> msg(e::buffer::create(HYPERCLIENT_HEADER_SIZE + sizeof(uint64_t) + s.packed_size()));
    bool packed = !(msg->pack_at(HYPERCLIENT_HEADER_SIZE) << searchid << s).error();
    assert(packed);
    std::tr1::shared_ptr<uint64_t> refcount(new uint64_t(0));

    for (std::map<hyperdex::entityid, hyperdex::instance>::const_iterator ent_inst = search_entities.begin();
            ent_inst != search_entities.end(); ++ent_inst)
    {
        e::intrusive_ptr<pending> op   = new pending_search(searchid, refcount, status, attrs, attrs_sz);
        op->set_server_visible_nonce(m_server_nonce);
        ++m_server_nonce;
        op->set_entity(ent_inst->first);
        op->set_instance(ent_inst->second);
        m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
        std::auto_ptr<e::buffer> tosend(msg->copy());

        if (send(op, tosend) < 0)
        {
            m_complete.push(completedop(op, HYPERCLIENT_RECONFIGURE, 0));
            m_incomplete.erase(op->server_visible_nonce());
        }
    }

    return searchid;
}

int64_t
hyperclient :: loop(int timeout, hyperclient_returncode* status)
{
    while (!m_incomplete.empty() && m_complete.empty())
    {
        if (maintain_coord_connection(status) < 0)
        {
            return -1;
        }

        po6::net::location sender;
        std::auto_ptr<e::buffer> msg;
        m_busybee->set_timeout(timeout);
        busybee_returncode ret = m_busybee->recv(&sender, &msg);

        switch (ret)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
                *status = HYPERCLIENT_POLLFAILED;
                return -1;
            case BUSYBEE_TIMEOUT:
                *status = HYPERCLIENT_TIMEOUT;
                return -1;
            case BUSYBEE_EXTERNAL:
                // We need to call maintain_coord_connection again
                continue;
            case BUSYBEE_DISCONNECT:
            case BUSYBEE_CONNECTFAIL:
                *status = HYPERCLIENT_SUCCESS;
                killall(sender, HYPERCLIENT_RECONFIGURE);
                m_coord->fail_location(sender);
                continue;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_QUEUED:
            case BUSYBEE_BUFFERFULL:
            default:
                abort();
        }

        uint8_t type_num;
        uint64_t version;
        uint16_t fromver;
        uint16_t tover;
        hyperdex::entityid from;
        hyperdex::entityid to;
        int64_t nonce;
        e::buffer::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        up = up >> type_num >> version >> fromver >> tover >> from >> to >> nonce;

        if (up.error())
        {
            killall(sender, HYPERCLIENT_SERVERERROR);
            continue;
        }

        hyperdex::network_msgtype msg_type = static_cast<hyperdex::network_msgtype>(type_num);

        incomplete_map_t::iterator it = m_incomplete.find(nonce);

        if (it == m_incomplete.end())
        {
            killall(sender, HYPERCLIENT_SERVERERROR);
            continue;
        }

        e::intrusive_ptr<pending> op = it->second;
        assert(op);
        assert(nonce == op->server_visible_nonce());

        if (msg_type == hyperdex::CONFIGMISMATCH)
        {
            op->set_status(HYPERCLIENT_RECONFIGURE);
            m_incomplete.erase(it);
            return op->client_visible_id();
        }

        if (sender.address == op->instance().address &&
            sender.port == op->instance().inbound_port &&
            fromver == op->instance().inbound_version &&
            from == op->entity())
        {
            // Handle response will either successfully finish one event and
            // then return >0, fail many and return 0, or encounter another sort
            // of error and return <0.  In the first case we leave immediately.
            // In the second case, we go back around the loop, and start
            // returning from m_complete.  In the third case, we return
            // immediately.
            m_incomplete.erase(it);
            int64_t id = op->handle_response(this, sender, msg, msg_type, status);

            if (id != 0)
            {
                return id;
            }
        }
        else
        {
            killall(sender, HYPERCLIENT_SERVERERROR);
        }
    }

    if (m_incomplete.empty() && m_complete.empty())
    {
        *status = HYPERCLIENT_NONEPENDING;
        return -1;
    }

    assert(!m_complete.empty());

    completedop c = m_complete.front();
    m_complete.pop();

    *status = HYPERCLIENT_SUCCESS;
    int64_t ret = c.op->client_visible_id();
    c.op->set_status(c.why);

    if (c.error != 0)
    {
        errno = c.error;
    }

    return ret;
}

// XXX If we lose the coord connection, this whole thing fails.
// It would be better if we could stay alive when the coordinator dies (assuming
// the config does not change).
int64_t
hyperclient :: maintain_coord_connection(hyperclient_returncode* status)
{
    if (m_have_seen_config)
    {
        switch (m_coord->poll(1, 0))
        {
            case hyperdex::coordinatorlink::SUCCESS:
                break;
            case hyperdex::coordinatorlink::CONNECTFAIL:
                *status = HYPERCLIENT_COORDFAIL;
                m_old_coord_fd = -1;
                return -1;
            default:
                abort();
        }
    }
    else
    {
        switch (m_coord->poll(5, -1))
        {
            case hyperdex::coordinatorlink::SUCCESS:
                break;
            case hyperdex::coordinatorlink::CONNECTFAIL:
                *status = HYPERCLIENT_COORDFAIL;
                m_old_coord_fd = -1;
                return -1;
            default:
                abort();
        }
    }

    if (m_old_coord_fd != m_coord->poll_on())
    {
        if (m_busybee->add_external_fd(m_coord->poll_on(), BUSYBEE_E_IN) < 0 &&
            errno != EEXIST)
        {
            *status = HYPERCLIENT_COORDFAIL;
            m_old_coord_fd = -1;
            return -1;
        }
    }

    int64_t reconfigured = 0;

    if (m_coord->unacknowledged())
    {
        *m_config = m_coord->config();
        incomplete_map_t::iterator i = m_incomplete.begin();

        while (i != m_incomplete.end())
        {
            // If the mapping that was true when the operation started is no
            // longer true, we just abort the operation.
            if (m_config->instancefor(i->second->entity()) != i->second->instance())
            {
                m_complete.push(completedop(i->second, HYPERCLIENT_RECONFIGURE, 0));
                m_incomplete.erase(i);
                i = m_incomplete.begin();
                ++reconfigured;
            }
            else
            {
                ++i;
            }
        }

        m_coord->acknowledge();
        m_have_seen_config = true;
    }

    return reconfigured;
}

int64_t
hyperclient :: add_keyop(const char* space, const char* key, size_t key_sz,
                         std::auto_ptr<e::buffer> msg, e::intrusive_ptr<pending> op)
{
    hyperdex::spaceid si = m_config->space(space);

    if (si == hyperdex::spaceid())
    {
        op->set_status(HYPERCLIENT_UNKNOWNSPACE);
        return -1;
    }

    // Figure out who to talk with.
    hyperdex::entityid dst_ent;
    hyperdex::instance dst_inst;

    // XXX We could postpone this, and then send it out on the next
    // reconfiguration, assuming the configuration allows us to connect.
    if (!m_config->point_leader_entity(si, e::slice(key, key_sz), &dst_ent, &dst_inst))
    {
        op->set_status(HYPERCLIENT_RECONFIGURE);
        return -1;
    }

    op->set_server_visible_nonce(m_server_nonce);
    ++m_server_nonce;
    op->set_entity(dst_ent);
    op->set_instance(dst_inst);
    m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
    typedef size_t (incomplete_map_t::*functype)(const int64_t&);
    functype f = &incomplete_map_t::erase;
    e::guard remove_op = e::makeobjguard(m_incomplete, f, op->server_visible_nonce());
    e::guard fail_op = e::makeobjguard(*op, &pending::set_status, HYPERCLIENT_RECONFIGURE);
    int64_t ret = send(op, msg);
    assert(ret <= 0);

    // We dismiss the remove_op guard when the operation succeeds within send.
    if (ret == 0)
    {
        remove_op.dismiss();
        op->set_client_visible_id(m_client_id);
        ret = op->client_visible_id();
        ++m_client_id;
    }

    // We dismiss the fail_op guard no matter what because if ret < 0, then send
    // already set the status to a more-detailed reason.
    fail_op.dismiss();
    return ret;
}

int64_t
hyperclient :: send(e::intrusive_ptr<pending> op,
                    std::auto_ptr<e::buffer> msg)
{
    const uint8_t type = static_cast<uint8_t>(op->request_type());
    const uint64_t version = m_config->version();
    const uint16_t fromver = 1;
    const uint16_t tover = op->instance().inbound_version;
    const hyperdex::entityid from(hyperdex::configuration::CLIENTSPACE, 0, 0, 0, 0);
    const hyperdex::entityid& to(op->entity());
    const uint64_t nonce = op->server_visible_nonce();
    e::buffer::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
    pa = pa << type << version << fromver << tover << from << to << nonce;
    assert(!pa.error());
    po6::net::location dest(op->instance().address, op->instance().inbound_port);

    switch (m_busybee->send(dest, msg))
    {
        case BUSYBEE_SUCCESS:
        case BUSYBEE_QUEUED:
            return 0;
        case BUSYBEE_DISCONNECT:
        case BUSYBEE_CONNECTFAIL:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_BUFFERFULL:
            killall(dest, HYPERCLIENT_RECONFIGURE);
            m_coord->fail_location(dest);
            return -1;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        default:
            abort();
    }
}

int64_t
hyperclient :: pack_attrs(const char* space, e::buffer::packer* p,
                          const struct hyperclient_attribute* attrs,
                          size_t attrs_sz, hyperclient_returncode* status)
{
    hyperdex::spaceid si = m_config->space(space);

    if (si == hyperdex::spaceid())
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return -1;
    }

    std::vector<hyperdex::attribute> dims = m_config->dimension_names(si);
    assert(dims.size() > 0);
    e::bitfield dims_seen(dims.size());
    uint32_t sz = attrs_sz;
    *p = *p << sz;

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        hyperdatatype coerced = attrs[i].datatype;
        int idx = validate_attr(coerce_generic, dims, &dims_seen, attrs[i].attr, &coerced, status);

        if (idx < 0)
        {
            return -1 - i;
        }

        *p = *p << static_cast<uint16_t>(idx)
                << e::slice(attrs[i].value, attrs[i].value_sz);
    }

    assert(!(*p).error());
    return 0;
}

size_t
hyperclient :: pack_attrs_sz(const struct hyperclient_attribute* attrs,
                             size_t attrs_sz)
{
    size_t sz = sizeof(uint32_t) + attrs_sz * (sizeof(uint16_t) + sizeof(uint32_t));

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        sz += attrs[i].value_sz;
    }

    return sz;
}

void
hyperclient :: killall(const po6::net::location& loc,
                       hyperclient_returncode status)
{
    incomplete_map_t::iterator r = m_incomplete.begin();

    while (r != m_incomplete.end())
    {
        if (r->second->instance().address == loc.address &&
            r->second->instance().inbound_port == loc.port)
        {
            m_complete.push(completedop(r->second, status, 0));
            m_incomplete.erase(r);
            r = m_incomplete.begin();
        }
        else
        {
            ++r;
        }
    }

    m_busybee->drop(loc);
}

int64_t
hyperclient :: attributes_to_microops(hyperdatatype (*coerce_datatype)(hyperdatatype e, hyperdatatype p),
                                      int action, const char* space,
                                      const char* key, size_t key_sz,
                                      const struct hyperclient_attribute* attrs,
                                      size_t attrs_sz,
                                      hyperclient_returncode* status)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    // A new pending op
    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_ATOMIC, hyperdex::RESP_ATOMIC, status);
    // The size of the buffer we need
    size_t sz = HYPERCLIENT_HEADER_SIZE
              + sizeof(uint32_t)
              + key_sz
              + sizeof(uint32_t)
              + MICROOP_BASE_SIZE * attrs_sz;

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        sz += attrs[i].value_sz;
    }

    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer p = msg->pack_at(HYPERCLIENT_HEADER_SIZE);
    p = p << e::slice(key, key_sz) << static_cast<uint32_t>(attrs_sz);

    hyperdex::spaceid si = m_config->space(space);

    if (si == hyperdex::spaceid())
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return -1;
    }

    std::vector<hyperdex::attribute> dims = m_config->dimension_names(si);
    assert(dims.size() > 0);
    std::vector<hyperdex::microop> ops;
    ops.reserve(attrs_sz);

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        hyperdatatype coerced = attrs[i].datatype;
        int idx = validate_attr(coerce_datatype, dims, NULL, attrs[i].attr, &coerced, status);

        if (idx < 0)
        {
            return -1 - i;
        }

        hyperdex::microop o;
        o.attr = idx;
        o.action = static_cast<hyperdex::microop_type>(action);
        o.type = coerced;
        o.arg1 = e::slice(attrs[i].value, attrs[i].value_sz);
        ops.push_back(o);
    }

    std::sort(ops.begin(), ops.end(), compare_for_microop_sort);

    for (size_t i = 0; i < ops.size(); ++i)
    {
        p = p << ops[i];
    }

    assert(!p.error());
    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: map_attributes_to_microops(hyperdatatype (*coerce_datatype)(hyperdatatype e, hyperdatatype p),
                                          int action, const char* space,
                                          const char* key, size_t key_sz,
                                          const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                                          enum hyperclient_returncode* status)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    // A new pending op
    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_ATOMIC, hyperdex::RESP_ATOMIC, status);
    // The size of the buffer we need
    size_t sz = HYPERCLIENT_HEADER_SIZE
              + sizeof(uint32_t)
              + key_sz
              + sizeof(uint32_t)
              + MICROOP_BASE_SIZE * attrs_sz;

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        sz += attrs[i].map_key_sz;
        sz += attrs[i].value_sz;
    }

    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer p = msg->pack_at(HYPERCLIENT_HEADER_SIZE);
    p = p << e::slice(key, key_sz) << static_cast<uint32_t>(attrs_sz);
    hyperdex::spaceid si = m_config->space(space);

    if (si == hyperdex::spaceid())
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return -1;
    }

    std::vector<hyperdex::attribute> dims = m_config->dimension_names(si);
    assert(dims.size() > 0);
    std::vector<hyperdex::microop> ops;
    ops.reserve(attrs_sz);

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        hyperdatatype coerced = attrs[i].datatype;
        int idx = validate_attr(coerce_datatype, dims, NULL, attrs[i].attr, &coerced, status);

        if (idx < 0)
        {
            return -1 - i;
        }

        hyperdex::microop o;
        o.attr = idx;
        o.action = static_cast<hyperdex::microop_type>(action);
        o.type = coerced;
        o.arg2 = e::slice(attrs[i].map_key, attrs[i].map_key_sz);
        o.arg1 = e::slice(attrs[i].value, attrs[i].value_sz);
        ops.push_back(o);
    }

    std::sort(ops.begin(), ops.end(), compare_for_microop_sort);

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        p = p << ops[i];
    }

    assert(!p.error());
    return add_keyop(space, key, key_sz, msg, op);
}

int
hyperclient :: validate_attr(hyperdatatype (*coerce_datatype)(hyperdatatype e, hyperdatatype p),
                             const std::vector<hyperdex::attribute>& dimension_names,
                             e::bitfield* dimensions_seen,
                             const char* attr,
                             hyperdatatype* type,
                             hyperclient_returncode* status)
{
    std::vector<hyperdex::attribute>::const_iterator dim;
    dim = dimension_names.begin();

    while (dim < dimension_names.end() && dim->name != attr)
    {
        ++dim;
    }

    if (dim == dimension_names.begin())
    {
        *status = HYPERCLIENT_DONTUSEKEY;
        return -1;
    }

    if (dim == dimension_names.end())
    {
        *status = HYPERCLIENT_UNKNOWNATTR;
        return -1;
    }

    uint16_t dimnum = dim - dimension_names.begin();

    if (dimensions_seen && dimensions_seen->get(dimnum))
    {
        *status = HYPERCLIENT_DUPEATTR;
        return -1;
    }

    if (coerce_datatype(dim->type, *type) != dim->type)
    {
        *status = HYPERCLIENT_WRONGTYPE;
        return -1;
    }

    *type = dim->type;

    if (dimensions_seen)
    {
        dimensions_seen->set(dimnum);
    }

    return dimnum;
}

#define str(x) #x
#define xstr(x) str(x)
#define stringify(x) case (x): lhs << xstr(x); break

std::ostream&
operator << (std::ostream& lhs, hyperclient_returncode rhs)
{
    switch (rhs)
    {
        stringify(HYPERCLIENT_SUCCESS);
        stringify(HYPERCLIENT_NOTFOUND);
        stringify(HYPERCLIENT_SEARCHDONE);
        stringify(HYPERCLIENT_CMPFAIL);
        stringify(HYPERCLIENT_READONLY);
        stringify(HYPERCLIENT_UNKNOWNSPACE);
        stringify(HYPERCLIENT_COORDFAIL);
        stringify(HYPERCLIENT_SERVERERROR);
        stringify(HYPERCLIENT_POLLFAILED);
        stringify(HYPERCLIENT_OVERFLOW);
        stringify(HYPERCLIENT_RECONFIGURE);
        stringify(HYPERCLIENT_TIMEOUT);
        stringify(HYPERCLIENT_UNKNOWNATTR);
        stringify(HYPERCLIENT_DUPEATTR);
        stringify(HYPERCLIENT_NONEPENDING);
        stringify(HYPERCLIENT_DONTUSEKEY);
        stringify(HYPERCLIENT_WRONGTYPE);
        stringify(HYPERCLIENT_NOMEM);
        stringify(HYPERCLIENT_EXCEPTION);
        stringify(HYPERCLIENT_ZERO);
        stringify(HYPERCLIENT_A);
        stringify(HYPERCLIENT_B);
        default:
            lhs << "unknown returncode";
            break;
    }

    return lhs;
}

#undef stringify
#undef xstr
#undef str
