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
#include "macros.h"
#include "datatypes/coercion.h"
#include "datatypes/schema.h"
#include "datatypes/microcheck.h"
#include "datatypes/micropredicate.h"
#include "datatypes/microop.h"
#include "datatypes/validate.h"
#include "hyperdex/hyperdex/configuration.h"
#include "hyperdex/hyperdex/coordinatorlink.h"

// HyperClient
#include "hyperclient/constants.h"
#include "hyperclient/hyperclient.h"
#include "hyperclient/hyperclient_completedop.h"
#include "hyperclient/hyperclient_pending.h"
#include "hyperclient/hyperclient_pending_count.h"
#include "hyperclient/hyperclient_pending_get.h"
#include "hyperclient/hyperclient_pending_group_del.h"
#include "hyperclient/hyperclient_pending_search.h"
#include "hyperclient/hyperclient_pending_sorted_search.h"
#include "hyperclient/hyperclient_pending_statusonly.h"
#include "hyperclient/keyop_info.h"
#include "hyperclient/util.h"

// Macros for convenience.  These conditional blocks appear quite a lot.  I want
// to make them easy to change.
#define MAINTAIN_COORD_CONNECTION(STATUS) \
    if (maintain_coord_connection(STATUS) < 0) \
    { \
        return -1; \
    }
#define VALIDATE_KEY(SCHEMA, KEY, KEY_SZ) \
    if (!SCHEMA) \
    { \
        *status = HYPERCLIENT_UNKNOWNSPACE; \
        return -1; \
    } \
    if (!validate_as_type(e::slice((KEY), (KEY_SZ)), (SCHEMA)->attrs[0].type)) \
    { \
        *status = HYPERCLIENT_WRONGTYPE; \
        return -1; \
    }

hyperclient :: hyperclient(const char* coordinator, in_port_t port)
    : m_coord(new hyperdex::coordinatorlink(po6::net::hostname(coordinator, port)))
    , m_config(new hyperdex::configuration())
    , m_busybee(new busybee_st())
    , m_incomplete()
    , m_complete_succeeded()
    , m_complete_failed()
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
    MAINTAIN_COORD_CONNECTION(status)
    schema* sc = m_config->get_schema(space);
    VALIDATE_KEY(sc, key, key_sz) // Checks sc

    e::intrusive_ptr<pending> op = new pending_get(status, attrs, attrs_sz);

    size_t sz = HYPERCLIENT_HEADER_SIZE + sizeof(uint32_t) + key_sz;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE) << e::slice(key, key_sz);
    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: condput(const char* space, const char* key, size_t key_sz,
                       const struct hyperclient_attribute* condattrs, size_t condattrs_sz,
                       const struct hyperclient_attribute* attrs, size_t attrs_sz,
                       hyperclient_returncode* status)
{
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup("condput", 7);
    return perform_microop1(opinfo, space, key, key_sz,
                            condattrs, condattrs_sz, attrs, attrs_sz, status);
}

int64_t
hyperclient :: del(const char* space, const char* key, size_t key_sz,
                   hyperclient_returncode* status)
{
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup("del", 3);
    return perform_microop1(opinfo, space, key, key_sz, NULL, 0, NULL, 0, status);
}

#define HYPERCLIENT_CPPDEF(OPNAME) \
    int64_t \
    hyperclient :: OPNAME(const char* space, const char* key, size_t key_sz, \
                          const struct hyperclient_attribute* attrs, size_t attrs_sz, \
                          enum hyperclient_returncode* status) \
    { \
        const hyperclient_keyop_info* opinfo; \
        opinfo = hyperclient_keyop_info_lookup(hdxstr(OPNAME), strlen(hdxstr(OPNAME))); \
        return perform_microop1(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, status); \
    }

HYPERCLIENT_CPPDEF(put)
HYPERCLIENT_CPPDEF(put_if_not_exist)
HYPERCLIENT_CPPDEF(atomic_add)
HYPERCLIENT_CPPDEF(atomic_sub)
HYPERCLIENT_CPPDEF(atomic_mul)
HYPERCLIENT_CPPDEF(atomic_div)
HYPERCLIENT_CPPDEF(atomic_mod)
HYPERCLIENT_CPPDEF(atomic_and)
HYPERCLIENT_CPPDEF(atomic_or)
HYPERCLIENT_CPPDEF(atomic_xor)
HYPERCLIENT_CPPDEF(string_prepend)
HYPERCLIENT_CPPDEF(string_append)
HYPERCLIENT_CPPDEF(list_lpush)
HYPERCLIENT_CPPDEF(list_rpush)
HYPERCLIENT_CPPDEF(set_add)
HYPERCLIENT_CPPDEF(set_remove)
HYPERCLIENT_CPPDEF(set_intersect)
HYPERCLIENT_CPPDEF(set_union)

#define HYPERCLIENT_MAP_CPPDEF(OPNAME) \
    int64_t \
    hyperclient :: OPNAME(const char* space, const char* key, size_t key_sz, \
                          const struct hyperclient_map_attribute* attrs, size_t attrs_sz, \
                          enum hyperclient_returncode* status) \
    { \
        const hyperclient_keyop_info* opinfo; \
        opinfo = hyperclient_keyop_info_lookup(hdxstr(OPNAME), strlen(hdxstr(OPNAME))); \
        return perform_microop2(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, status); \
    }

HYPERCLIENT_MAP_CPPDEF(map_add)
HYPERCLIENT_MAP_CPPDEF(map_remove)
HYPERCLIENT_MAP_CPPDEF(map_atomic_add)
HYPERCLIENT_MAP_CPPDEF(map_atomic_sub)
HYPERCLIENT_MAP_CPPDEF(map_atomic_mul)
HYPERCLIENT_MAP_CPPDEF(map_atomic_div)
HYPERCLIENT_MAP_CPPDEF(map_atomic_mod)
HYPERCLIENT_MAP_CPPDEF(map_atomic_and)
HYPERCLIENT_MAP_CPPDEF(map_atomic_or)
HYPERCLIENT_MAP_CPPDEF(map_atomic_xor)
HYPERCLIENT_MAP_CPPDEF(map_string_prepend)
HYPERCLIENT_MAP_CPPDEF(map_string_append)

int64_t
hyperclient :: search(const char* space,
                      const struct hyperclient_attribute* eq, size_t eq_sz,
                      const struct hyperclient_range_query* rn, size_t rn_sz,
                      enum hyperclient_returncode* status,
                      struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    MAINTAIN_COORD_CONNECTION(status)

    // Figure out who to contact for the search.
    hyperspacehashing::search s;
    std::map<hyperdex::entityid, hyperdex::instance> search_entities;
    int64_t ret = prepare_searchop(space, eq, eq_sz, rn, rn_sz, NULL, status, &s, &search_entities, NULL, NULL);

    if (ret < 0)
    {
        return ret;
    }

    // Send a search query to each matching host.
    int64_t searchid = m_client_id;
    ++m_client_id;

    // Pack the message to send
    std::auto_ptr<e::buffer> msg(e::buffer::create(HYPERCLIENT_HEADER_SIZE + sizeof(uint64_t) + s.packed_size()));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE) << searchid << s;
    std::tr1::shared_ptr<uint64_t> refcount(new uint64_t(0));

    for (std::map<hyperdex::entityid, hyperdex::instance>::const_iterator ent_inst = search_entities.begin();
            ent_inst != search_entities.end(); ++ent_inst)
    {
        e::intrusive_ptr<pending> op = new pending_search(searchid, refcount, status, attrs, attrs_sz);
        op->set_server_visible_nonce(m_server_nonce);
        ++m_server_nonce;
        op->set_entity(ent_inst->first);
        op->set_instance(ent_inst->second);
        m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
        std::auto_ptr<e::buffer> tosend(msg->copy());

        if (send(op, tosend) < 0)
        {
            m_complete_failed.push(completedop(op, HYPERCLIENT_RECONFIGURE, 0));
            m_incomplete.erase(op->server_visible_nonce());
        }
    }

    return searchid;
}

static void
delete_bracket_auto_ptr(std::auto_ptr<e::buffer>* ptrs)
{
    delete[] ptrs;
}

int64_t
hyperclient :: sorted_search(const char* space,
                             const struct hyperclient_attribute* eq, size_t eq_sz,
                             const struct hyperclient_range_query* rn, size_t rn_sz,
                             const char* sort_by,
                             uint64_t limit,
                             bool maximize,
                             enum hyperclient_returncode* status,
                             struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    MAINTAIN_COORD_CONNECTION(status)

    // Figure out who to contact for the group_del.
    hyperspacehashing::search s;
    std::map<hyperdex::entityid, hyperdex::instance> entities;
    uint16_t attrno;
    hyperdatatype attrtype;
    int64_t ret = prepare_searchop(space, eq, eq_sz, rn, rn_sz, sort_by, status, &s, &entities, &attrno, &attrtype);

    if (ret < 0)
    {
        return ret;
    }

    if (attrtype != HYPERDATATYPE_STRING && attrtype != HYPERDATATYPE_INT64)
    {
        *status = HYPERCLIENT_WRONGTYPE;
        return -1 - eq_sz - rn_sz;
    }

    // Send a sorted_search query to each matching host.
    int64_t searchid = m_client_id;
    ++m_client_id;

    // Pack the message to send
    size_t sz = HYPERCLIENT_HEADER_SIZE
              + s.packed_size()
              + sizeof(uint64_t)
              + sizeof(uint16_t)
              + sizeof(uint8_t);
    int8_t max = maximize ? 1 : 0;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE) << s << limit << attrno << max;
    std::auto_ptr<e::buffer>* backings = new std::auto_ptr<e::buffer>[entities.size()];
    e::guard g = e::makeguard(delete_bracket_auto_ptr, backings);
    e::intrusive_ptr<pending_sorted_search::state> state;
    state = new pending_sorted_search::state(backings, limit, attrno, attrtype, maximize);
    g.dismiss();

    for (std::map<hyperdex::entityid, hyperdex::instance>::const_iterator ent_inst = entities.begin();
            ent_inst != entities.end(); ++ent_inst)
    {
        e::intrusive_ptr<pending> op = new pending_sorted_search(searchid, state, status, attrs, attrs_sz);
        op->set_server_visible_nonce(m_server_nonce);
        ++m_server_nonce;
        op->set_entity(ent_inst->first);
        op->set_instance(ent_inst->second);
        m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
        std::auto_ptr<e::buffer> tosend(msg->copy());

        if (send(op, tosend) < 0)
        {
            m_complete_failed.push(completedop(op, HYPERCLIENT_RECONFIGURE, 0));
            m_incomplete.erase(op->server_visible_nonce());
        }
    }

    return searchid;
}

int64_t
hyperclient :: group_del(const char* space,
                         const struct hyperclient_attribute* eq, size_t eq_sz,
                         const struct hyperclient_range_query* rn, size_t rn_sz,
                         enum hyperclient_returncode* status)
{
    MAINTAIN_COORD_CONNECTION(status)

    // Figure out who to contact for the group_del.
    hyperspacehashing::search s;
    std::map<hyperdex::entityid, hyperdex::instance> entities;
    int64_t ret = prepare_searchop(space, eq, eq_sz, rn, rn_sz, NULL, status, &s, &entities, NULL, NULL);

    if (ret < 0)
    {
        return ret;
    }

    // Send a group_del query to each matching host.
    int64_t group_del_id = m_client_id;
    ++m_client_id;

    // Pack the message to send
    std::auto_ptr<e::buffer> msg(e::buffer::create(HYPERCLIENT_HEADER_SIZE + s.packed_size()));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE) << s;
    std::tr1::shared_ptr<uint64_t> refcount(new uint64_t(0));

    for (std::map<hyperdex::entityid, hyperdex::instance>::const_iterator ent_inst = entities.begin();
            ent_inst != entities.end(); ++ent_inst)
    {
        e::intrusive_ptr<pending> op = new pending_group_del(group_del_id, refcount, status);
        op->set_server_visible_nonce(m_server_nonce);
        ++m_server_nonce;
        op->set_entity(ent_inst->first);
        op->set_instance(ent_inst->second);
        m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
        std::auto_ptr<e::buffer> tosend(msg->copy());

        if (send(op, tosend) < 0)
        {
            m_complete_failed.push(completedop(op, HYPERCLIENT_RECONFIGURE, 0));
            m_incomplete.erase(op->server_visible_nonce());
        }
    }

    return group_del_id;
}

int64_t
hyperclient :: count(const char* space,
                     const struct hyperclient_attribute* eq, size_t eq_sz,
                     const struct hyperclient_range_query* rn, size_t rn_sz,
                     enum hyperclient_returncode* status,
                     uint64_t* result)
{
    // Must do this first so that client can use it to determine if anything
    // happened.
    *result = 0;

    MAINTAIN_COORD_CONNECTION(status)

    // Figure out who to contact for the count.
    hyperspacehashing::search s;
    std::map<hyperdex::entityid, hyperdex::instance> entities;
    int64_t ret = prepare_searchop(space, eq, eq_sz, rn, rn_sz, NULL, status, &s, &entities, NULL, NULL);

    if (ret < 0)
    {
        return ret;
    }

    // Send a count query to each matching host.
    int64_t count_id = m_client_id;
    ++m_client_id;

    // Pack the message to send
    std::auto_ptr<e::buffer> msg(e::buffer::create(HYPERCLIENT_HEADER_SIZE + s.packed_size()));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE) << s;
    std::tr1::shared_ptr<uint64_t> refcount(new uint64_t(0));

    for (std::map<hyperdex::entityid, hyperdex::instance>::const_iterator ent_inst = entities.begin();
            ent_inst != entities.end(); ++ent_inst)
    {
        e::intrusive_ptr<pending> op = new pending_count(count_id, refcount, status, result);
        op->set_server_visible_nonce(m_server_nonce);
        ++m_server_nonce;
        op->set_entity(ent_inst->first);
        op->set_instance(ent_inst->second);
        m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
        std::auto_ptr<e::buffer> tosend(msg->copy());

        if (send(op, tosend) < 0)
        {
            m_complete_failed.push(completedop(op, HYPERCLIENT_RECONFIGURE, 0));
            m_incomplete.erase(op->server_visible_nonce());
        }
    }

    *status = HYPERCLIENT_SUCCESS;
    return count_id;
}

int64_t
hyperclient :: loop(int timeout, hyperclient_returncode* status)
{
    while (!m_incomplete.empty() && m_complete_failed.empty() &&
           m_complete_succeeded.empty())
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

    if (!m_complete_succeeded.empty())
    {
        int64_t nonce = m_complete_succeeded.front();
        m_complete_succeeded.pop();
        incomplete_map_t::iterator it = m_incomplete.find(nonce);
        assert(it != m_incomplete.end());
        e::intrusive_ptr<pending> op = it->second;
        m_incomplete.erase(it);
        *status = HYPERCLIENT_SUCCESS;
        return op->return_one(this, status);
    }

    if (!m_complete_failed.empty())
    {
        completedop c = m_complete_failed.front();
        m_complete_failed.pop();
        c.op->set_status(c.why);

        if (c.error != 0)
        {
            errno = c.error;
        }

        *status = HYPERCLIENT_SUCCESS;
        return c.op->client_visible_id();
    }

    if (m_incomplete.empty())
    {
        *status = HYPERCLIENT_NONEPENDING;
        return -1;
    }

    abort();
}

enum hyperdatatype
hyperclient :: attribute_type(const char* space, const char* name,
                              enum hyperclient_returncode* status)
{
    if (maintain_coord_connection(status) < 0)
    {
        return HYPERDATATYPE_GARBAGE;
    }

    schema* sc = m_config->get_schema(space);

    if (!sc)
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return HYPERDATATYPE_GARBAGE;
    }

    uint16_t attrnum = sc->lookup_attr(name);

    if (attrnum == sc->attrs_sz)
    {
        *status = HYPERCLIENT_UNKNOWNATTR;
        return HYPERDATATYPE_GARBAGE;
    }

    return sc->attrs[attrnum].type;
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
                m_complete_failed.push(completedop(i->second, HYPERCLIENT_RECONFIGURE, 0));
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
hyperclient :: perform_microop1(const class hyperclient_keyop_info* opinfo,
                                const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute* condattrs, size_t condattrs_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
{
    MAINTAIN_COORD_CONNECTION(status)
    schema* sc = m_config->get_schema(space);
    VALIDATE_KEY(sc, key, key_sz) // Checks sc

    // A new pending op
    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_ATOMIC, hyperdex::RESP_ATOMIC, status);

    // Prepare the checks
    std::vector<microcheck> checks;
    size_t num_checks = prepare_checks(sc, opinfo, condattrs, condattrs_sz, status, &checks);

    if (num_checks < condattrs_sz)
    {
        return -2 - num_checks;
    }

    // Prepare the ops
    std::vector<microop> ops;
    size_t num_ops = prepare_ops(sc, opinfo, attrs, attrs_sz, status, &ops);

    if (num_ops < attrs_sz)
    {
        return -2 - condattrs_sz - num_ops;
    }

    // The size of the buffer we need
    size_t sz = HYPERCLIENT_HEADER_SIZE
              + sizeof(uint8_t)
              + sizeof(uint32_t) + key_sz
              + sizeof(uint32_t) + sizeof(uint32_t);

    for (size_t i = 0; i < checks.size(); ++i)
    {
        sz += pack_size(checks[i]);
    }

    for (size_t i = 0; i < ops.size(); ++i)
    {
        sz += pack_size(ops[i]);
    }

    std::sort(checks.begin(), checks.end());
    std::sort(ops.begin(), ops.end());
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    uint8_t flags = (opinfo->fail_if_not_exist ? 1 : 0)
                  | (opinfo->fail_if_exist ? 2 : 0)
                  | (opinfo->has_microops ? 128 : 0);
    msg->pack_at(HYPERCLIENT_HEADER_SIZE) << e::slice(key, key_sz) << flags << checks << ops;
    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: perform_microop2(const class hyperclient_keyop_info* opinfo,
                                const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute* condattrs, size_t condattrs_sz,
                                const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
{
    MAINTAIN_COORD_CONNECTION(status)
    schema* sc = m_config->get_schema(space);
    VALIDATE_KEY(sc, key, key_sz) // Checks sc

    // A new pending op
    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_ATOMIC, hyperdex::RESP_ATOMIC, status);

    // Prepare the checks
    std::vector<microcheck> checks;
    size_t num_checks = prepare_checks(sc, opinfo, condattrs, condattrs_sz, status, &checks);

    if (num_checks < condattrs_sz)
    {
        return -2 - num_checks;
    }

    // Prepare the ops
    std::vector<microop> ops;
    size_t num_ops = prepare_ops(sc, opinfo, attrs, attrs_sz, status, &ops);

    if (num_ops < attrs_sz)
    {
        return -2 - condattrs_sz - num_ops;
    }

    // The size of the buffer we need
    size_t sz = HYPERCLIENT_HEADER_SIZE
              + sizeof(uint8_t)
              + sizeof(uint32_t) + key_sz
              + sizeof(uint32_t) + sizeof(uint32_t);

    for (size_t i = 0; i < checks.size(); ++i)
    {
        sz += pack_size(checks[i]);
    }

    for (size_t i = 0; i < ops.size(); ++i)
    {
        sz += pack_size(ops[i]);
    }

    std::sort(checks.begin(), checks.end());
    std::sort(ops.begin(), ops.end());
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    uint8_t flags = (opinfo->fail_if_not_exist ? 1 : 0)
                  | (opinfo->has_microops ? 128 : 0);
    msg->pack_at(HYPERCLIENT_HEADER_SIZE) << e::slice(key, key_sz) << flags << checks << ops;
    return add_keyop(space, key, key_sz, msg, op);
}

size_t
hyperclient :: prepare_checks(const class schema* sc,
                              const class hyperclient_keyop_info*,
                              const hyperclient_attribute* condattrs, size_t condattrs_sz,
                              hyperclient_returncode* status,
                              std::vector<class microcheck>* checks)
{
    checks->reserve(condattrs_sz);

    for (size_t i = 0; i < condattrs_sz; ++i)
    {
        uint16_t attrnum = sc->lookup_attr(condattrs[i].attr);

        if (attrnum == sc->attrs_sz)
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return i;
        }

        if (!container_implicit_coercion(sc->attrs[attrnum].type,
                                         e::slice(condattrs[i].value, condattrs[i].value_sz),
                                         condattrs[i].datatype))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return i;
        }

        microcheck c;
        c.attr = attrnum;
        c.value = e::slice(condattrs[i].value, condattrs[i].value_sz);
        c.datatype = condattrs[i].datatype;
        c.predicate = PRED_EQUALS;
        checks->push_back(c);
    }

    return condattrs_sz;
}

size_t
hyperclient :: prepare_ops(const class schema* sc,
                           const class hyperclient_keyop_info* opinfo,
                           const hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status,
                           std::vector<class microop>* ops)
{
    ops->reserve(attrs_sz);

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        uint16_t attrnum = sc->lookup_attr(attrs[i].attr);

        if (attrnum == sc->attrs_sz)
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return i;
        }

        if (attrnum == 0)
        {
            *status = HYPERCLIENT_DONTUSEKEY;
            return i;
        }

        if (!opinfo->check(sc->attrs[attrnum].type,
                           e::slice(attrs[i].value, attrs[i].value_sz), attrs[i].datatype,
                           e::slice(), HYPERDATATYPE_GARBAGE))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return i;
        }

        microop o;
        o.attr = attrnum;
        o.action = opinfo->action;
        o.arg1 = e::slice(attrs[i].value, attrs[i].value_sz);
        o.arg1_datatype = attrs[i].datatype;
        ops->push_back(o);
    }

    return attrs_sz;
}

size_t
hyperclient :: prepare_ops(const class schema* sc,
                           const class hyperclient_keyop_info* opinfo,
                           const hyperclient_map_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status,
                           std::vector<class microop>* ops)
{
    ops->reserve(attrs_sz);

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        uint16_t attrnum = sc->lookup_attr(attrs[i].attr);

        if (attrnum == sc->attrs_sz)
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return i;
        }

        if (attrnum == 0)
        {
            *status = HYPERCLIENT_DONTUSEKEY;
            return i;
        }

        if (!opinfo->check(sc->attrs[attrnum].type,
                           e::slice(attrs[i].value, attrs[i].value_sz), attrs[i].value_datatype,
                           e::slice(attrs[i].map_key, attrs[i].map_key_sz), attrs[i].map_key_datatype))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return i;
        }

        microop o;
        o.attr = attrnum;
        o.action = opinfo->action;
        o.arg2 = e::slice(attrs[i].map_key, attrs[i].map_key_sz);
        o.arg2_datatype = attrs[i].map_key_datatype;
        o.arg1 = e::slice(attrs[i].value, attrs[i].value_sz);
        o.arg1_datatype = attrs[i].value_datatype;
        ops->push_back(o);
    }

    return attrs_sz;
}

int64_t
hyperclient :: prepare_searchop(const char* space,
                                const struct hyperclient_attribute* eq, size_t eq_sz,
                                const struct hyperclient_range_query* rn, size_t rn_sz,
                                const char* attr, /* optional */
                                hyperclient_returncode* status,
                                hyperspacehashing::search* s,
                                std::map<hyperdex::entityid, hyperdex::instance>* search_entities,
                                uint16_t* attrno,
                                hyperdatatype* attrtype)
{
    schema* sc = m_config->get_schema(space);
    *s = hyperspacehashing::search(sc->attrs_sz);

    if (!sc)
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return -1;
    }

    e::bitfield seen(sc->attrs_sz);

    // Check the equality conditions.
    for (size_t i = 0; i < eq_sz; ++i)
    {
        uint16_t attrnum = validate_attribute(sc, eq + i, status);

        if (attrnum == sc->attrs_sz)
        {
            return -1 - i;
        }

        if (seen.get(attrnum))
        {
            *status = HYPERCLIENT_DUPEATTR;
            return -1 - i;
        }

        seen.set(attrnum);
        s->equality_set(attrnum, e::slice(eq[i].value, eq[i].value_sz));
    }

    // Check the range conditions.
    for (size_t i = 0; i < rn_sz; ++i)
    {
        uint16_t attrnum = sc->lookup_attr(rn[i].attr);

        if (attrnum == sc->attrs_sz)
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return -1 - eq_sz - i;
        }

        if (attrnum == 0)
        {
            *status = HYPERCLIENT_DONTUSEKEY;
            return -1 - eq_sz - i;
        }

        if (seen.get(attrnum))
        {
            *status = HYPERCLIENT_DUPEATTR;
            return -1 - eq_sz - i;
        }

        if ((sc->attrs[attrnum].type != HYPERDATATYPE_INT64) && (sc->attrs[attrnum].type != HYPERDATATYPE_FLOAT))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return -1 - eq_sz - i;
        }

        seen.set(attrnum);
        s->range_set(attrnum, rn[i].lower_t.i, rn[i].upper_t.i);
    }

    if (attr)
    {
        uint16_t attrnum = sc->lookup_attr(attr);

        if (attrnum == sc->attrs_sz)
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return -1 - eq_sz - rn_sz;
        }

        *attrno = attrnum;
        *attrtype = sc->attrs[attrnum].type;
    }

    // Get the hosts that match our search terms.
    *search_entities = m_config->search_entities(m_config->space(space), *s);
    return 0;
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
            m_complete_failed.push(completedop(r->second, status, 0));
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

uint16_t
hyperclient :: validate_attribute(schema* sc,
                                  const hyperclient_attribute* attr,
                                  hyperclient_returncode* status)
{
    uint16_t attrnum = sc->lookup_attr(attr->attr);

    if (attrnum == sc->attrs_sz)
    {
        *status = HYPERCLIENT_UNKNOWNATTR;
        return sc->attrs_sz;
    }

    if (attrnum == 0)
    {
        *status = HYPERCLIENT_DONTUSEKEY;
        return sc->attrs_sz;
    }

    if (!container_implicit_coercion(sc->attrs[attrnum].type,
                                     e::slice(attr->value, attr->value_sz),
                                     attr->datatype))
    {
        *status = HYPERCLIENT_WRONGTYPE;
        return sc->attrs_sz;
    }

    return attrnum;
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
