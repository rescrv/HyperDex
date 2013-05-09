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

// e
#include <e/endian.h>

// BusyBee
#include <busybee_returncode.h>
#include <busybee_st.h>
#include <busybee_utils.h>

// Replicant
#include <replicant.h>

// HyperDex
#include "common/attribute.h"
#include "common/attribute_check.h"
#include "common/configuration.h"
#include "common/coordinator_returncode.h"
#include "common/datatypes.h"
#include "common/funcall.h"
#include "common/ids.h"
#include "common/macros.h"
#include "common/mapper.h"
#include "common/schema.h"
#include "common/serialization.h"
#include "client/complete.h"
#include "client/constants.h"
#include "client/coordinator_link.h"
#include "client/description.h"
#include "client/hyperclient.h"
#include "client/keyop_info.h"
#include "client/pending.h"
#include "client/pending_count.h"
#include "client/pending_get.h"
#include "client/pending_group_del.h"
#include "client/pending_search.h"
#include "client/pending_search_description.h"
#include "client/pending_sorted_search.h"
#include "client/pending_statusonly.h"
#include "client/refcount.h"
#include "client/space_description.h"
#include "client/wrap.h"

using hyperdex::attribute_check;
using hyperdex::coordinator_returncode;
using hyperdex::datatype_info;
using hyperdex::funcall;
using hyperdex::server_id;
using hyperdex::virtual_server_id;

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
    datatype_info* CONCAT(_di_, __LINE__) = datatype_info::lookup(SCHEMA->attrs[0].type); \
    if (!CONCAT(_di_, __LINE__)->validate(e::slice((KEY), (KEY_SZ)))) \
    { \
        *status = HYPERCLIENT_WRONGTYPE; \
        return -1; \
    }

hyperclient :: hyperclient(const char* coordinator, uint16_t port)
    : m_config(new hyperdex::configuration())
    , m_busybee_mapper(new hyperdex::mapper(m_config.get()))
    , m_busybee(new busybee_st(m_busybee_mapper.get(), busybee_generate_id()))
    , m_coord(new hyperdex::coordinator_link(po6::net::hostname(coordinator, port)))
    , m_incomplete()
    , m_complete_succeeded()
    , m_complete_failed()
    , m_server_nonce(1)
    , m_client_id(1)
    , m_have_seen_config(false)
{
}

hyperclient :: ~hyperclient() throw ()
{
}

hyperclient_returncode
hyperclient :: add_space(const char* _description)
{
    hyperclient_returncode status;
    hyperdex::space s;

    if (!space_description_to_space(_description, &s))
    {
        return HYPERCLIENT_BADSPACE;
    }

    std::auto_ptr<e::buffer> msg(e::buffer::create(pack_size(s)));
    msg->pack_at(0) << s;
    const char* output;
    size_t output_sz;

    if (!m_coord->make_rpc("add-space", reinterpret_cast<const char*>(msg->data()), msg->size(),
                           &status, &output, &output_sz))
    {
        return status;
    }

    status = HYPERCLIENT_SUCCESS;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case hyperdex::COORD_SUCCESS:
                status = HYPERCLIENT_SUCCESS;
                break;
            case hyperdex::COORD_MALFORMED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_DUPLICATE:
                status = HYPERCLIENT_DUPLICATE;
                break;
            case hyperdex::COORD_NOT_FOUND:
                status = HYPERCLIENT_NOTFOUND;
                break;
            case hyperdex::COORD_INITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_UNINITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_TRANSFER_IN_PROGRESS:
                status = HYPERCLIENT_INTERNAL;
                break;
            default:
                status = HYPERCLIENT_INTERNAL;
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return status;
}

hyperclient_returncode
hyperclient :: rm_space(const char* space)
{
    hyperclient_returncode status;
    const char* output;
    size_t output_sz;

    if (!m_coord->make_rpc("rm-space", space, strlen(space) + 1,
                           &status, &output, &output_sz))
    {
        return status;
    }

    status = HYPERCLIENT_SUCCESS;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case hyperdex::COORD_SUCCESS:
                status = HYPERCLIENT_SUCCESS;
                break;
            case hyperdex::COORD_MALFORMED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_DUPLICATE:
                status = HYPERCLIENT_DUPLICATE;
                break;
            case hyperdex::COORD_NOT_FOUND:
                status = HYPERCLIENT_NOTFOUND;
                break;
            case hyperdex::COORD_INITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_UNINITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_TRANSFER_IN_PROGRESS:
                status = HYPERCLIENT_INTERNAL;
                break;
            default:
                status = HYPERCLIENT_INTERNAL;
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return status;
}

int64_t
hyperclient :: get(const char* space, const char* key, size_t key_sz,
                   hyperclient_returncode* status,
                   struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    MAINTAIN_COORD_CONNECTION(status)
    const hyperdex::schema* sc = m_config->get_schema(space);
    VALIDATE_KEY(sc, key, key_sz) // Checks sc
    e::intrusive_ptr<pending> op = new pending_get(status, attrs, attrs_sz);
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ + sizeof(uint32_t) + key_sz;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << e::slice(key, key_sz);
    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: del(const char* space, const char* key, size_t key_sz,
                   hyperclient_returncode* status)
{
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup("del", 3);
    return perform_funcall1(opinfo, space, key, key_sz, NULL, 0, NULL, 0, status);
}

int64_t
hyperclient :: cond_del(const char* space, const char* key, size_t key_sz,
                        const struct hyperclient_attribute_check* checks, size_t checks_sz, \
                        hyperclient_returncode* status)
{
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup("cond_del", 8);
    return perform_funcall1(opinfo, space, key, key_sz, checks, checks_sz, NULL, 0, status);
}

int64_t
hyperclient :: put_if_not_exist(const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                enum hyperclient_returncode* status)
{
    const hyperclient_keyop_info* opinfo;
    opinfo = hyperclient_keyop_info_lookup(XSTR(put_if_not_exist), strlen(XSTR(put_if_not_exist)));
    return perform_funcall1(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, status);
}

#define HYPERCLIENT_CPPDEF(OPNAME) \
    int64_t \
    hyperclient :: OPNAME(const char* space, const char* key, size_t key_sz, \
                          const struct hyperclient_attribute* attrs, size_t attrs_sz, \
                          enum hyperclient_returncode* status) \
    { \
        const hyperclient_keyop_info* opinfo; \
        opinfo = hyperclient_keyop_info_lookup(XSTR(OPNAME), strlen(XSTR(OPNAME))); \
        return perform_funcall1(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, status); \
    } \
    int64_t \
    hyperclient :: cond_ ## OPNAME(const char* space, const char* key, size_t key_sz, \
                                   const struct hyperclient_attribute_check* checks, size_t checks_sz, \
                                   const struct hyperclient_attribute* attrs, size_t attrs_sz, \
                                   enum hyperclient_returncode* status) \
    { \
        const hyperclient_keyop_info* opinfo; \
        opinfo = hyperclient_keyop_info_lookup(XSTR(OPNAME), strlen(XSTR(OPNAME))); \
        return perform_funcall1(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); \
    } \
    extern "C" \
    { \
    int64_t \
    hyperclient_ ## OPNAME(struct hyperclient* client, \
                           const char* space, const char* key, size_t key_sz, \
                           const struct hyperclient_attribute* attrs, size_t attrs_sz, \
                           hyperclient_returncode* status) \
    { \
        C_WRAP_EXCEPT(client->OPNAME(space, key, key_sz, attrs, attrs_sz, status)); \
    } \
    int64_t \
    hyperclient_cond_ ## OPNAME(struct hyperclient* client, \
                                const char* space, const char* key, size_t key_sz, \
                                const struct hyperclient_attribute_check* checks, size_t checks_sz, \
                                const struct hyperclient_attribute* attrs, size_t attrs_sz, \
                                hyperclient_returncode* status) \
    { \
        C_WRAP_EXCEPT(client->cond_ ## OPNAME(space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)); \
    } \
    }

HYPERCLIENT_CPPDEF(put)
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
        opinfo = hyperclient_keyop_info_lookup(XSTR(OPNAME), strlen(XSTR(OPNAME))); \
        return perform_funcall2(opinfo, space, key, key_sz, NULL, 0, attrs, attrs_sz, status); \
    } \
    int64_t \
    hyperclient :: cond_ ## OPNAME(const char* space, const char* key, size_t key_sz, \
                                   const struct hyperclient_attribute_check* checks, size_t checks_sz, \
                                   const struct hyperclient_map_attribute* attrs, size_t attrs_sz, \
                                   enum hyperclient_returncode* status) \
    { \
        const hyperclient_keyop_info* opinfo; \
        opinfo = hyperclient_keyop_info_lookup(XSTR(OPNAME), strlen(XSTR(OPNAME))); \
        return perform_funcall2(opinfo, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status); \
    } \
    extern "C" \
    { \
    int64_t \
    hyperclient_ ## OPNAME(struct hyperclient* client, \
                           const char* space, const char* key, size_t key_sz, \
                           const struct hyperclient_map_attribute* attrs, size_t attrs_sz, \
                           hyperclient_returncode* status) \
    { \
        C_WRAP_EXCEPT(client->OPNAME(space, key, key_sz, attrs, attrs_sz, status)); \
    } \
    int64_t \
    hyperclient_cond_ ## OPNAME(struct hyperclient* client, \
                                const char* space, const char* key, size_t key_sz, \
                                const struct hyperclient_attribute_check* checks, size_t checks_sz, \
                                const struct hyperclient_map_attribute* attrs, size_t attrs_sz, \
                                hyperclient_returncode* status) \
    { \
        C_WRAP_EXCEPT(client->cond_ ## OPNAME(space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)); \
    } \
    }

HYPERCLIENT_MAP_CPPDEF(map_add)
HYPERCLIENT_CPPDEF(map_remove)
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
                      const struct hyperclient_attribute_check* checks, size_t checks_sz,
                      enum hyperclient_returncode* status,
                      struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    MAINTAIN_COORD_CONNECTION(status)
    std::vector<hyperdex::attribute_check> chks;
    std::vector<hyperdex::virtual_server_id> servers;
    int64_t ret = prepare_searchop(space, checks, checks_sz, status, &chks, &servers);

    if (ret < 0)
    {
        return ret;
    }

    int64_t search_id = m_client_id;
    ++m_client_id;
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + sizeof(int64_t)
              + pack_size(chks);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << search_id << chks;
    e::intrusive_ptr<refcount> ref(new refcount());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        e::intrusive_ptr<pending> op = new pending_search(search_id, ref, status, attrs, attrs_sz);
        op->set_server_visible_nonce(m_server_nonce);
        ++m_server_nonce;
        op->set_sent_to(servers[i]);
        m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
        std::auto_ptr<e::buffer> tosend(msg->copy());

        if (send(op, tosend) < 0)
        {
#ifdef _MSC_VER
            m_complete_failed.push(std::shared_ptr<complete>(new complete(search_id, status, HYPERCLIENT_RECONFIGURE, 0)));
#else
            m_complete_failed.push(complete(search_id, status, HYPERCLIENT_RECONFIGURE, 0));
#endif
            m_incomplete.erase(op->server_visible_nonce());
        }
    }

    return search_id;
}

int64_t
hyperclient :: search_describe(const char* space,
                               const struct hyperclient_attribute_check* checks, size_t checks_sz,
                               enum hyperclient_returncode* status, const char** _description)
{
    MAINTAIN_COORD_CONNECTION(status)
    std::vector<hyperdex::attribute_check> chks;
    std::vector<hyperdex::virtual_server_id> servers;
    int64_t ret = prepare_searchop(space, checks, checks_sz, status, &chks, &servers);

    if (ret < 0)
    {
        return ret;
    }

    int64_t search_id = m_client_id;
    ++m_client_id;
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + pack_size(chks);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << chks;
    e::intrusive_ptr<description> sd(new description(_description));

    for (size_t i = 0; i < servers.size(); ++i)
    {
        sd->add_text(servers[i], "touched by search");
        e::intrusive_ptr<pending> op = new pending_search_description(search_id, status, sd);
        op->set_server_visible_nonce(m_server_nonce);
        ++m_server_nonce;
        op->set_sent_to(servers[i]);
        m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
        std::auto_ptr<e::buffer> tosend(msg->copy());

        if (send(op, tosend) < 0)
        {
            sd->add_text(servers[i], "failed");
#ifdef _MSC_VER
            m_complete_failed.push(std::shared_ptr<complete>(new complete(search_id, status, HYPERCLIENT_RECONFIGURE, 0)));
#else
            m_complete_failed.push(complete(search_id, status, HYPERCLIENT_RECONFIGURE, 0));
#endif
            m_incomplete.erase(op->server_visible_nonce());
        }
    }

    return search_id;
}

static void
delete_bracket_auto_ptr(std::auto_ptr<e::buffer>* ptrs)
{
    delete[] ptrs;
}

int64_t
hyperclient :: sorted_search(const char* space,
                             const struct hyperclient_attribute_check* checks, size_t checks_sz,
                             const char* sort_by,
                             uint64_t limit,
                             bool maximize,
                             enum hyperclient_returncode* status,
                             struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    MAINTAIN_COORD_CONNECTION(status)
    std::vector<hyperdex::attribute_check> chks;
    std::vector<hyperdex::virtual_server_id> servers;
    uint16_t sort_by_no;
    hyperdatatype sort_by_type;
    int64_t ret = prepare_searchop(space, checks, checks_sz, sort_by, status, &chks, &servers, &sort_by_no, &sort_by_type);

    if (ret < 0)
    {
        return ret;
    }

    if (sort_by_type != HYPERDATATYPE_STRING &&
        sort_by_type != HYPERDATATYPE_INT64 &&
        sort_by_type != HYPERDATATYPE_FLOAT)
    {
        *status = HYPERCLIENT_WRONGTYPE;
        return -1 - checks_sz;
    }

    int64_t search_id = m_client_id;
    ++m_client_id;
    int8_t max = maximize ? 1 : 0;
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + pack_size(chks)
              + sizeof(limit)
              + sizeof(sort_by_no)
              + sizeof(max);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << chks << limit << sort_by_no << max;
    std::auto_ptr<e::buffer>* backings = new std::auto_ptr<e::buffer>[servers.size()];
    e::guard g = e::makeguard(delete_bracket_auto_ptr, backings);
    e::intrusive_ptr<pending_sorted_search::state> state;
    state = new pending_sorted_search::state(backings, limit, sort_by_no, sort_by_type, maximize);
    g.dismiss();

    for (size_t i = 0; i < servers.size(); ++i)
    {
        e::intrusive_ptr<pending> op = new pending_sorted_search(search_id, state, status, attrs, attrs_sz);
        op->set_server_visible_nonce(m_server_nonce);
        ++m_server_nonce;
        op->set_sent_to(servers[i]);
        m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
        std::auto_ptr<e::buffer> tosend(msg->copy());

        if (send(op, tosend) < 0)
        {
#ifdef _MSC_VER
            m_complete_failed.push(std::shared_ptr<complete>(new complete(search_id, status, HYPERCLIENT_RECONFIGURE, 0)));
#else
            m_complete_failed.push(complete(search_id, status, HYPERCLIENT_RECONFIGURE, 0));
#endif
            m_incomplete.erase(op->server_visible_nonce());
        }
    }

    return search_id;
}

int64_t
hyperclient :: group_del(const char* space,
                         const struct hyperclient_attribute_check* checks, size_t checks_sz,
                         enum hyperclient_returncode* status)
{
    MAINTAIN_COORD_CONNECTION(status)
    std::vector<hyperdex::attribute_check> chks;
    std::vector<hyperdex::virtual_server_id> servers;
    int64_t ret = prepare_searchop(space, checks, checks_sz, status, &chks, &servers);

    if (ret < 0)
    {
        return ret;
    }

    int64_t search_id = m_client_id;
    ++m_client_id;
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + pack_size(chks);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << chks;
    e::intrusive_ptr<refcount> ref(new refcount());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        e::intrusive_ptr<pending> op = new pending_group_del(search_id, ref, status);
        op->set_server_visible_nonce(m_server_nonce);
        ++m_server_nonce;
        op->set_sent_to(servers[i]);
        m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
        std::auto_ptr<e::buffer> tosend(msg->copy());

        if (send(op, tosend) < 0)
        {
#ifdef _MSC_VER
            m_complete_failed.push(std::shared_ptr<complete>(new complete(search_id, status, HYPERCLIENT_RECONFIGURE, 0)));
#else
            m_complete_failed.push(complete(search_id, status, HYPERCLIENT_RECONFIGURE, 0));
#endif
            m_incomplete.erase(op->server_visible_nonce());
        }
    }

    return search_id;
}

int64_t
hyperclient :: count(const char* space,
                     const struct hyperclient_attribute_check* checks, size_t checks_sz,
                     enum hyperclient_returncode* status,
                     uint64_t* result)
{
    MAINTAIN_COORD_CONNECTION(status)
    std::vector<hyperdex::attribute_check> chks;
    std::vector<hyperdex::virtual_server_id> servers;
    int64_t ret = prepare_searchop(space, checks, checks_sz, status, &chks, &servers);

    if (ret < 0)
    {
        return ret;
    }

    int64_t search_id = m_client_id;
    ++m_client_id;
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + pack_size(chks);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << chks;
    e::intrusive_ptr<refcount> ref(new refcount());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        e::intrusive_ptr<pending> op = new pending_count(search_id, ref, status, result);
        op->set_server_visible_nonce(m_server_nonce);
        ++m_server_nonce;
        op->set_sent_to(servers[i]);
        m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
        std::auto_ptr<e::buffer> tosend(msg->copy());

        if (send(op, tosend) < 0)
        {
#ifdef _MSC_VER
            m_complete_failed.push(std::shared_ptr<complete>(new complete(search_id, status, HYPERCLIENT_RECONFIGURE, 0)));
#else
            m_complete_failed.push(complete(search_id, status, HYPERCLIENT_RECONFIGURE, 0));
#endif
            m_incomplete.erase(op->server_visible_nonce());
        }
    }

    return search_id;
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

        uint64_t sid_num;
        std::auto_ptr<e::buffer> msg;
        m_busybee->set_timeout(timeout);
        busybee_returncode rc = m_busybee->recv(&sid_num, &msg);
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
                *status = HYPERCLIENT_SUCCESS;
                killall(id, HYPERCLIENT_RECONFIGURE);
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
        uint64_t vfrom;
        int64_t nonce;
        up = up >> mt >> vfrom >> nonce;

        if (up.error())
        {
            killall(id, HYPERCLIENT_SERVERERROR);
            continue;
        }

        hyperdex::network_msgtype msg_type = static_cast<hyperdex::network_msgtype>(mt);
        incomplete_map_t::iterator it = m_incomplete.find(nonce);

        if (it == m_incomplete.end())
        {
            killall(id, HYPERCLIENT_SERVERERROR);
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

        if (m_config->get_server_id(virtual_server_id(vfrom)) == id)
        {
            // Handle response will either successfully finish one event and
            // then return >0, fail many and return 0, or encounter another sort
            // of error and return <0.  In the first case we leave immediately.
            // In the second case, we go back around the loop, and start
            // returning from m_complete.  In the third case, we return
            // immediately.
            m_incomplete.erase(it);
            int64_t cid = op->handle_response(this, id, msg, msg_type, status);

            if (cid != 0)
            {
                return cid;
            }
        }
        else
        {
            killall(id, HYPERCLIENT_SERVERERROR);
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
#ifdef _MSC_VER
        complete c = *m_complete_failed.front();
#else
        complete c = m_complete_failed.front();
#endif
        m_complete_failed.pop();
        *c.status = c.why;

        if (c.error != 0)
        {
            errno = c.error;
        }

        *status = HYPERCLIENT_SUCCESS;
        return c.client_id;
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

    const hyperdex::schema* sc = m_config->get_schema(space);

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

hyperclient_returncode
hyperclient :: initialize_cluster(uint64_t cluster, const char* path)
{
    replicant_client* repl = m_coord->replicant();
    replicant_returncode rstatus;
    const char* errmsg = NULL;
    size_t errmsg_sz = 0;

    int64_t noid = repl->new_object("hyperdex", path, &rstatus,
                                    &errmsg, &errmsg_sz);

    if (noid < 0)
    {
        switch (rstatus)
        {
            case REPLICANT_BAD_LIBRARY:
                return HYPERCLIENT_NOTFOUND;
            case REPLICANT_INTERRUPTED:
                return HYPERCLIENT_INTERRUPTED;
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_BACKOFF:
                return HYPERCLIENT_COORDFAIL;
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_TIMEOUT:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                return HYPERCLIENT_INTERNAL;
        }
    }

    replicant_returncode lstatus;
    int64_t lid = repl->loop(noid, -1, &lstatus);

    if (lid < 0)
    {
        repl->kill(noid);

        switch (lstatus)
        {
            case REPLICANT_INTERRUPTED:
                return HYPERCLIENT_INTERRUPTED;
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_BACKOFF:
                return HYPERCLIENT_COORDFAIL;
            case REPLICANT_TIMEOUT:
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                return HYPERCLIENT_INTERNAL;
        }
    }

    assert(lid == noid);

    switch (rstatus)
    {
        case REPLICANT_SUCCESS:
            break;
        case REPLICANT_INTERRUPTED:
            return HYPERCLIENT_INTERRUPTED;
        case REPLICANT_OBJ_EXIST:
            return HYPERCLIENT_DUPLICATE;
        case REPLICANT_FUNC_NOT_FOUND:
        case REPLICANT_OBJ_NOT_FOUND:
        case REPLICANT_COND_NOT_FOUND:
        case REPLICANT_COND_DESTROYED:
        case REPLICANT_SERVER_ERROR:
        case REPLICANT_NEED_BOOTSTRAP:
        case REPLICANT_MISBEHAVING_SERVER:
            return HYPERCLIENT_COORDFAIL;
        case REPLICANT_NAME_TOO_LONG:
        case REPLICANT_BAD_LIBRARY:
            return HYPERCLIENT_COORD_LOGGED;
        case REPLICANT_TIMEOUT:
        case REPLICANT_BACKOFF:
        case REPLICANT_INTERNAL_ERROR:
        case REPLICANT_NONE_PENDING:
        case REPLICANT_GARBAGE:
        default:
            return HYPERCLIENT_INTERNAL;
    }

    hyperclient_returncode status;
    char data[sizeof(uint64_t)];
    e::pack64be(cluster, data);
    const char* output;
    size_t output_sz;

    if (!m_coord->make_rpc("initialize", data, sizeof(uint64_t),
                           &status, &output, &output_sz))
    {
        return status;
    }

    status = HYPERCLIENT_SUCCESS;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case hyperdex::COORD_SUCCESS:
                status = HYPERCLIENT_SUCCESS;
                break;
            case hyperdex::COORD_MALFORMED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_DUPLICATE:
                status = HYPERCLIENT_CLUSTER_JUMP;
                break;
            case hyperdex::COORD_NOT_FOUND:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_INITIALIZED:
                status = HYPERCLIENT_DUPLICATE;
                break;
            case hyperdex::COORD_UNINITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_TRANSFER_IN_PROGRESS:
                status = HYPERCLIENT_INTERNAL;
                break;
            default:
                status = HYPERCLIENT_INTERNAL;
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return status;
}

hyperclient_returncode
hyperclient :: show_config(std::ostream& out)
{
    hyperclient_returncode status;

    if (maintain_coord_connection(&status) < 0)
    {
        return status;
    }

    m_config->debug_dump(out);
    return HYPERCLIENT_SUCCESS;
}

hyperclient_returncode
hyperclient :: kill(uint64_t server_id)
{
    hyperclient_returncode status;
    char data[sizeof(uint64_t)];
    e::pack64be(server_id, data);
    const char* output;
    size_t output_sz;

    if (!m_coord->make_rpc("kill", data, sizeof(uint64_t),
                           &status, &output, &output_sz))
    {
        return status;
    }

    status = HYPERCLIENT_SUCCESS;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case hyperdex::COORD_SUCCESS:
                status = HYPERCLIENT_SUCCESS;
                break;
            case hyperdex::COORD_MALFORMED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_DUPLICATE:
                status = HYPERCLIENT_DUPLICATE;
                break;
            case hyperdex::COORD_NOT_FOUND:
                status = HYPERCLIENT_NOTFOUND;
                break;
            case hyperdex::COORD_INITIALIZED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_UNINITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_TRANSFER_IN_PROGRESS:
                status = HYPERCLIENT_INTERNAL;
                break;
            default:
                status = HYPERCLIENT_INTERNAL;
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return status;
}

hyperclient_returncode
hyperclient :: initiate_transfer(uint64_t region_id, uint64_t server_id)
{
    hyperclient_returncode status;
    char data[2 * sizeof(uint64_t)];
    e::pack64be(region_id, data);
    e::pack64be(server_id, data + sizeof(uint64_t));
    const char* output;
    size_t output_sz;

    if (!m_coord->make_rpc("xfer-begin", data, 2 * sizeof(uint64_t),
                           &status, &output, &output_sz))
    {
        return status;
    }

    status = HYPERCLIENT_SUCCESS;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case hyperdex::COORD_SUCCESS:
                status = HYPERCLIENT_SUCCESS;
                break;
            case hyperdex::COORD_MALFORMED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_DUPLICATE:
                status = HYPERCLIENT_DUPLICATE;
                break;
            case hyperdex::COORD_NOT_FOUND:
                status = HYPERCLIENT_NOTFOUND;
                break;
            case hyperdex::COORD_INITIALIZED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_UNINITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_TRANSFER_IN_PROGRESS:
                status = HYPERCLIENT_DUPLICATE;
                break;
            default:
                status = HYPERCLIENT_INTERNAL;
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return status;
}

int64_t
hyperclient :: maintain_coord_connection(hyperclient_returncode* status)
{
    if (!m_have_seen_config)
    {
        if (!m_coord->wait_for_config(status))
        {
            return -1;
        }

        if (m_busybee->set_external_fd(m_coord->poll_fd()) < 0)
        {
            *status = HYPERCLIENT_POLLFAILED;
            return -1;
        }
    }
    else
    {
        if (!m_coord->poll_for_config(status))
        {
            return 0;
        }
    }

    int64_t reconfigured = 0;

    if (m_config->version() < m_coord->config().version())
    {
        *m_config = m_coord->config();
        incomplete_map_t::iterator i = m_incomplete.begin();

        while (i != m_incomplete.end())
        {
            // If the mapping that was true when the operation started is no
            // longer true, we just abort the operation.
            if (m_config->get_server_id(i->second->sent_to()) == server_id())
            {
#ifdef _MSC_VER
                m_complete_failed.push(std::shared_ptr<complete>(new complete(i->second->client_visible_id(), i->second->status_ptr(), HYPERCLIENT_RECONFIGURE, 0)));
#else
                m_complete_failed.push(complete(i->second->client_visible_id(), i->second->status_ptr(), HYPERCLIENT_RECONFIGURE, 0));
#endif
                m_incomplete.erase(i);
                i = m_incomplete.begin();
                ++reconfigured;
            }
            else
            {
                ++i;
            }
        }

        m_have_seen_config = true;
    }

    return reconfigured;
}

int64_t
hyperclient :: perform_funcall1(const hyperclient_keyop_info* opinfo,
                                const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
{
    MAINTAIN_COORD_CONNECTION(status)
    const hyperdex::schema* sc = m_config->get_schema(space);
    VALIDATE_KEY(sc, key, key_sz) // Checks sc

    // A new pending op
    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_ATOMIC, hyperdex::RESP_ATOMIC, status);

    // Prepare the checks
    std::vector<attribute_check> chks;
    size_t num_checks = prepare_checks(sc, checks, checks_sz, status, &chks);

    if (num_checks < checks_sz)
    {
        return -2 - num_checks;
    }

    // Prepare the ops
    std::vector<funcall> ops;
    size_t num_ops = prepare_ops(sc, opinfo, attrs, attrs_sz, status, &ops);

    if (num_ops < attrs_sz)
    {
        return -2 - checks_sz - num_ops;
    }

    // The size of the buffer we need
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + sizeof(uint8_t)
              + sizeof(uint32_t) + key_sz
              + sizeof(uint32_t) + sizeof(uint32_t);

    for (size_t i = 0; i < chks.size(); ++i)
    {
        sz += pack_size(chks[i]);
    }

    for (size_t i = 0; i < ops.size(); ++i)
    {
        sz += pack_size(ops[i]);
    }

    std::sort(chks.begin(), chks.end());
    std::sort(ops.begin(), ops.end());
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    uint8_t flags = (opinfo->fail_if_not_exist ? 1 : 0)
                  | (opinfo->fail_if_exist ? 2 : 0)
                  | (opinfo->has_funcalls ? 128 : 0);
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << e::slice(key, key_sz) << flags << chks << ops;
    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: perform_funcall2(const hyperclient_keyop_info* opinfo,
                                const char* space, const char* key, size_t key_sz,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const struct hyperclient_map_attribute* attrs, size_t attrs_sz,
                                hyperclient_returncode* status)
{
    MAINTAIN_COORD_CONNECTION(status)
    const hyperdex::schema* sc = m_config->get_schema(space);
    VALIDATE_KEY(sc, key, key_sz) // Checks sc

    // A new pending op
    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_ATOMIC, hyperdex::RESP_ATOMIC, status);

    // Prepare the checks
    std::vector<attribute_check> chks;
    size_t num_checks = prepare_checks(sc, checks, checks_sz, status, &chks);

    if (num_checks < checks_sz)
    {
        return -2 - num_checks;
    }

    // Prepare the ops
    std::vector<funcall> ops;
    size_t num_ops = prepare_ops(sc, opinfo, attrs, attrs_sz, status, &ops);

    if (num_ops < attrs_sz)
    {
        return -2 - checks_sz - num_ops;
    }

    // The size of the buffer we need
    size_t sz = HYPERCLIENT_HEADER_SIZE_REQ
              + sizeof(uint8_t)
              + sizeof(uint32_t) + key_sz
              + sizeof(uint32_t) + sizeof(uint32_t);

    for (size_t i = 0; i < chks.size(); ++i)
    {
        sz += pack_size(chks[i]);
    }

    for (size_t i = 0; i < ops.size(); ++i)
    {
        sz += pack_size(ops[i]);
    }

    std::sort(chks.begin(), chks.end());
    std::sort(ops.begin(), ops.end());
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    uint8_t flags = (opinfo->fail_if_not_exist ? 1 : 0)
                  | (opinfo->has_funcalls ? 128 : 0);
    msg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << e::slice(key, key_sz) << flags << chks << ops;
    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: prepare_searchop(const char* space,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                hyperclient_returncode* status,
                                std::vector<hyperdex::attribute_check>* chks,
                                std::vector<hyperdex::virtual_server_id>* servers)
{
    return prepare_searchop(space, checks, checks_sz, NULL, status, chks, servers, NULL, NULL);
}

int64_t
hyperclient :: prepare_searchop(const char* space,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                const char* aux_attr,
                                hyperclient_returncode* status,
                                std::vector<hyperdex::attribute_check>* chks,
                                std::vector<hyperdex::virtual_server_id>* servers,
                                uint16_t* aux_attrno,
                                hyperdatatype* aux_attrtype)
{
    const hyperdex::schema* sc = m_config->get_schema(space);

    if (!sc)
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return -1;
    }

    size_t num_checks = prepare_checks(sc, checks, checks_sz, status, chks);

    if (num_checks != checks_sz)
    {
        return -1 - num_checks;
    }

    if (aux_attr)
    {
        uint16_t attrnum = sc->lookup_attr(aux_attr);

        if (attrnum == sc->attrs_sz)
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return -1 - checks_sz;
        }

        *aux_attrno = attrnum;
        *aux_attrtype = sc->attrs[attrnum].type;
    }

    std::stable_sort(chks->begin(), chks->end());
    m_config->lookup_search(space, *chks, servers);

    if (servers->empty())
    {
        *status = HYPERCLIENT_INTERNAL;
        return -1;
    }

    return 0;
}

size_t
hyperclient :: prepare_checks(const hyperdex::schema* sc,
                              const hyperclient_attribute_check* checks, size_t checks_sz,
                              hyperclient_returncode* status,
                              std::vector<hyperdex::attribute_check>* chks)
{
    chks->reserve(checks_sz);

    for (size_t i = 0; i < checks_sz; ++i)
    {
        uint16_t attrnum = sc->lookup_attr(checks[i].attr);

        if (attrnum >= sc->attrs_sz)
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return i;
        }

        hyperdatatype datatype = checks[i].datatype;

        if (datatype == CONTAINER_TYPE(datatype) &&
            CONTAINER_TYPE(datatype) == CONTAINER_TYPE(sc->attrs[attrnum].type) &&
            checks[i].value_sz == 0)
        {
            datatype = sc->attrs[attrnum].type;
        }

        attribute_check c;
        c.attr = attrnum;
        c.value = e::slice(checks[i].value, checks[i].value_sz);
        c.datatype = datatype;
        c.predicate = checks[i].predicate;

        if (!validate_attribute_check(*sc, c))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return i;
        }

        chks->push_back(c);
    }

    return checks_sz;
}

size_t
hyperclient :: prepare_ops(const hyperdex::schema* sc,
                           const hyperclient_keyop_info* opinfo,
                           const hyperclient_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status,
                           std::vector<funcall>* ops)
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

        hyperdatatype datatype = attrs[i].datatype;

        if (datatype == CONTAINER_TYPE(datatype) &&
            CONTAINER_TYPE(datatype) == CONTAINER_TYPE(sc->attrs[attrnum].type) &&
            attrs[i].value_sz == 0)
        {
            datatype = sc->attrs[attrnum].type;
        }

        funcall o;
        o.attr = attrnum;
        o.name = opinfo->fname;
        o.arg1 = e::slice(attrs[i].value, attrs[i].value_sz);
        o.arg1_datatype = datatype;
        datatype_info* type = datatype_info::lookup(sc->attrs[attrnum].type);

        if (!type->check_args(o))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return i;
        }

        ops->push_back(o);
    }

    return attrs_sz;
}

size_t
hyperclient :: prepare_ops(const hyperdex::schema* sc,
                           const hyperclient_keyop_info* opinfo,
                           const hyperclient_map_attribute* attrs, size_t attrs_sz,
                           hyperclient_returncode* status,
                           std::vector<funcall>* ops)
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

        hyperdatatype k_datatype = attrs[i].map_key_datatype;

        if (k_datatype == CONTAINER_TYPE(k_datatype) &&
            CONTAINER_TYPE(k_datatype) == CONTAINER_TYPE(sc->attrs[attrnum].type) &&
            attrs[i].value_sz == 0)
        {
            k_datatype = sc->attrs[attrnum].type;
        }

        hyperdatatype v_datatype = attrs[i].value_datatype;

        if (v_datatype == CONTAINER_TYPE(v_datatype) &&
            CONTAINER_TYPE(v_datatype) == CONTAINER_TYPE(sc->attrs[attrnum].type) &&
            attrs[i].value_sz == 0)
        {
            v_datatype = sc->attrs[attrnum].type;
        }

        funcall o;
        o.attr = attrnum;
        o.name = opinfo->fname;
        o.arg2 = e::slice(attrs[i].map_key, attrs[i].map_key_sz);
        o.arg2_datatype = k_datatype;
        o.arg1 = e::slice(attrs[i].value, attrs[i].value_sz);
        o.arg1_datatype = v_datatype;
        datatype_info* type = datatype_info::lookup(sc->attrs[attrnum].type);

        if (!type->check_args(o))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return i;
        }

        ops->push_back(o);
    }

    return attrs_sz;
}

int64_t
hyperclient :: add_keyop(const char* space, const char* key, size_t key_sz,
                         std::auto_ptr<e::buffer> msg, e::intrusive_ptr<pending> op)
{
    if (!m_config->get_schema(space))
    {
        op->set_status(HYPERCLIENT_UNKNOWNSPACE);
        return -1;
    }

    hyperdex::virtual_server_id vsi = m_config->point_leader(space, e::slice(key, key_sz));

    if (vsi == hyperdex::virtual_server_id())
    {
        op->set_status(HYPERCLIENT_RECONFIGURE);
        return -1;
    }

    op->set_server_visible_nonce(m_server_nonce);
    ++m_server_nonce;
    op->set_sent_to(vsi);
    int64_t ret = send(op, msg);
    assert(ret <= 0);

    if (ret == 0)
    {
        op->set_client_visible_id(m_client_id);
        ++m_client_id;
        m_incomplete.insert(std::make_pair(op->server_visible_nonce(), op));
        return op->client_visible_id();
    }
    else
    {
        return ret;
    }
}

int64_t
hyperclient :: send(e::intrusive_ptr<pending> op,
                    std::auto_ptr<e::buffer> msg)
{
    const uint8_t type = static_cast<uint8_t>(op->request_type());
    const uint8_t flags = 0;
    const uint64_t version = m_config->version();
    const uint64_t vto = op->sent_to().get();
    const uint64_t nonce = op->server_visible_nonce();
    msg->pack_at(BUSYBEE_HEADER_SIZE) << type << flags << version << vto << nonce;
    server_id dest = m_config->get_server_id(op->sent_to());
    m_busybee->set_timeout(-1);

    switch (m_busybee->send(dest.get(), msg))
    {
        case BUSYBEE_SUCCESS:
            return 0;
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_DISRUPTED:
        case BUSYBEE_ADDFDFAIL:
            op->set_status(HYPERCLIENT_RECONFIGURE);
            killall(dest, HYPERCLIENT_RECONFIGURE);
            return -1;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        case BUSYBEE_INTERRUPTED:
        default:
            abort();
    }
}

void
hyperclient :: killall(const hyperdex::server_id& id,
                       hyperclient_returncode status)
{
    incomplete_map_t::iterator r = m_incomplete.begin();

    while (r != m_incomplete.end())
    {
        if (m_config->get_server_id(r->second->sent_to()) == id)
        {
#ifdef _MSC_VER
            m_complete_failed.push(std::shared_ptr<complete>(new complete(r->second->client_visible_id(),
                                            r->second->status_ptr(),
                                            status, 0)));
#else
            m_complete_failed.push(complete(r->second->client_visible_id(),
                                            r->second->status_ptr(),
                                            status, 0));
#endif
            m_incomplete.erase(r);
            r = m_incomplete.begin();
        }
        else
        {
            ++r;
        }
    }

    m_busybee->drop(id.get());
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
        STRINGIFY(HYPERCLIENT_INTERNAL);
        STRINGIFY(HYPERCLIENT_EXCEPTION);
        STRINGIFY(HYPERCLIENT_GARBAGE);
        default:
            lhs << "unknown returncode";
            break;
    }

    return lhs;
}
