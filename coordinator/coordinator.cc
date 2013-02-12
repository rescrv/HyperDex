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

#define __STDC_LIMIT_MACROS

// C++
#include <sstream>

// STL
#include <algorithm>
#include <tr1/memory>

// po6
#include <po6/net/location.h>

// HyperDex
#include "common/coordinator_returncode.h"
#include "common/serialization.h"
#include "coordinator/coordinator.h"
#include "coordinator/server_state.h"
#include "coordinator/transitions.h"

//////////////////////////// C Transition Functions ////////////////////////////

using namespace hyperdex;

static inline void
generate_response(replicant_state_machine_context* ctx, coordinator_returncode x)
{
    const char* ptr = NULL;

    switch (x)
    {
        case COORD_SUCCESS:
            ptr = "\x22\x80";
            break;
        case COORD_MALFORMED:
            ptr = "\x22\x81";
            break;
        case COORD_DUPLICATE:
            ptr = "\x22\x82";
            break;
        case COORD_NOT_FOUND:
            ptr = "\x22\x83";
            break;
        case COORD_INITIALIZED:
            ptr = "\x22\x84";
            break;
        case COORD_UNINITIALIZED:
            ptr = "\x22\x85";
            break;
        case COORD_TRANSFER_IN_PROGRESS:
            ptr = "\x22\x86";
            break;
        default:
            ptr = "\x00\x00";
            break;
    }

    replicant_state_machine_set_response(ctx, ptr, 2);
}

#define PROTECT_UNINITIALIZED \
    do \
    { \
        if (!obj) \
        { \
            fprintf(replicant_state_machine_log_stream(ctx), "cannot operate on NULL object\n"); \
            return generate_response(ctx, COORD_UNINITIALIZED); \
        } \
        coordinator* c = static_cast<coordinator*>(obj); \
        if (c->cluster() == 0) \
        { \
            fprintf(replicant_state_machine_log_stream(ctx), "cluster not initialized\n"); \
            return generate_response(ctx, COORD_UNINITIALIZED); \
        } \
    } \
    while (0)

#define CHECK_UNPACK(MSGTYPE) \
    do \
    { \
        if (up.error() || up.remain()) \
        { \
            fprintf(log, "received malformed \"" #MSGTYPE "\" message\n"); \
            return generate_response(ctx, COORD_MALFORMED); \
        } \
    } while (0)

#define INVARIANT_BROKEN(X) \
    fprintf(log, "invariant broken at " __FILE__ ":%d:  %s\n", __LINE__, (X))

extern "C"
{

void*
hyperdex_coordinator_create(struct replicant_state_machine_context* ctx)
{
    if (replicant_state_machine_condition_create(ctx, "config") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"config\"\n");
        return NULL;
    }

    if (replicant_state_machine_condition_create(ctx, "acked") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"acked\"\n");
        return NULL;
    }

    coordinator* c = new (std::nothrow) coordinator();

    if (!c)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "memory allocation failed\n");
    }

    return c;
}

void*
hyperdex_coordinator_recreate(struct replicant_state_machine_context* ctx,
                              const char* /*data*/, size_t /*data_sz*/)
{
    coordinator* c = new (std::nothrow) coordinator();

    if (!c)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "memory allocation failed\n");
    }

    fprintf(replicant_state_machine_log_stream(ctx), "recreate is not implemented\n");
    // XXX recreate from (data,data_sz)
    return c;
}

void
hyperdex_coordinator_destroy(struct replicant_state_machine_context*, void* obj)
{
    if (obj)
    {
        delete static_cast<coordinator*>(obj);
    }
}

void
hyperdex_coordinator_snapshot(struct replicant_state_machine_context* ctx,
                              void* /*obj*/, const char** data, size_t* sz)
{
    fprintf(replicant_state_machine_log_stream(ctx), "snapshot is not implemented\n");
    // XXX snapshot to (data,data_sz)
    *data = NULL;
    *sz = 0;
}

void
hyperdex_coordinator_initialize(struct replicant_state_machine_context* ctx,
                                void* obj, const char* data, size_t data_sz)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);

    if (!c)
    {
        fprintf(log, "cannot operate on NULL object\n");
        return generate_response(ctx, COORD_UNINITIALIZED);
    }

    uint64_t cluster;
    e::unpacker up(data, data_sz);
    up = up >> cluster;
    CHECK_UNPACK(initialize);
    c->initialize(ctx, cluster);
}

void
hyperdex_coordinator_add_space(struct replicant_state_machine_context* ctx,
                               void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    space s;
    e::unpacker up(data, data_sz);
    up = up >> s;
    CHECK_UNPACK(add_space);
    c->add_space(ctx, s);
}

void
hyperdex_coordinator_rm_space(struct replicant_state_machine_context* ctx,
                              void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);

    if (data_sz == 0 || data[data_sz - 1] != '\0')
    {
        fprintf(log, "received malformed \"rm_space\" message\n");
        return generate_response(ctx, COORD_MALFORMED);
    }

    c->rm_space(ctx, data);
}

void
hyperdex_coordinator_get_config(struct replicant_state_machine_context* ctx,
                                void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    e::unpacker up(data, data_sz);
    CHECK_UNPACK(get_config);
    c->get_config(ctx);
}

void
hyperdex_coordinator_ack_config(struct replicant_state_machine_context* ctx,
                                void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> _sid >> version;
    CHECK_UNPACK(ack_config);
    server_id sid(_sid);
    c->ack_config(ctx, sid, version);
}

void
hyperdex_coordinator_server_register(struct replicant_state_machine_context* ctx,
                                     void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    po6::net::location bind_to;
    e::unpacker up(data, data_sz);
    up = up >> _sid >> bind_to;
    CHECK_UNPACK(server_register);
    server_id sid(_sid);
    c->server_register(ctx, sid, bind_to);
}

void
hyperdex_coordinator_server_reregister(struct replicant_state_machine_context* ctx,
                                       void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    po6::net::location bind_to;
    e::unpacker up(data, data_sz);
    up = up >> _sid >> bind_to;
    CHECK_UNPACK(server_reregister);
    server_id sid(_sid);
    c->server_reregister(ctx, sid, bind_to);
}

void
hyperdex_coordinator_server_suspect(struct replicant_state_machine_context* ctx,
                                    void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> _sid >> version;
    CHECK_UNPACK(server_suspect);
    server_id sid(_sid);
    c->server_suspect(ctx, sid, version);
}

void
hyperdex_coordinator_server_shutdown1(struct replicant_state_machine_context* ctx,
                                      void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    e::unpacker up(data, data_sz);
    up = up >> _sid;
    CHECK_UNPACK(server_shutdown1);
    server_id sid(_sid);
    c->server_shutdown1(ctx, sid);
}

void
hyperdex_coordinator_server_shutdown2(struct replicant_state_machine_context* ctx,
                                      void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    e::unpacker up(data, data_sz);
    up = up >> _sid;
    CHECK_UNPACK(server_shutdown2);
    server_id sid(_sid);
    c->server_shutdown2(ctx, sid);
}

void
hyperdex_coordinator_xfer_begin(struct replicant_state_machine_context* ctx,
                                void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _rid;
    uint64_t _sid;
    e::unpacker up(data, data_sz);
    up = up >> _rid >> _sid;
    CHECK_UNPACK(xfer_begin);
    region_id rid(_rid);
    server_id sid(_sid);
    c->xfer_begin(ctx, rid, sid);
}

void
hyperdex_coordinator_xfer_go_live(struct replicant_state_machine_context* ctx,
                                  void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _xid;
    e::unpacker up(data, data_sz);
    up = up >> _xid;
    CHECK_UNPACK(xfer_go_live);
    transfer_id xid(_xid);
    c->xfer_go_live(ctx, xid);
}

void
hyperdex_coordinator_xfer_complete(struct replicant_state_machine_context* ctx,
                                   void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _xid;
    e::unpacker up(data, data_sz);
    up = up >> _xid;
    CHECK_UNPACK(xfer_complete);
    transfer_id xid(_xid);
    c->xfer_complete(ctx, xid);
}

} // extern "C"

/////////////////////////////// Coordinator Class //////////////////////////////

coordinator :: coordinator()
    : m_cluster(0)
    , m_version(0)
    , m_counter(1)
    , m_acked(0)
    , m_servers()
    , m_spaces()
    , m_captures()
    , m_transfers()
    , m_missing_acks()
    , m_capture_server_references()
    , m_capture_transfer_references()
    , m_region_server_references()
    , m_latest_config()
    , m_resp()
    , m_seed()
{
}

coordinator :: ~coordinator() throw ()
{
}

uint64_t
coordinator :: cluster() const
{
    return m_cluster;
}

void
coordinator :: initialize(replicant_state_machine_context* ctx, uint64_t token)
{
    FILE* log = replicant_state_machine_log_stream(ctx);

    if (m_cluster == 0)
    {
        fprintf(log, "initializing HyperDex cluster with id %lu\n", token);
        m_cluster = token;
        memset(&m_seed, 0, sizeof(m_seed));
#ifdef __APPLE__
        srand(m_seed);
#else
        srand48_r(m_cluster, &m_seed);
#endif
        issue_new_config(ctx);
        return generate_response(ctx, COORD_SUCCESS);
    }
    else
    {
        return generate_response(ctx, COORD_INITIALIZED);
    }
}

void
coordinator :: add_space(replicant_state_machine_context* ctx, const space& _s)
{
    FILE* log = replicant_state_machine_log_stream(ctx);

    if (!_s.validate())
    {
        fprintf(log, "could not add space \"%s\" because the space does not validate\n", _s.name);
        return generate_response(ctx, COORD_MALFORMED);
    }

    if (m_spaces.find(std::string(_s.name)) != m_spaces.end())
    {
        fprintf(log, "could not add space \"%s\" because there is already a space with that name\n", _s.name);
        return generate_response(ctx, COORD_DUPLICATE);
    }

    // Assign unique ids throughout the space
    std::tr1::shared_ptr<space> s(new space(_s));
    s->id = space_id(m_counter);
    ++m_counter;

    for (size_t i = 0; i < s->subspaces.size(); ++i)
    {
        s->subspaces[i].id = subspace_id(m_counter);
        ++m_counter;

        for (size_t j = 0; j < s->subspaces[i].regions.size(); ++j)
        {
            s->subspaces[i].regions[j].id = region_id(m_counter);
            ++m_counter;
            s->subspaces[i].regions[j].replicas.clear();
        }
    }

    m_spaces.insert(std::make_pair(std::string(s->name), s));
    initial_layout(ctx, s.get());
    fprintf(log, "successfully added space \"%s\" with space_id(%lu)\n", s->name, s->id.get());
    issue_new_config(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: rm_space(replicant_state_machine_context* ctx, const char* name)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    std::map<std::string, std::tr1::shared_ptr<space> >::iterator it;
    it = m_spaces.find(std::string(name));

    if (it == m_spaces.end())
    {
        fprintf(log, "could not remove space \"%s\" because it doesn't exist\n", name);
        return generate_response(ctx, COORD_NOT_FOUND);
    }
    else
    {
        fprintf(log, "successfully removed space \"%s\"/space_id(%lu)\n", name, it->second->id.get());
        m_spaces.erase(it);
        issue_new_config(ctx);
        return generate_response(ctx, COORD_SUCCESS);
    }
}

void
coordinator :: get_config(replicant_state_machine_context* ctx)
{
    assert(m_cluster != 0 && m_version != 0);

    if (!m_latest_config.get())
    {
        regenerate_cached(ctx);
    }

    const char* output = reinterpret_cast<const char*>(m_latest_config->data());
    size_t output_sz = m_latest_config->size();
    replicant_state_machine_set_response(ctx, output, output_sz);
}

void
coordinator :: ack_config(replicant_state_machine_context* ctx,
                          const server_id& sid,
                          uint64_t version)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server_state* ss = get_state(sid);

    if (ss)
    {
        ss->acked = std::max(ss->acked, version);
        fprintf(log, "server_id(%lu) acks config %lu\n", sid.get(), version);

        if (ss->state == server_state::NOT_AVAILABLE &&
            ss->acked > ss->version)
        {
            ss->state = server_state::AVAILABLE;
        }

        uint64_t remove_up_to = 0;

        for (std::list<missing_acks>::iterator it = m_missing_acks.begin();
                it != m_missing_acks.end(); ++it)
        {
            if (it->version() > ss->acked)
            {
                break;
            }

            it->ack(ss->id);

            if (it->empty())
            {
                assert(remove_up_to < it->version());
                remove_up_to = it->version();
            }
        }

        while (!m_missing_acks.empty() && m_missing_acks.front().version() <= remove_up_to)
        {
            m_missing_acks.pop_front();
        }

        while (m_acked < remove_up_to)
        {
            if (replicant_state_machine_condition_broadcast(ctx, "acked", &m_acked) < 0)
            {
                fprintf(log, "could not broadcast on \"acked\" condition\n");
                break;
            }
        }

        fprintf(log, "servers have acked through version %lu\n", m_acked);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: server_register(replicant_state_machine_context* ctx,
                               const server_id& sid,
                               const po6::net::location& bind_to)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    std::ostringstream oss;
    oss << bind_to;

    if (is_registered(sid))
    {
        fprintf(log, "cannot register server_id(%lu) on address %s because the id is in use\n", sid.get(), oss.str().c_str());
        return generate_response(ctx, hyperdex::COORD_DUPLICATE);
    }

    if (is_registered(bind_to))
    {
        fprintf(log, "cannot register server_id(%lu) on address %s because the address is in use\n", sid.get(), oss.str().c_str());
        return generate_response(ctx, hyperdex::COORD_DUPLICATE);
    }

    m_servers.push_back(server_state(sid, bind_to));
    m_servers.back().state = server_state::AVAILABLE;
    std::stable_sort(m_servers.begin(), m_servers.end());
    fprintf(log, "registered server_id(%lu) on address %s\n", sid.get(), oss.str().c_str());
    maintain_layout(ctx);
    return generate_response(ctx, hyperdex::COORD_SUCCESS);
}

void
coordinator :: server_reregister(replicant_state_machine_context* ctx,
                                 const server_id& sid,
                                 const po6::net::location& bind_to)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    std::ostringstream oss;
    oss << bind_to;
    server_state* ss = get_state(sid);

    if (!ss)
    {
        fprintf(log, "cannot re-register server_id(%lu) on address %s because the server doesn't exist\n", sid.get(), oss.str().c_str());
        return generate_response(ctx, hyperdex::COORD_DUPLICATE);
    }

    ss->bind_to = po6::net::location();

    if (is_registered(bind_to))
    {
        fprintf(log, "cannot register server_id(%lu) on address %s because the address is in use\n", sid.get(), oss.str().c_str());
        return generate_response(ctx, hyperdex::COORD_DUPLICATE);
    }

    ss->bind_to = bind_to;
    ss->state = server_state::AVAILABLE;
    fprintf(log, "re-registered server_id(%lu) on address %s\n", sid.get(), oss.str().c_str());
    maintain_layout(ctx);
    release_capture_references(sid);
    return generate_response(ctx, hyperdex::COORD_SUCCESS);
}

void
coordinator :: server_suspect(replicant_state_machine_context* ctx,
                              const server_id& sid, uint64_t version)
{
    FILE* log = replicant_state_machine_log_stream(ctx);

    if (version < m_version)
    {
        return generate_response(ctx, COORD_SUCCESS);
    }

    fprintf(log, "server_id(%lu) suspected\n", sid.get());

    server_state* state = get_state(sid);

    if (state && state->state == server_state::AVAILABLE)
    {
        state->state = server_state::NOT_AVAILABLE;
        state->version = m_version;
    }

    std::vector<region_id> rids;
    std::vector<transfer_id> xids;
    remove_server(sid, false, false, &rids, &xids);

    for (size_t i = 0; i < rids.size(); ++i)
    {
        fprintf(log, "server_id(%lu) removed from region_id(%lu)\n", sid.get(), rids[i].get());
    }

    for (size_t i = 0; i < xids.size(); ++i)
    {
        fprintf(log, "ending transfer_id(%lu) ended because it "
                     "involved server_id(%lu)\n",
                     xids[i].get(), sid.get());
    }

    if (!rids.empty() || !xids.empty())
    {
        issue_new_config(ctx);
        maintain_layout(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: server_shutdown1(replicant_state_machine_context* ctx,
                                const server_id& sid)
{
    std::vector<region_id> rids;
    std::vector<transfer_id> xids;
    remove_server(sid, true, false, &rids, &xids);

    for (size_t i = 0; i < rids.size(); ++i)
    {
        capture* cap = get_capture(rids[i]);

        if (!cap)
        {
            cap = new_capture(rids[i]);
        }

        assert(cap);
        add_capture_reference(sid, cap->id);
    }

    m_resp.reset(e::buffer::create(sizeof(uint16_t) + sizeof(uint64_t)));
    *m_resp << static_cast<uint16_t>(COORD_SUCCESS) << m_version;
    replicant_state_machine_set_response(ctx, reinterpret_cast<const char*>(m_resp->data()), m_resp->size());
}

void
coordinator :: server_shutdown2(replicant_state_machine_context* ctx,
                                const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server_state* ss = get_state(sid);

    if (ss)
    {
        ss->state = server_state::SHUTDOWN;
    }

    std::vector<region_id> rids;
    std::vector<transfer_id> xids;
    remove_server(sid, false, true, &rids, &xids);
    fprintf(log, "server_id(%lu) shutdown cleanly\n", sid.get());
    issue_new_config(ctx);
    maintain_layout(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: xfer_begin(replicant_state_machine_context* ctx,
                          const region_id& rid,
                          const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);

    if (!is_registered(sid))
    {
        fprintf(log, "cannot begin transfer of region_id(%lu) "
                     "to server_id(%lu) because the server "
                     "does not exist\n", rid.get(), sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    region* reg = get_region(rid);

    if (!reg)
    {
        fprintf(log, "cannot begin transfer of region_id(%lu) "
                     "to server_id(%lu) because the region "
                     "does not exist\n", rid.get(), sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    transfer* xfer = get_transfer(rid);

    if (xfer)
    {
        fprintf(log, "cannot begin transfer of region_id(%lu) "
                     "to server_id(%lu) because transfer_id(%lu) "
                     "is already in progress\n", rid.get(), sid.get(), xfer->id.get());
        return generate_response(ctx, COORD_TRANSFER_IN_PROGRESS);
    }

    for (size_t i = 0; i < reg->replicas.size(); ++i)
    {
        if (reg->replicas[i].si == sid)
        {
            fprintf(log, "cannot begin transfer of region_id(%lu) "
                         "to server_id(%lu) because server_id(%lu) "
                         "is already in the region\n", rid.get(), sid.get(), sid.get());
            return generate_response(ctx, COORD_DUPLICATE);
        }
    }

    if (reg->replicas.empty())
    {
        fprintf(log, "region_id(%lu) completely empty; cannot transfer to another node\n", reg->id.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    xfer = new_transfer(reg, sid);

    if (xfer)
    {
        issue_new_config(ctx);
        fprintf(log, "adding server_id(%lu) to region_id(%lu) "
                     "using transfer_id(%lu)/virtual_server_id(%lu)\n",
                     sid.get(), reg->id.get(),
                     xfer->id.get(), xfer->vdst.get());
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: xfer_go_live(replicant_state_machine_context* ctx,
                            const transfer_id& xid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    transfer* xfer = get_transfer(xid);

    if (!xfer)
    {
        fprintf(log, "cannot make transfer_id(%lu) live because it doesn't exist\n", xid.get());
        return generate_response(ctx, COORD_SUCCESS);
    }

    region* reg = get_region(xfer->rid);

    if (!reg)
    {
        fprintf(log, "cannot make transfer_id(%lu) live because it doesn't exist\n", xid.get());
        INVARIANT_BROKEN("transfer refers to nonexistent region");
        return generate_response(ctx, COORD_SUCCESS);
    }

    // If the transfer is already live
    if (reg->replicas.size() > 1 &&
        reg->replicas[reg->replicas.size() - 2].si == xfer->src &&
        reg->replicas[reg->replicas.size() - 1].si == xfer->dst)
    {
        return generate_response(ctx, COORD_SUCCESS);
    }

    if (reg->replicas.empty() || reg->replicas.back().si != xfer->src)
    {
        INVARIANT_BROKEN("transfer in a bad state");
        return generate_response(ctx, COORD_SUCCESS);
    }

    reg->replicas.push_back(replica(xfer->dst, xfer->vdst));
    fprintf(log, "transfer_id(%lu) is now live\n", xid.get());
    issue_new_config(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: xfer_complete(replicant_state_machine_context* ctx,
                             const transfer_id& xid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    transfer* xfer = get_transfer(xid);

    if (!xfer)
    {
        fprintf(log, "cannot complete transfer_id(%lu) because it doesn't exist\n", xid.get());
        return generate_response(ctx, COORD_SUCCESS);
    }

    region* reg = get_region(xfer->rid);

    if (!reg)
    {
        fprintf(log, "cannot make transfer_id(%lu) live because it doesn't exist\n", xid.get());
        INVARIANT_BROKEN("transfer refers to nonexistent region");
        return generate_response(ctx, COORD_SUCCESS);
    }

    if (!(reg->replicas.size() > 1 &&
          reg->replicas[reg->replicas.size() - 2].si == xfer->src &&
          reg->replicas[reg->replicas.size() - 1].si == xfer->dst))
    {
        fprintf(log, "cannot complete transfer_id(%lu) because it is not live\n", xid.get());
        return generate_response(ctx, COORD_SUCCESS);
    }

    del_transfer(xfer->id);
    fprintf(log, "transfer_id(%lu) is now complete\n", xid.get());
    issue_new_config(ctx);
    maintain_layout(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

server_state*
coordinator :: get_state(const server_id& sid)
{
    std::vector<server_state>::iterator it;
    it = std::lower_bound(m_servers.begin(), m_servers.end(), sid);

    if (it != m_servers.end() && it->id == sid)
    {
        return &(*it);
    }

    return NULL;
}

bool
coordinator :: is_registered(const server_id& sid)
{
    std::vector<server_state>::iterator it;
    it = std::lower_bound(m_servers.begin(), m_servers.end(), sid);
    return it != m_servers.end() && it->id == sid;
}

bool
coordinator :: is_registered(const po6::net::location& bind_to)
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].bind_to == bind_to)
        {
            return true;
        }
    }

    return false;
}

region*
coordinator :: get_region(const region_id& rid)
{
    for (std::map<std::string, std::tr1::shared_ptr<space> >::iterator it = m_spaces.begin();
            it != m_spaces.end(); ++it)
    {
        space& s(*it->second);

        for (size_t i = 0; i < s.subspaces.size(); ++i)
        {
            subspace& ss(s.subspaces[i]);

            for (size_t j = 0; j < ss.regions.size(); ++j)
            {
                if (ss.regions[j].id == rid)
                {
                    return &ss.regions[j];
                }
            }
        }
    }

    return NULL;
}

capture*
coordinator :: new_capture(const region_id& rid)
{
    assert(get_capture(rid) == NULL);
    m_captures.push_back(capture());
    capture* c = &m_captures.back();
    c->id = capture_id(m_counter);
    ++m_counter;
    c->rid = rid;
    return c;
}

capture*
coordinator :: get_capture(const region_id& rid)
{
    for (size_t i = 0; i < m_captures.size(); ++i)
    {
        if (m_captures[i].rid == rid)
        {
            return &m_captures[i];
        }
    }

    return NULL;
}

transfer*
coordinator :: new_transfer(region* reg,
                            const server_id& sid)
{
    assert(!reg->replicas.empty());
    capture* cap = get_capture(reg->id);

    if (!cap)
    {
        cap = new_capture(reg->id);
    }

    m_transfers.push_back(transfer());
    transfer* t = &m_transfers.back();
    t->id = transfer_id(m_counter);
    ++m_counter;
    t->rid = reg->id;
    t->src = reg->replicas.back().si;
    t->vsrc = reg->replicas.back().vsi;
    t->dst = sid;
    t->vdst = virtual_server_id(m_counter);
    ++m_counter;
    add_capture_reference(t->id, cap->id);
    return t;
}

transfer*
coordinator :: get_transfer(const region_id& rid)
{
    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        if (m_transfers[i].rid == rid)
        {
            return &m_transfers[i];
        }
    }

    return NULL;
}

transfer*
coordinator :: get_transfer(const transfer_id& xid)
{
    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        if (m_transfers[i].id == xid)
        {
            return &m_transfers[i];
        }
    }

    return NULL;
}

void
coordinator :: del_transfer(const transfer_id& xid)
{
    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        if (m_transfers[i].id == xid)
        {
            release_capture_reference(m_transfers[i].id);

            for (size_t j = i; j + 1 < m_transfers.size(); ++j)
            {
                m_transfers[j] = m_transfers[j + 1];
            }

            m_transfers.pop_back();
            return;
        }
    }
}

template <class T>
static void
uniquify/*not a word*/(std::vector<T>* v)
{
    std::sort(v->begin(), v->end());
    typename std::vector<T>::iterator it;
    it = std::unique(v->begin(), v->end());
    v->resize(it - v->begin());
}

template <class T, class U>
static bool
contains(const std::vector<std::pair<T, U> >& v, const T& e)
{
    typename std::vector<std::pair<T, U> >::const_iterator it;
    it = std::lower_bound(v.begin(), v.end(), std::make_pair(e, U()));
    return it != v.end() && it->first == e;
}

template <class T, class TID>
static void
remove(std::vector<T>* v, const TID& id)
{
    for (size_t i = 0; i < v->size(); ++i)
    {
        if ((*v)[i].id == id)
        {
            for (size_t j = i; j + 1 < v->size(); ++j)
            {
                (*v)[j] = (*v)[j + 1];
            }

            v->pop_back();
            return;
        }
    }
}

void
coordinator :: add_capture_reference(const server_id& sid, const capture_id& cid)
{
    m_capture_server_references.push_back(std::make_pair(cid, sid));
    uniquify(&m_capture_server_references);
}

void
coordinator :: add_capture_reference(const transfer_id& xid, const capture_id& cid)
{
    m_capture_transfer_references.push_back(std::make_pair(cid, xid));
    uniquify(&m_capture_transfer_references);
}

void
coordinator :: release_capture_reference(const transfer_id& xid)
{
    size_t i = 0;

    while (i < m_capture_transfer_references.size())
    {
        if (m_capture_transfer_references[i].second == xid)
        {
            capture_id cid = m_capture_transfer_references[i].first;
            m_capture_transfer_references[i] = m_capture_transfer_references.back();
            m_capture_transfer_references.pop_back();
            uniquify(&m_capture_transfer_references);
            maybe_garbage_collect_references(cid);
        }
        else
        {
            ++i;
        }
    }
}

void
coordinator :: release_capture_references(const server_id& sid)
{
    size_t i = 0;

    while (i < m_capture_server_references.size())
    {
        if (m_capture_server_references[i].second == sid)
        {
            capture_id cid = m_capture_server_references[i].first;
            m_capture_server_references[i] = m_capture_server_references.back();
            m_capture_server_references.pop_back();
            uniquify(&m_capture_server_references);
            maybe_garbage_collect_references(cid);
        }
        else
        {
            ++i;
        }
    }
}

void
coordinator :: maybe_garbage_collect_references(const capture_id& cid)
{
    if (!contains(m_capture_server_references, cid) &&
        !contains(m_capture_transfer_references, cid))
    {
        remove(&m_captures, cid);
    }
}

void
coordinator :: add_region_reference(const server_id& sid, const region_id& rid)
{
    m_region_server_references.push_back(std::make_pair(rid, sid));
    uniquify(&m_region_server_references);
}

server_id
coordinator :: get_region_reference(const region_id& rid)
{
    for (size_t i = 0; i < m_region_server_references.size(); ++i)
    {
        if (m_region_server_references[i].first == rid)
        {
            return m_region_server_references[i].second;
        }
    }

    return server_id();
}

void
coordinator :: release_region_reference(const region_id& rid)
{
    size_t i = 0;

    while (i < m_region_server_references.size())
    {
        if (m_region_server_references[i].first == rid)
        {
            m_region_server_references[i] = m_region_server_references.back();
            m_region_server_references.pop_back();
        }
        else
        {
            ++i;
        }
    }

    uniquify(&m_region_server_references);
}

void
coordinator :: release_region_references(const server_id& sid)
{
    size_t i = 0;

    while (i < m_region_server_references.size())
    {
        if (m_region_server_references[i].second == sid)
        {
            m_region_server_references[i] = m_region_server_references.back();
            m_region_server_references.pop_back();
        }
        else
        {
            ++i;
        }
    }

    uniquify(&m_region_server_references);
}

void
coordinator :: remove_server(const server_id& sid, bool dry_run, bool shutdown,
                             std::vector<region_id>* rids,
                             std::vector<transfer_id>* xids)
{
    shutdown = !dry_run && shutdown;

    for (std::map<std::string, std::tr1::shared_ptr<space> >::iterator it = m_spaces.begin();
            it != m_spaces.end(); ++it)
    {
        space& s(*it->second);

        for (size_t i = 0; i < s.subspaces.size(); ++i)
        {
            subspace& ss(s.subspaces[i]);

            for (size_t j = 0; j < ss.regions.size(); ++j)
            {
                region& reg(ss.regions[j]);
                size_t k = 0;

                if (shutdown &&
                    reg.replicas.size() == 1 &&
                    reg.replicas[0].si == sid)
                {
                    rids->push_back(reg.id);
                    add_region_reference(sid, reg.id);
                    reg.replicas.clear();
                }

                while (k < reg.replicas.size())
                {
                    if (reg.replicas[k].si == sid)
                    {
                        rids->push_back(reg.id);

                        if (!dry_run)
                        {
                            for (size_t x = k; x + 1 < reg.replicas.size(); ++x)
                            {
                                reg.replicas[x] = reg.replicas[x + 1];
                            }

                            reg.replicas.pop_back();
                        }
                        else
                        {
                            ++k;
                        }
                    }
                    else
                    {
                        ++k;
                    }
                }
            }
        }
    }

    size_t i = 0;

    while (i < m_transfers.size())
    {
        if (m_transfers[i].src == sid || m_transfers[i].dst == sid)
        {
            xids->push_back(m_transfers[i].id);

            if (!dry_run)
            {
                del_transfer(m_transfers[i].id);
                i = 0;
            }
            else
            {
                ++i;
            }
        }
        else
        {
            ++i;
        }
    }

    if (!dry_run && !shutdown)
    {
        release_region_references(sid);
        release_capture_references(sid);
    }
}

server_id
coordinator :: select_new_server_for(const std::vector<replica>& replicas)
{
    std::vector<server_id> available;

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].state == server_state::AVAILABLE)
        {
            available.push_back(m_servers[i].id);
        }
    }

    while (available.size() > replicas.size())
    {
#ifdef __APPLE__
        int y;
        y = rand_r(&m_seed);
#else
        long int y;
        lrand48_r(&m_seed, &y);
#endif
        bool found = false;
        server_id tmp(m_servers[y % m_servers.size()].id);

        for (size_t x = 0; x < replicas.size(); ++x)
        {
            if (tmp == replicas[x].si)
            {
                found = true;
            }
        }

        if (!found)
        {
            return tmp;
        }
    }

    return server_id();
}

void
coordinator :: issue_new_config(struct replicant_state_machine_context* ctx)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    uint64_t cond_state;

    if (replicant_state_machine_condition_broadcast(ctx, "config", &cond_state) < 0)
    {
        fprintf(log, "could not broadcast on \"config\" condition\n");
    }

    ++m_version;
    std::vector<server_id> sids;

    for (std::map<std::string, std::tr1::shared_ptr<space> >::iterator it = m_spaces.begin();
            it != m_spaces.end(); ++it)
    {
        space& s(*it->second);

        for (size_t i = 0; i < s.subspaces.size(); ++i)
        {
            subspace& ss(s.subspaces[i]);

            for (size_t j = 0; j < ss.regions.size(); ++j)
            {
                region& reg(ss.regions[j]);

                for (size_t k = 0; k < reg.replicas.size(); ++k)
                {
                    sids.push_back(reg.replicas[k].si);
                }
            }
        }
    }

    m_missing_acks.push_back(missing_acks(m_version, sids));
    fprintf(log, "issuing new configuration version %lu\n", m_version);
    m_latest_config.reset();
}

void
coordinator :: initial_layout(struct replicant_state_machine_context* ctx,
                              space* s)
{
    for (size_t i = 0; i < s->subspaces.size(); ++i)
    {
        subspace& ss(s->subspaces[i]);

        for (size_t j = 0; j < ss.regions.size(); ++j)
        {
            region& reg(ss.regions[j]);
            server_id new_server;

            while (reg.replicas.size() < s->fault_tolerance + 1 &&
                   (new_server = select_new_server_for(reg.replicas)) != server_id())
            {
                reg.replicas.push_back(replica());
                reg.replicas.back().si = new_server;
                reg.replicas.back().vsi = virtual_server_id(m_counter);
                ++m_counter;
            }
        }
    }

    if (s->fault_tolerance + 1 < m_servers.size())
    {
        FILE* log = replicant_state_machine_log_stream(ctx);
        fprintf(log, "space \"%s\" desires %lu-fault tolerance, but has %lu-fault "
                     "tolerance; add more servers\n",
                     s->name, s->fault_tolerance, m_servers.empty() ? 0 : m_servers.size() - 1);
    }
}

void
coordinator :: maintain_layout(replicant_state_machine_context* ctx)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    uint64_t changes = 0;

    for (std::map<std::string, std::tr1::shared_ptr<space> >::iterator it = m_spaces.begin();
            it != m_spaces.end(); ++it)
    {
        space& s(*it->second);

        for (size_t i = 0; i < s.subspaces.size(); ++i)
        {
            subspace& ss(s.subspaces[i]);
            server_id replacement;

            for (size_t j = 0; j < ss.regions.size(); ++j)
            {
                region& reg(ss.regions[j]);

                if (reg.replicas.empty())
                {
                    replacement = get_region_reference(reg.id);
                    server_state* state = get_state(replacement);

                    if (replacement != server_id() && state &&
                        state->state != server_state::AVAILABLE)
                    {
                        fprintf(log, "region_id(%lu) empty and the last server to leave it is offline\n", reg.id.get());
                    }
                    else if (replacement == server_id() || !state)
                    {
                        fprintf(log, "region_id(%lu) completely empty; cannot transfer to another node\n", reg.id.get());
                    }
                    else
                    {
                        reg.replicas.push_back(replica());
                        reg.replicas.back().si = replacement;
                        reg.replicas.back().vsi = virtual_server_id(m_counter);
                        ++m_counter;
                        release_region_reference(reg.id);
                        ++changes;
                        fprintf(log, "adding server_id(%lu) to region_id(%lu) because "
                                     "it was the last surviving server\n",
                                     replacement.get(), reg.id.get());
                    }
                }
                else if (!get_transfer(reg.id) &&
                         reg.replicas.size() < s.fault_tolerance + 1 &&
                         (replacement = select_new_server_for(reg.replicas)) != server_id())
                {
                    transfer* xfer = new_transfer(&reg, replacement);

                    if (xfer)
                    {
                        ++changes;
                        fprintf(log, "adding server_id(%lu) to region_id(%lu) "
                                     "using transfer_id(%lu)/virtual_server_id(%lu)\n",
                                     replacement.get(), reg.id.get(),
                                     xfer->id.get(), xfer->vdst.get());
                    }
                }
            }
        }
    }

    if (changes > 0)
    {
        issue_new_config(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: regenerate_cached(struct replicant_state_machine_context*)
{
    size_t sz = 6 * sizeof(uint64_t);
    uint64_t num_servers = 0;

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].state == server_state::AVAILABLE)
        {
            sz += sizeof(uint64_t) + pack_size(m_servers[i].bind_to);
            ++num_servers;
        }
    }

    for (std::map<std::string, std::tr1::shared_ptr<space> >::iterator it = m_spaces.begin();
            it != m_spaces.end(); ++it)
    {
        sz += pack_size(*it->second);
    }

    for (size_t i = 0; i < m_captures.size(); ++i)
    {
        sz += pack_size(m_captures[i]);
    }

    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        sz += pack_size(m_transfers[i]);
    }

    std::auto_ptr<e::buffer> new_config(e::buffer::create(sz));
    e::buffer::packer pa = new_config->pack_at(0);
    pa = pa << m_cluster << m_version
            << num_servers
            << uint64_t(m_spaces.size())
            << uint64_t(m_captures.size())
            << uint64_t(m_transfers.size());

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].state == server_state::AVAILABLE)
        {
            pa = pa << m_servers[i].id.get() << m_servers[i].bind_to;
        }
    }

    for (std::map<std::string, std::tr1::shared_ptr<space> >::iterator it = m_spaces.begin();
            it != m_spaces.end(); ++it)
    {
        pa = pa << *it->second;
    }

    for (size_t i = 0; i < m_captures.size(); ++i)
    {
        pa = pa << m_captures[i];
    }

    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        pa = pa << m_transfers[i];
    }

    m_latest_config = new_config;
}
