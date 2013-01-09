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

extern "C"
{

void*
hyperdex_coordinator_create(struct replicant_state_machine_context* ctx)
{
    if (replicant_state_machine_condition_create(ctx, "config") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "condition creation failed\n");
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
    CHECK_UNPACK(xfer_go_live);
    transfer_id xid(_xid);
    c->xfer_go_live(ctx, xid);
}

} // extern "C"

/////////////////////////////// Coordinator Class //////////////////////////////

coordinator :: coordinator()
    : m_cluster(0)
    , m_version(0)
    , m_counter(1)
    , m_servers()
    , m_spaces()
    , m_latest_config()
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
        srand48_r(m_cluster, &m_seed);
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
            s->subspaces[i].regions[j].cid = capture_id();
            s->subspaces[i].regions[j].tid = transfer_id();
            s->subspaces[i].regions[j].tsi = server_id();
            s->subspaces[i].regions[j].tvi = virtual_server_id();
        }
    }

    m_spaces.insert(std::make_pair(std::string(s->name), s));
    maintain_layout(ctx, s.get());
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

    m_servers.push_back(server_state(sid, bind_to));
    std::stable_sort(m_servers.begin(), m_servers.end());
    fprintf(log, "registered server_id(%lu) on address %s\n", sid.get(), oss.str().c_str());
    return generate_response(ctx, hyperdex::COORD_SUCCESS);
}

void
coordinator :: server_suspect(replicant_state_machine_context* ctx,
                              const server_id& sid, uint64_t version)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    fprintf(log, "server_id(%lu) suspected in version %lu\n", sid.get(), version);
    // XXX
#if 0
    server_id sid(_sid);

    for (std::list<space>::iterator s = c->spaces.begin(); s != c->spaces.end(); ++s)
    {
        for (size_t i = 0; i < s->subspaces.size(); ++i)
        {
            for (size_t j = 0; j < s->subspaces[i].regions.size(); ++j)
            {
                size_t k = 0;

                while (k < s->subspaces[i].regions[j].replicas.size())
                {
                    if (s->subspaces[i].regions[j].replicas[k].si == sid)
                    {
                        for (size_t r = k; r + 1 < s->subspaces[i].regions[j].replicas.size(); ++r)
                        {
                            s->subspaces[i].regions[j].replicas[r] = s->subspaces[i].regions[j].replicas[r + 1];
                        }

                        s->subspaces[i].regions[j].replicas.pop_back();
                    }
                    else
                    {
                        ++k;
                    }
                }

                if (s->subspaces[i].regions[j].replicas.empty())
                {
                    fprintf(replicant_state_machine_log_stream(ctx), "kill completely emptied a region\n");
                }
            }
        }
    }

#endif
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

    if (reg->tid != transfer_id())
    {
        fprintf(log, "cannot begin transfer of region_id(%lu) "
                     "to server_id(%lu) because transfer_id(%lu) "
                     "is already in progress\n", rid.get(), sid.get(), reg->tid.get());
        return generate_response(ctx, COORD_TRANSFER_IN_PROGRESS);
    }

    if (reg->cid == capture_id())
    {
        reg->cid = capture_id(m_counter);
        ++m_counter;
    }

    // XXX make sure sid not already in region

    reg->tid = transfer_id(m_counter);
    ++m_counter;
    reg->tsi = sid;
    reg->tvi = virtual_server_id(m_counter);
    ++m_counter;
    fprintf(log, "successfully began transfer of region_id(%lu) "
                 "to server_id(%lu) using capture_id(%lu)/transfer_id(%lu)/"
                 "virtual_server_id(%lu)\n",
                 rid.get(), sid.get(), reg->cid.get(), reg->tid.get(), reg->tvi.get());
    issue_new_config(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: xfer_go_live(replicant_state_machine_context* ctx,
                            const transfer_id& xid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    region* reg = get_region(xid);

    if (!reg)
    {
        fprintf(log, "cannot make transfer_id(%lu) live because it doesn't exist\n", xid.get());
        return generate_response(ctx, COORD_SUCCESS);
    }

    if (reg->replicas.empty())
    {
        fprintf(log, "cannot make transfer_id(%lu) live because no replicas are live\n", xid.get());
        return generate_response(ctx, COORD_SUCCESS);
    }

    if (reg->replicas.back().si == reg->tsi)
    {
        return generate_response(ctx, COORD_SUCCESS);
    }

    reg->replicas.push_back(replica());
    reg->replicas.back().si = reg->tsi;
    reg->replicas.back().vsi = reg->tvi;
    fprintf(log, "transfer_id(%lu) is now live\n", xid.get());
    issue_new_config(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: xfer_complete(replicant_state_machine_context* ctx,
                             const transfer_id& xid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    region* reg = get_region(xid);

    if (!reg)
    {
        fprintf(log, "cannot complete transfer_id(%lu) because it doesn't exist\n", xid.get());
        return generate_response(ctx, COORD_SUCCESS);
    }

    if (reg->replicas.empty())
    {
        fprintf(log, "cannot complete transfer_id(%lu) because no replicas are live\n", xid.get());
        return generate_response(ctx, COORD_SUCCESS);
    }

    if (reg->replicas.back().si != reg->tsi)
    {
        fprintf(log, "cannot complete transfer_id(%lu) because it is not live\n", xid.get());
        return generate_response(ctx, COORD_SUCCESS);
    }

    reg->cid = capture_id(); // XXX don't always reset capture ID
    reg->tid = transfer_id();
    reg->tsi = server_id();
    reg->tvi = virtual_server_id();
    fprintf(log, "transfer_id(%lu) is now complete\n", xid.get());
    issue_new_config(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

bool
coordinator :: is_registered(const server_id& sid)
{
    std::vector<server_state>::iterator it;
    it = std::lower_bound(m_servers.begin(), m_servers.end(), sid);
    return it != m_servers.end() && it->id == sid;
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

region*
coordinator :: get_region(const transfer_id& xid)
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
                if (ss.regions[j].tid == xid)
                {
                    return &ss.regions[j];
                }
            }
        }
    }

    return NULL;
}

void
coordinator :: maintain_layout(struct replicant_state_machine_context* ctx,
                               space* s)
{
    for (size_t i = 0; i < s->subspaces.size(); ++i)
    {
        subspace& ss(s->subspaces[i]);

        for (size_t j = 0; j < ss.regions.size(); ++j)
        {
            region& reg(ss.regions[j]);

            while (reg.replicas.size() < std::min(s->fault_tolerance + 1, m_servers.size()))
            {
                long int y;
                lrand48_r(&m_seed, &y);
                server_id sid(m_servers[y % m_servers.size()].id);
                bool found = false;

                for (size_t k = 0; k < reg.replicas.size(); ++k)
                {
                    if (sid == reg.replicas[k].si)
                    {
                        found = true;
                    }
                }

                if (!found)
                {
                    reg.replicas.push_back(replica());
                    reg.replicas.back().si = sid;
                    reg.replicas.back().vsi = virtual_server_id(m_counter);
                    ++m_counter;
                }
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
coordinator :: issue_new_config(struct replicant_state_machine_context* ctx)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    uint64_t cond_state;

    if (replicant_state_machine_condition_broadcast(ctx, "config", &cond_state) < 0)
    {
        fprintf(log, "could not broadcast on \"config\" condition\n");
    }

    ++m_version;
    fprintf(log, "issuing new configuration version %lu\n", m_version);
    m_latest_config.reset();
}

void
coordinator :: regenerate_cached(struct replicant_state_machine_context*)
{
    size_t sz = 4 * sizeof(uint64_t);

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        sz += sizeof(uint64_t) + pack_size(m_servers[i].bind_to);
    }

    for (std::map<std::string, std::tr1::shared_ptr<space> >::iterator it = m_spaces.begin();
            it != m_spaces.end(); ++it)
    {
        sz += pack_size(*it->second);
    }

    std::auto_ptr<e::buffer> new_config(e::buffer::create(sz));
    e::buffer::packer pa = new_config->pack_at(0);
    pa = pa << m_cluster << m_version << uint64_t(m_servers.size()) << uint64_t(m_spaces.size());

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        pa = pa << m_servers[i].id.get() << m_servers[i].bind_to;
    }

    for (std::map<std::string, std::tr1::shared_ptr<space> >::iterator it = m_spaces.begin();
            it != m_spaces.end(); ++it)
    {
        pa = pa << *it->second;
    }

    m_latest_config = new_config;
}
