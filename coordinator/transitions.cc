// Copyright (c) 2012-2013, Cornell University
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
#include <new>

// STL
#include <string>

// HyperDex
#include "common/coordinator_returncode.h"
#include "common/ids.h"
#include "common/serialization.h"
#include "coordinator/coordinator.h"
#include "coordinator/transitions.h"
#include "coordinator/util.h"

using namespace hyperdex;

#define PROTECT_UNINITIALIZED \
    coordinator* c = static_cast<coordinator*>(obj); \
    do \
    { \
        if (c->cluster() == 0) \
        { \
            rsm_log(ctx, "cluster not initialized\n"); \
            return generate_response(ctx, hyperdex::COORD_UNINITIALIZED); \
        } \
    } \
    while (0)

#define CHECK_UNPACK(MSGTYPE) \
    do \
    { \
        if (up.error() || up.remain()) \
        { \
            rsm_log(ctx, "received malformed \"" #MSGTYPE "\" message\n"); \
            return generate_response(ctx, hyperdex::COORD_MALFORMED); \
        } \
    } while (0)

extern "C"
{

void*
hyperdex_coordinator_create(struct rsm_context* ctx)
{
    rsm_cond_create(ctx, "config");
    rsm_cond_create(ctx, "ack");
    rsm_cond_create(ctx, "stable");
    rsm_cond_create(ctx, "checkpoint");
    return new (std::nothrow) coordinator();
}

void*
hyperdex_coordinator_recreate(struct rsm_context* ctx,
                              const char* data, size_t data_sz)
{
    return coordinator::recreate(ctx, data, data_sz);
}

int
hyperdex_coordinator_snapshot(struct rsm_context* ctx,
                              void* obj, char** data, size_t* data_sz)
{
    coordinator* c = static_cast<coordinator*>(obj);
    return c->snapshot(ctx, data, data_sz);
}

void
hyperdex_coordinator_init(struct rsm_context* ctx,
                          void* obj, const char* data, size_t data_sz)
{
    coordinator* c = static_cast<coordinator*>(obj);

    std::string id(data, data_sz);
    uint64_t cluster = strtoull(id.c_str(), NULL, 0);
    c->init(ctx, cluster);
}

void
hyperdex_coordinator_read_only(struct rsm_context* ctx,
                               void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    uint8_t set;
    e::unpacker up(data, data_sz);
    up = up >> set;
    CHECK_UNPACK(read_only);
    c->read_only(ctx, set != 0);
}

void
hyperdex_coordinator_fault_tolerance(struct rsm_context* ctx,
                                     void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    uint64_t ft;
    const char* ft_ptr = data + data_sz - sizeof(uint64_t);
    e::unpacker up(ft_ptr, sizeof(uint64_t));
    up = up >> ft;
    CHECK_UNPACK(fault_tolerance);
    c->fault_tolerance(ctx, data, ft);
}

void
hyperdex_coordinator_config_get(struct rsm_context* ctx,
                                void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    c->config_get(ctx);
}

void
hyperdex_coordinator_config_ack(struct rsm_context* ctx,
                                void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    server_id sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> sid >> version;
    CHECK_UNPACK(config_ack);
    c->config_ack(ctx, sid, version);
}

void
hyperdex_coordinator_config_stable(struct rsm_context* ctx,
                                   void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    server_id sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> sid >> version;
    CHECK_UNPACK(config_stable);
    c->config_stable(ctx, sid, version);
}

void
hyperdex_coordinator_server_register(struct rsm_context* ctx,
                                     void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    server_id sid;
    po6::net::location bind_to;
    e::unpacker up(data, data_sz);
    up = up >> sid >> bind_to;
    CHECK_UNPACK(server_register);
    c->server_register(ctx, sid, bind_to);
}

void
hyperdex_coordinator_server_online(struct rsm_context* ctx,
                                   void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    server_id sid;
    po6::net::location bind_to;
    e::unpacker up(data, data_sz);
    up = up >> sid;

    if (!up.error() && !up.remain())
    {
        c->server_online(ctx, sid, NULL);
    }
    else
    {
        up = up >> bind_to;
        CHECK_UNPACK(server_online);
        c->server_online(ctx, sid, &bind_to);
    }
}

void
hyperdex_coordinator_server_offline(struct rsm_context* ctx,
                                    void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_offline);
    c->server_offline(ctx, sid);
}

void
hyperdex_coordinator_server_shutdown(struct rsm_context* ctx,
                                     void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_shutdown);
    c->server_shutdown(ctx, sid);
}

void
hyperdex_coordinator_server_kill(struct rsm_context* ctx,
                                 void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_kill);
    c->server_kill(ctx, sid);
}

void
hyperdex_coordinator_server_forget(struct rsm_context* ctx,
                                   void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_forget);
    c->server_forget(ctx, sid);
}

void
hyperdex_coordinator_server_suspect(struct rsm_context* ctx,
                                    void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_suspect);
    c->server_suspect(ctx, sid);
}

void
hyperdex_coordinator_report_disconnect(struct rsm_context* ctx,
                                       void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    server_id sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> sid >> version;
    CHECK_UNPACK(report_disconnect);
    c->report_disconnect(ctx, sid, version);
}

void
hyperdex_coordinator_space_add(struct rsm_context* ctx,
                               void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    space s;
    e::unpacker up(data, data_sz);
    up = up >> s;
    CHECK_UNPACK(space_add);
    c->space_add(ctx, s);
}

void
hyperdex_coordinator_space_rm(struct rsm_context* ctx,
                              void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;

    if (data_sz == 0 || data[data_sz - 1] != '\0')
    {
        rsm_log(ctx, "received malformed \"rm_space\" message\n");
        return generate_response(ctx, COORD_MALFORMED);
    }

    c->space_rm(ctx, data);
}

void
hyperdex_coordinator_space_mv(struct rsm_context* ctx,
                              void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;

    if (data_sz < 2 || data[data_sz - 1] != '\0')
    {
        rsm_log(ctx, "received malformed \"mv_space\" message\n");
        return generate_response(ctx, COORD_MALFORMED);
    }

    const char* src = data;
    const char* dst = data + strlen(data) + 1;

    if (dst >= data + data_sz)
    {
        rsm_log(ctx, "received malformed \"mv_space\" message\n");
        return generate_response(ctx, COORD_MALFORMED);
    }

    c->space_mv(ctx, src, dst);
}

void
hyperdex_coordinator_index_add(struct rsm_context* ctx,
                               void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    const char* space = data;
    size_t space_sz = strnlen(data, data_sz);

    if (data_sz == 0 || data[data_sz - 1] != '\0' || space_sz + 2 >= data_sz)
    {
        rsm_log(ctx, "received malformed \"add_index\" message\n");
        return generate_response(ctx, COORD_MALFORMED);
    }

    const char* attr = data + space_sz + 1;
    size_t attr_sz = strnlen(attr, data_sz - space_sz - 1);

    if (space_sz + attr_sz + 2 != data_sz)
    {
        rsm_log(ctx, "received malformed \"add_index\" message\n");
        return generate_response(ctx, COORD_MALFORMED);
    }

    c->index_add(ctx, space, attr);
}

void
hyperdex_coordinator_index_rm(struct rsm_context* ctx,
                              void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    index_id ii;
    e::unpacker up(data, data_sz);
    up = up >> ii;
    CHECK_UNPACK(index_rm);
    c->index_rm(ctx, ii);
}

void
hyperdex_coordinator_transfer_go_live(struct rsm_context* ctx,
                                      void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    transfer_id xid;
    e::unpacker up(data, data_sz);
    up = up >> xid;
    CHECK_UNPACK(transfer_go_live);
    c->transfer_go_live(ctx, xid);
}

void
hyperdex_coordinator_transfer_complete(struct rsm_context* ctx,
                                       void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    transfer_id xid;
    e::unpacker up(data, data_sz);
    up = up >> xid;
    CHECK_UNPACK(transfer_complete);
    c->transfer_complete(ctx, xid);
}

void
hyperdex_coordinator_checkpoint_stable(struct rsm_context* ctx,
                                       void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    server_id sid;
    uint64_t config;
    uint64_t checkpoint;
    e::unpacker up(data, data_sz);
    up = up >> sid >> config >> checkpoint;
    CHECK_UNPACK(checkpoint_stable);
    c->checkpoint_stable(ctx, sid, config, checkpoint);
}

void
hyperdex_coordinator_checkpoints(struct rsm_context* ctx,
                                 void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    c->checkpoints(ctx);
}

void
hyperdex_coordinator_periodic(struct rsm_context* ctx,
                              void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    c->periodic(ctx);
}

void
hyperdex_coordinator_debug_dump(struct rsm_context* ctx,
                                void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    c->debug_dump(ctx);
}

} // extern "C"
