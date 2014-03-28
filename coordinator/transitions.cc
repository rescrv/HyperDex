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

#define PROTECT_NULL \
    do \
    { \
        if (!obj) \
        { \
            fprintf(replicant_state_machine_log_stream(ctx), "cannot operate on NULL object\n"); \
            return generate_response(ctx, hyperdex::COORD_UNINITIALIZED); \
        } \
    } \
    while (0)

#define PROTECT_UNINITIALIZED \
    do \
    { \
        PROTECT_NULL; \
        coordinator* c = static_cast<coordinator*>(obj); \
        if (c->cluster() == 0) \
        { \
            fprintf(replicant_state_machine_log_stream(ctx), "cluster not initialized\n"); \
            return generate_response(ctx, hyperdex::COORD_UNINITIALIZED); \
        } \
    } \
    while (0)

#define CHECK_UNPACK(MSGTYPE) \
    do \
    { \
        if (up.error() || up.remain()) \
        { \
            fprintf(log, "received malformed \"" #MSGTYPE "\" message\n"); \
            return generate_response(ctx, hyperdex::COORD_MALFORMED); \
        } \
    } while (0)

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

    if (replicant_state_machine_condition_create(ctx, "ack") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"ack\"\n");
        return NULL;
    }

    if (replicant_state_machine_condition_create(ctx, "stable") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"stable\"\n");
        return NULL;
    }

    if (replicant_state_machine_condition_create(ctx, "checkp") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"checkp\"\n");
        return NULL;
    }

    if (replicant_state_machine_condition_create(ctx, "checkps") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"checkps\"\n");
        return NULL;
    }

    if (replicant_state_machine_condition_create(ctx, "checkpgc") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"checkpgc\"\n");
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
                              const char* data, size_t data_sz)
{
    return coordinator::recreate(ctx, data, data_sz);
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
                              void* obj, const char** data, size_t* data_sz)
{
    PROTECT_NULL;
    coordinator* c = static_cast<coordinator*>(obj);
    c->snapshot(ctx, data, data_sz);
}

void
hyperdex_coordinator_init(struct replicant_state_machine_context* ctx,
                          void* obj, const char* data, size_t data_sz)
{
    PROTECT_NULL;
    coordinator* c = static_cast<coordinator*>(obj);

    std::string id(data, data_sz);
    uint64_t cluster = strtoull(id.c_str(), NULL, 0);
    c->init(ctx, cluster);
}

void
hyperdex_coordinator_read_only(struct replicant_state_machine_context* ctx,
                               void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint8_t set;
    e::unpacker up(data, data_sz);
    up = up >> set;
    CHECK_UNPACK(read_only);
    c->read_only(ctx, set != 0);
}

void
hyperdex_coordinator_fault_tolerance(struct replicant_state_machine_context* ctx,
                                     void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t ft;
    const char* ft_ptr = data + data_sz - sizeof(uint64_t);
    e::unpacker up(ft_ptr, sizeof(uint64_t));
    up = up >> ft;
    CHECK_UNPACK(fault_tolerance);
    c->fault_tolerance(ctx, data, ft);
}

void
hyperdex_coordinator_migrate_data(struct replicant_state_machine_context* ctx,
                                  void *obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);

    char* pos = const_cast<char*>(data);

    uint64_t space_from_sz;
    e::unpacker up_one(pos, sizeof(uint64_t));
    up_one >> space_from_sz;
    pos += sizeof(uint64_t);
    char space_from[space_from_sz + 1];
    memcpy(space_from, pos, space_from_sz);
    pos += space_from_sz;
    space_from[space_from_sz] = '\0';

    uint64_t space_to_sz;
    e::unpacker up_two(pos, sizeof(uint64_t));
    up_two >> space_to_sz;
    pos += sizeof(uint64_t);
    char space_to[space_to_sz + 1];
    memcpy(space_to, pos, space_to_sz);
    space_to[space_to_sz] = '\0';

    c->new_migration(ctx, space_from, space_to);
}

void
hyperdex_coordinator_config_get(struct replicant_state_machine_context* ctx,
                                void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    coordinator* c = static_cast<coordinator*>(obj);
    c->config_get(ctx);
}

void
hyperdex_coordinator_config_ack(struct replicant_state_machine_context* ctx,
                                void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    server_id sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> sid >> version;
    CHECK_UNPACK(config_ack);
    c->config_ack(ctx, sid, version);
}

void
hyperdex_coordinator_config_stable(struct replicant_state_machine_context* ctx,
                                   void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    server_id sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> sid >> version;
    CHECK_UNPACK(config_stable);
    c->config_stable(ctx, sid, version);
}

void
hyperdex_coordinator_server_register(struct replicant_state_machine_context* ctx,
                                     void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    server_id sid;
    po6::net::location bind_to;
    e::unpacker up(data, data_sz);
    up = up >> sid >> bind_to;
    CHECK_UNPACK(server_register);
    c->server_register(ctx, sid, bind_to);
}

void
hyperdex_coordinator_server_online(struct replicant_state_machine_context* ctx,
                                   void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
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
hyperdex_coordinator_server_offline(struct replicant_state_machine_context* ctx,
                                    void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_offline);
    c->server_offline(ctx, sid);
}

void
hyperdex_coordinator_server_shutdown(struct replicant_state_machine_context* ctx,
                                     void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_shutdown);
    c->server_shutdown(ctx, sid);
}

void
hyperdex_coordinator_server_kill(struct replicant_state_machine_context* ctx,
                                 void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_kill);
    c->server_kill(ctx, sid);
}

void
hyperdex_coordinator_server_forget(struct replicant_state_machine_context* ctx,
                                   void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_forget);
    c->server_forget(ctx, sid);
}

void
hyperdex_coordinator_server_suspect(struct replicant_state_machine_context* ctx,
                                    void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_suspect);
    c->server_suspect(ctx, sid);
}

void
hyperdex_coordinator_report_disconnect(struct replicant_state_machine_context* ctx,
                                       void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    server_id sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> sid >> version;
    CHECK_UNPACK(report_disconnect);
    c->report_disconnect(ctx, sid, version);
}

void
hyperdex_coordinator_space_add(struct replicant_state_machine_context* ctx,
                               void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    space s;
    e::unpacker up(data, data_sz);
    up = up >> s;
    CHECK_UNPACK(space_add);
    c->space_add(ctx, s);
}

void
hyperdex_coordinator_space_rm(struct replicant_state_machine_context* ctx,
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

    c->space_rm(ctx, data);
}

void
hyperdex_coordinator_transfer_go_live(struct replicant_state_machine_context* ctx,
                                      void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    transfer_id xid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> xid >> version;
    CHECK_UNPACK(transfer_go_live);
    c->transfer_go_live(ctx, version, xid);
}

void
hyperdex_coordinator_transfer_complete(struct replicant_state_machine_context* ctx,
                                       void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    transfer_id xid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> xid >> version;
    CHECK_UNPACK(transfer_complete);
    c->transfer_complete(ctx, version, xid);
}

void
hyperdex_coordinator_migration_complete(struct replicant_state_machine_context* ctx,
                                        void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    migration_id mid;
    region_id rid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> mid >> rid >> version;
    CHECK_UNPACK(migration_complete);
    c->migration_complete(ctx, version, mid, rid);
}

void
hyperdex_coordinator_checkpoint_stable(struct replicant_state_machine_context* ctx,
                                       void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    server_id sid;
    uint64_t config;
    uint64_t checkpoint;
    e::unpacker up(data, data_sz);
    up = up >> sid >> config >> checkpoint;
    CHECK_UNPACK(checkpoint_stable);
    c->checkpoint_stable(ctx, sid, config, checkpoint);
}

void
hyperdex_coordinator_checkpoints(struct replicant_state_machine_context* ctx,
                                 void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    coordinator* c = static_cast<coordinator*>(obj);
    c->checkpoints(ctx);
}

void
hyperdex_coordinator_alarm(struct replicant_state_machine_context* ctx,
                           void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    coordinator* c = static_cast<coordinator*>(obj);
    c->alarm(ctx);
}

void
hyperdex_coordinator_debug_dump(struct replicant_state_machine_context* ctx,
                                void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    coordinator* c = static_cast<coordinator*>(obj);
    c->debug_dump(ctx);
}

} // extern "C"
