// Copyright (c) 2013, Cornell University
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

// STL
#include <sstream>

// e
#include <e/endian.h>

// BusyBee
#include <busybee_constants.h>

// HyperDex
#include <hyperdex/hyperspace_builder.h>
#include "visibility.h"
#include "common/macros.h"
#include "common/serialization.h"
#include "admin/admin.h"
#include "admin/backup_state_machine.h"
#include "admin/constants.h"
#include "admin/coord_rpc_backup.h"
#include "admin/coord_rpc_generic.h"
#include "admin/hyperspace_builder_internal.h"
#include "admin/pending_perf_counters.h"
#include "admin/pending_raw_backup.h"
#include "admin/pending_string.h"
#include "admin/yieldable.h"

#define ERROR(CODE) \
    *status = HYPERDEX_ADMIN_ ## CODE; \
    m_last_error.set_loc(__FILE__, __LINE__); \
    m_last_error.set_msg()

using hyperdex::admin;

admin :: admin(const char* coordinator, uint16_t port)
    : m_coord(replicant_client_create(coordinator, port))
    , m_busybee_mapper(&m_config)
    , m_busybee(&m_busybee_mapper, 0)
    , m_config()
    , m_config_id(-1)
    , m_config_status()
    , m_config_state(0)
    , m_config_data(NULL)
    , m_config_data_sz(0)
    , m_next_admin_id(1)
    , m_next_server_nonce(1)
    , m_handle_coord_ops(false)
    , m_coord_ops()
    , m_server_ops()
    , m_multi_ops()
    , m_failed()
    , m_yieldable()
    , m_yielding()
    , m_yielded()
    , m_pcs()
    , m_last_error()
{
    m_busybee.set_external_fd(replicant_client_poll_fd(m_coord));
}

admin :: ~admin() throw ()
{
    replicant_client_destroy(m_coord);
}

int64_t
admin :: dump_config(hyperdex_admin_returncode* status,
                     const char** config)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    std::string tmp = m_config.dump();
    e::intrusive_ptr<pending> op = new pending_string(id, status, HYPERDEX_ADMIN_SUCCESS, tmp, config);
    m_yieldable.push_back(op.get());
    return op->admin_visible_id();
}

int64_t
admin :: list_subspaces(const char* space, hyperdex_admin_returncode* status, const char** subspaces)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    std::string tmp = m_config.list_subspaces(space);
    e::intrusive_ptr<pending> op = new pending_string(id, status, HYPERDEX_ADMIN_SUCCESS, tmp, subspaces);
    m_yieldable.push_back(op.get());
    return op->admin_visible_id();
}

int64_t
admin :: read_only(int ro, hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    bool set = ro != 0;
    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, (set ? "set read-only" : "set read-write"));
    char buf[sizeof(uint8_t)];
    buf[0] = set ? 1 : 0;
    int64_t cid = rpc("read_only", buf, sizeof(uint8_t),
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: wait_until_stable(enum hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "wait for stability");
    int64_t cid = replicant_client_cond_wait(m_coord, "hyperdex", "stable", m_config.version(), &op->repl_status, NULL, NULL);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: fault_tolerance(const char* space, uint64_t ft,
                         hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "fault tolerance");
    size_t space_sz = strlen(space);
    std::vector<char> buf(space_sz + sizeof(uint64_t));
    memcpy(&buf[0], space, space_sz);
    e::pack64be(ft, &buf[0] + space_sz);

    int64_t cid = rpc("fault_tolerance", &buf[0], space_sz + sizeof(uint64_t),
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int
admin :: validate_space(const char* description,
                        hyperdex_admin_returncode* status)
{
    struct hyperspace* space_builder = hyperspace_parse(description);

    if (!space_builder)
    {
        ERROR(NOMEM) << "ran out of memory";
        return -1;
    }

    if (hyperspace_error(space_builder))
    {
        ERROR(BADSPACE) << "bad space " << hyperspace_error(space_builder);
        return -1;
    }

    return 0;
}

int64_t
admin :: add_space(const char* description,
                   hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    struct hyperspace* space_builder = hyperspace_parse(description);

    if (!space_builder)
    {
        ERROR(NOMEM) << "ran out of memory";
        return -1;
    }

    if (hyperspace_error(space_builder))
    {
        ERROR(BADSPACE) << "bad space " << hyperspace_error(space_builder);
        return -1;
    }

    hyperdex::space space;

    if (!space_to_space(space_builder, &space))
    {
        ERROR(BADSPACE) << "bad space";
        return -1;
    }

    std::auto_ptr<e::buffer> msg(e::buffer::create(pack_size(space)));
    msg->pack_at(0) << space;

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "add space");
    int64_t cid = rpc("space_add", reinterpret_cast<const char*>(msg->data()), msg->size(),
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: rm_space(const char* name,
                  hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "rm space");
    int64_t cid = rpc("space_rm", name, strlen(name) + 1,
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: mv_space(const char* source, const char* target,
                  hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    const size_t source_sz = strlen(source);
    const size_t target_sz = strlen(target);
    std::vector<char> buf(source_sz + target_sz + 2);
    memmove(&buf[0], source, source_sz);
    buf[source_sz] = '\0';
    memmove(&buf[source_sz + 1], target, target_sz);
    buf[source_sz + 1 + target_sz] = '\0';
    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "mv space");
    int64_t cid = rpc("space_mv", &buf[0], buf.size(),
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: add_index(const char* space, const char* attr,
                   hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    size_t space_sz = strlen(space);
    size_t attr_sz = strlen(attr);
    std::vector<char> buf(space_sz + attr_sz + 2);
    memmove(&buf[0], space, space_sz);
    memmove(&buf[0] + space_sz + 1, attr, attr_sz);
    buf[space_sz] = '\0';
    buf[space_sz + 1 + attr_sz] = '\0';
    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "add_index");
    int64_t cid = rpc("index_add", &buf[0], buf.size(),
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: rm_index(uint64_t idxid,
                  enum hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    char buf[sizeof(uint64_t)];
    e::pack64be(idxid, buf);
    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "rm_index");
    int64_t cid = rpc("index_rm", buf, sizeof(uint64_t),
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: list_indices(const char* space, enum hyperdex_admin_returncode* status,
                            const char** indexes)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    std::string tmp = m_config.list_indices(space);
    e::intrusive_ptr<pending> op = new pending_string(id, status, HYPERDEX_ADMIN_SUCCESS, tmp, indexes);
    m_yieldable.push_back(op.get());
    return op->admin_visible_id();
}

int64_t
admin :: list_spaces(hyperdex_admin_returncode* status,
                     const char** spaces)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    std::string tmp = m_config.list_spaces();
    e::intrusive_ptr<pending> op = new pending_string(id, status, HYPERDEX_ADMIN_SUCCESS, tmp, spaces);
    m_yieldable.push_back(op.get());
    return op->admin_visible_id();
}

int64_t
admin :: server_register(uint64_t token, const char* address,
                         enum hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    server_id sid(token);
    po6::net::location loc;
    loc = po6::net::location(address);

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "register server");
    std::auto_ptr<e::buffer> msg(e::buffer::create(sizeof(uint64_t) + pack_size(loc)));
    msg->pack() << sid << loc;
    int64_t cid = rpc("server_register", reinterpret_cast<const char*>(msg->data()), msg->size(),
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: server_online(uint64_t token, enum hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "bring server online");
    char buf[sizeof(uint64_t)];
    e::pack64be(token, buf);
    int64_t cid = rpc("server_online", buf, sizeof(uint64_t),
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: server_offline(uint64_t token, enum hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "bring server offline");
    char buf[sizeof(uint64_t)];
    e::pack64be(token, buf);
    int64_t cid = rpc("server_offline", buf, sizeof(uint64_t),
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: server_forget(uint64_t token, enum hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "forget server");
    char buf[sizeof(uint64_t)];
    e::pack64be(token, buf);
    int64_t cid = rpc("server_forget", buf, sizeof(uint64_t),
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: server_kill(uint64_t token, enum hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "kill server");
    char buf[sizeof(uint64_t)];
    e::pack64be(token, buf);
    int64_t cid = rpc("server_kill", buf, sizeof(uint64_t),
                      &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: backup(const char* name, enum hyperdex_admin_returncode* status, const char** backups)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<backup_state_machine> op;
    op = new backup_state_machine(name, id, status, backups);

    if (!op->initialize(this, status))
    {
        return -1;
    }

    return op->admin_visible_id();
}

int64_t
admin :: coord_backup(const char* path,
                      enum hyperdex_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_backup(id, status, path);
    int64_t cid = replicant_client_backup_object(m_coord, "hyperdex", &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_replicant_returncode(op->repl_status, status, &m_last_error);
        return -1;
    }
}

int64_t
admin :: raw_backup(const server_id& sid, const char* name,
                    enum hyperdex_admin_returncode* status,
                    const char** path)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    e::slice name_s(name, strlen(name) + 1);
    size_t sz = HYPERDEX_ADMIN_HEADER_SIZE_REQ
              + pack_size(name_s);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_ADMIN_HEADER_SIZE_REQ) << name_s;
    uint64_t id = m_next_admin_id;
    ++m_next_admin_id;
    uint64_t nonce = m_next_server_nonce;
    ++m_next_server_nonce;
    e::intrusive_ptr<pending> op = new pending_raw_backup(id, status, path);

    if (!send(BACKUP, sid, nonce, msg, op, status))
    {
        return -1;
    }

    return op->admin_visible_id();
}

int64_t
admin :: enable_perf_counters(hyperdex_admin_returncode* status,
                              hyperdex_admin_perf_counter* pc)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    if (m_pcs)
    {
        return m_pcs->admin_visible_id();
    }
    else
    {
        int64_t id = m_next_admin_id;
        ++m_next_admin_id;
        m_pcs = new pending_perf_counters(id, status, pc);
        m_pcs->send_perf_reqs(this, &m_config, status);
        return m_pcs->admin_visible_id();
    }
}

void
admin :: disable_perf_counters()
{
    m_pcs = NULL;
}

int64_t
admin :: loop(int timeout, hyperdex_admin_returncode* status)
{
    *status = HYPERDEX_ADMIN_SUCCESS;

    while (m_pcs ||
           m_yielding ||
           !m_failed.empty() ||
           !m_yieldable.empty() ||
           !m_coord_ops.empty() ||
           !m_server_ops.empty())
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

            int64_t admin_id = m_yielding->admin_visible_id();
            m_last_error = m_yielding->error();

            if (!m_yielding->can_yield())
            {
                m_yielded = m_yielding;
                m_yielding = NULL;
            }

            multi_yieldable_map_t::iterator it = m_multi_ops.find(admin_id);

            if (it != m_multi_ops.end())
            {
                e::intrusive_ptr<multi_yieldable> op = it->second;
                m_multi_ops.erase(it);

                if (!op->callback(this, admin_id, status))
                {
                    return -1;
                }

                m_yielding = op.get();
                continue;
            }

            return admin_id;
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
            psp.op->handle_failure(psp.si);
            m_yielding = psp.op.get();
            m_failed.pop_front();
            continue;
        }

        m_yielded = NULL;

        if (!maintain_coord_connection(status))
        {
            return -1;
        }

        assert(!m_coord_ops.empty() || !m_server_ops.empty() || m_pcs);

        if (m_pcs)
        {
            int t = m_pcs->millis_to_next_send();

            if (t <= 0)
            {
                m_pcs->send_perf_reqs(this, &m_config, status);
                t = m_pcs->millis_to_next_send();
            }

            if (timeout > t)
            {
                m_busybee.set_timeout(t);
                timeout -= t;
            }
            else if (timeout < 0)
            {
                m_busybee.set_timeout(t);
            }
            else
            {
                m_busybee.set_timeout(timeout);
                timeout = 0;
            }
        }
        else
        {
            m_busybee.set_timeout(timeout);
        }

        if (m_handle_coord_ops)
        {
            m_handle_coord_ops = false;
            replicant_returncode lrc = REPLICANT_GARBAGE;
            int64_t lid = replicant_client_loop(m_coord, 0, &lrc);

            if (lid < 0 && lrc != REPLICANT_TIMEOUT)
            {
                interpret_replicant_returncode(lrc, status, &m_last_error);
                return -1;
            }

            coord_rpc_map_t::iterator it = m_coord_ops.find(lid);

            if (it == m_coord_ops.end())
            {
                continue;
            }

            e::intrusive_ptr<coord_rpc> op = it->second;
            m_coord_ops.erase(it);

            if (!op->handle_response(this, status))
            {
                return -1;
            }

            m_yielding = op.get();
            continue;
        }

        uint64_t sid_num;
        std::auto_ptr<e::buffer> msg;
        busybee_returncode rc = m_busybee.recv(&sid_num, &msg);
        server_id id(sid_num);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
                ERROR(POLLFAILED) << "poll failed";
                return -1;
            case BUSYBEE_DISRUPTED:
                handle_disruption(id);
                continue;
            case BUSYBEE_TIMEOUT:
                if (m_pcs && timeout != 0)
                {
                    continue;
                }

                ERROR(TIMEOUT) << "operation timed out";
                return -1;
            case BUSYBEE_INTERRUPTED:
                ERROR(INTERRUPTED) << "signal received";
                return -1;
            case BUSYBEE_EXTERNAL:
                m_handle_coord_ops = true;
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
            ERROR(SERVERERROR) << "communication error: server "
                               << sid_num << " sent message="
                               << msg->as_slice().hex()
                               << " with invalid header";
            return -1;
        }

        network_msgtype msg_type = static_cast<network_msgtype>(mt);
        pending_map_t::iterator it = m_server_ops.find(nonce);

        if (it == m_server_ops.end())
        {
            continue;
        }

        const pending_server_pair psp(it->second);
        e::intrusive_ptr<pending> op = psp.op;

        if (msg_type == CONFIGMISMATCH)
        {
            m_failed.push_back(psp);
            continue;
        }

        if (id == it->second.si)
        {
            m_server_ops.erase(it);

            if (!op->handle_message(this, id, msg_type, msg, up, status))
            {
                return -1;
            }

            m_yielding = psp.op.get();
        }
        else
        {
            ERROR(SERVERERROR) << "server " << sid_num
                               << " responded for nonce " << nonce
                               << " which belongs to server " << it->second.si.get();
            return -1;
        }
    }

    ERROR(NONEPENDING) << "no outstanding operations to process";
    return -1;
}

const char*
admin :: error_message()
{
    return m_last_error.msg();
}

const char*
admin :: error_location()
{
    return m_last_error.loc();
}

void
admin :: set_error_message(const char* msg)
{
    m_last_error = e::error();
    m_last_error.set_loc(__FILE__, __LINE__);
    m_last_error.set_msg() << msg;
}

#define INTERPRET_ERROR(CODE) \
    *status = HYPERDEX_ADMIN_ ## CODE; \
    err->set_loc(__FILE__, __LINE__); \
    err->set_msg()

void
admin :: interpret_replicant_returncode(replicant_returncode rstatus,
                                        hyperdex_admin_returncode* status,
                                        e::error* err)
{
    switch (rstatus)
    {
        case REPLICANT_SUCCESS:
            *status = HYPERDEX_ADMIN_SUCCESS;
            break;
        case REPLICANT_OBJ_NOT_FOUND:
        case REPLICANT_OBJ_EXIST:
        case REPLICANT_FUNC_NOT_FOUND:
        case REPLICANT_COND_NOT_FOUND:
        case REPLICANT_COND_DESTROYED:
            INTERPRET_ERROR(COORDFAIL) << "persistent coordinator error: "
                                       << replicant_client_error_message(m_coord);
            break;
        case REPLICANT_MAYBE:
            INTERPRET_ERROR(COORDFAIL) << "transient coordinator error: "
                                       << "operation may or may not have completed";
            break;
        case REPLICANT_SEE_ERRNO:
            INTERPRET_ERROR(COORDFAIL) << "transient coordinator error: "
                                       << po6::strerror(errno);
            break;
        case REPLICANT_CLUSTER_JUMP:
        case REPLICANT_SERVER_ERROR:
        case REPLICANT_COMM_FAILED:
            INTERPRET_ERROR(COORDFAIL) << "transient coordinator error: "
                                       << replicant_client_error_message(m_coord);
            break;
        case REPLICANT_TIMEOUT:
            INTERPRET_ERROR(TIMEOUT) << "operation timed out";
            break;
        case REPLICANT_INTERRUPTED:
            INTERPRET_ERROR(INTERRUPTED) << "signal received";
            break;
        case REPLICANT_NONE_PENDING:
            INTERPRET_ERROR(NONEPENDING) << "no operations pending";
            break;
        case REPLICANT_INTERNAL:
        case REPLICANT_EXCEPTION:
        case REPLICANT_GARBAGE:
        default:
            INTERPRET_ERROR(INTERNAL) << "internal library error: "
                                      << replicant_client_error_message(m_coord);
            break;
    }
}

bool
admin :: maintain_coord_connection(hyperdex_admin_returncode* status)
{
    if (m_config_status != REPLICANT_SUCCESS)
    {
        replicant_client_kill(m_coord, m_config_id);
        m_config_id = -1;
    }

    replicant_returncode rc;

    if (m_config_id < 0)
    {
        m_config_status = REPLICANT_SUCCESS;
        m_config_id = replicant_client_cond_follow(m_coord, "hyperdex", "config",
                                                   &m_config_status, &m_config_state,
                                                   &m_config_data, &m_config_data_sz);
        if (replicant_client_wait(m_coord, m_config_id, -1, &rc) < 0)
        {
            ERROR(COORDFAIL) << "coordinator failure: " << replicant_client_error_message(m_coord);
            return false;
        }
    }

    if (replicant_client_wait(m_coord, m_config_id, 0, &rc) < 0)
    {
        if (rc == REPLICANT_INTERRUPTED)
        {
            ERROR(INTERRUPTED) << "interrupted by a signal";
            return false;
        }
        else if (rc != REPLICANT_NONE_PENDING && rc != REPLICANT_TIMEOUT)
        {
            ERROR(COORDFAIL) << "coordinator failure: " << replicant_client_error_message(m_coord);
            return false;
        }
    }

    if (m_config.version() < m_config_state)
    {
        configuration new_config;
        e::unpacker up(m_config_data, m_config_data_sz);
        up = up >> new_config;

        if (!up.error())
        {
            m_config = new_config;
        }
    }

    return true;
}

int64_t
admin :: rpc(const char* func,
             const char* data, size_t data_sz,
             replicant_returncode* status,
             char** output, size_t* output_sz)
{
    return replicant_client_call(m_coord, "hyperdex", func, data, data_sz,
                                 REPLICANT_CALL_ROBUST, status, output, output_sz);
}

bool
admin :: send(network_msgtype mt,
              server_id id,
              uint64_t nonce,
              std::auto_ptr<e::buffer> msg,
              e::intrusive_ptr<pending> op,
              hyperdex_admin_returncode* status)
{
    const uint8_t type = static_cast<uint8_t>(mt);
    const uint8_t flags = 0;
    const uint64_t version = m_config.version();
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << type << flags << version << uint64_t(UINT64_MAX) << nonce;
    m_busybee.set_timeout(-1);

    switch (m_busybee.send(id.get(), msg))
    {
        case BUSYBEE_SUCCESS:
            op->handle_sent_to(id);
            m_server_ops.insert(std::make_pair(nonce, pending_server_pair(id, op)));
            return true;
        case BUSYBEE_DISRUPTED:
            handle_disruption(id);
            ERROR(SERVERERROR) << "server " << id.get() << " had a communication disruption";
            return false;
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
            ERROR(POLLFAILED) << "poll failed";
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        case BUSYBEE_INTERRUPTED:
        default:
            abort();
    }
}

void
admin :: handle_disruption(const server_id& si)
{
    pending_map_t::iterator it = m_server_ops.begin();

    while (it != m_server_ops.end())
    {
        if (it->second.si == si)
        {
            m_failed.push_back(it->second);
            pending_map_t::iterator tmp = it;
            ++it;
            m_server_ops.erase(tmp);
        }
        else
        {
            ++it;
        }
    }

    m_busybee.drop(si.get());
}

HYPERDEX_API std::ostream&
operator << (std::ostream& lhs, hyperdex_admin_returncode rhs)
{
    switch (rhs)
    {
        STRINGIFY(HYPERDEX_ADMIN_SUCCESS);
        STRINGIFY(HYPERDEX_ADMIN_NOMEM);
        STRINGIFY(HYPERDEX_ADMIN_NONEPENDING);
        STRINGIFY(HYPERDEX_ADMIN_POLLFAILED);
        STRINGIFY(HYPERDEX_ADMIN_TIMEOUT);
        STRINGIFY(HYPERDEX_ADMIN_INTERRUPTED);
        STRINGIFY(HYPERDEX_ADMIN_SERVERERROR);
        STRINGIFY(HYPERDEX_ADMIN_COORDFAIL);
        STRINGIFY(HYPERDEX_ADMIN_BADSPACE);
        STRINGIFY(HYPERDEX_ADMIN_DUPLICATE);
        STRINGIFY(HYPERDEX_ADMIN_NOTFOUND);
        STRINGIFY(HYPERDEX_ADMIN_LOCALERROR);
        STRINGIFY(HYPERDEX_ADMIN_INTERNAL);
        STRINGIFY(HYPERDEX_ADMIN_EXCEPTION);
        STRINGIFY(HYPERDEX_ADMIN_GARBAGE);
        default:
            lhs << "unknown hyperdex_admin_returncode";
    }

    return lhs;
}
