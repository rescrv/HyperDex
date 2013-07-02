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

// STL
#include <sstream>

// BusyBee
#include <busybee_constants.h>

// HyperDex
#include "admin/admin.h"
#include "admin/pending_perf_counters.h"
#include "admin/pending_string.h"

using hyperdex::admin;

admin :: admin(const char* coordinator, uint16_t port)
    : m_coord(coordinator, port)
    , m_busybee_mapper(m_coord.config())
    , m_busybee(&m_busybee_mapper, 0)
    , m_next_admin_id(1)
    , m_next_server_nonce(1)
    , m_pending_ops()
    , m_failed()
    , m_yieldable()
    , m_yielding()
    , m_pcs()
{
}

admin :: ~admin() throw ()
{
}

int64_t
admin :: dump_config(enum hyperdex_admin_returncode* status,
                     const char** config)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    std::ostringstream ostr;
    m_coord.config()->dump(ostr);
    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<pending> op = new pending_string(id, status, ostr.str(), config);
    m_yieldable.push_back(op);
    return op->admin_visible_id();
}

int
admin :: validate_space(const char* description, enum hyperdex_admin_returncode* status, const char** error_msg)
{
    return -1;
}

int64_t
admin :: add_space(const char* description, enum hyperdex_admin_returncode* status)
{
    return -1;
}

int64_t
admin :: rm_space(const char* name, enum hyperdex_admin_returncode* status)
{
    return -1;
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
        m_pcs->send_perf_reqs(this, m_coord.config(), status);
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

            int64_t admin_id = m_yielding->admin_visible_id();

            if (!m_yielding->can_yield())
            {
                m_yielding = NULL;
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
            m_yielding = psp.op;
            m_failed.pop_front();
            continue;
        }

        assert(!m_pending_ops.empty() || m_pcs);

        if (!maintain_coord_connection(status))
        {
            return -1;
        }

        if (m_pcs)
        {
            int t = m_pcs->millis_to_next_send();

            if (t <= 0)
            {
                m_pcs->send_perf_reqs(this, m_coord.config(), status);
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
                *status = HYPERDEX_ADMIN_POLLFAILED;
                return -1;
            case BUSYBEE_DISRUPTED:
                handle_disruption(id);
                continue;
            case BUSYBEE_TIMEOUT:
                if (m_pcs && timeout != 0)
                {
                    continue;
                }

                *status = HYPERDEX_ADMIN_TIMEOUT;
                return -1;
            case BUSYBEE_INTERRUPTED:
                *status = HYPERDEX_ADMIN_INTERRUPTED;
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
            std::cout << "FUCKER " << __LINE__ << std::endl;
            *status = HYPERDEX_ADMIN_SERVERERROR;
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

        if (msg_type == CONFIGMISMATCH)
        {
            m_failed.push_back(psp);
            continue;
        }

        if (id == it->second.si)
        {
            m_pending_ops.erase(it);

            if (!op->handle_message(this, id, msg_type, msg, up, status))
            {
                return -1;
            }

            m_yielding = psp.op;
        }
        else
        {
            std::cout << "FUCKER " << __LINE__ << std::endl;
            *status = HYPERDEX_ADMIN_SERVERERROR;
            return -1;
        }
    }

    *status = HYPERDEX_ADMIN_NONEPENDING;
    return -1;
}

bool
admin :: maintain_coord_connection(hyperdex_admin_returncode* status)
{
    replicant_returncode rc;

    if (!m_coord.ensure_configuration(&rc))
    {
        switch (rc)
        {
            case REPLICANT_INTERRUPTED:
                *status = HYPERDEX_ADMIN_INTERRUPTED;
                return false;
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_BACKOFF:
                *status = HYPERDEX_ADMIN_COORDFAIL;
                return false;
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_TIMEOUT:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                *status = HYPERDEX_ADMIN_INTERNAL;
                return false;
        }
    }

    if (m_busybee.set_external_fd(m_coord.poll_fd()) < 0)
    {
        *status = HYPERDEX_ADMIN_POLLFAILED;
        return false;
    }

    return true;
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
    const uint64_t version = m_coord.config()->version();
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << type << flags << version << uint64_t(UINT64_MAX) << nonce;
    m_busybee.set_timeout(-1);

    switch (m_busybee.send(id.get(), msg))
    {
        case BUSYBEE_SUCCESS:
            op->handle_sent_to(id);
            m_pending_ops.insert(std::make_pair(nonce, pending_server_pair(id, op)));
            return true;
        case BUSYBEE_DISRUPTED:
            handle_disruption(id);
            return false;
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
            *status = HYPERDEX_ADMIN_POLLFAILED;
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
