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

// POSIX
#include <signal.h>

// Google Log
#include <glog/logging.h>

// e
#include <e/endian.h>

// HyperDex
#include "common/coordinator_returncode.h"
#include "common/serialization.h"
#include "daemon/coordinator_link.h"
#include "daemon/daemon.h"

using hyperdex::coordinator_link;

coordinator_link :: coordinator_link(daemon* d)
    : m_daemon(d)
    , m_looper(pthread_self())
    , m_repl()
    , m_state(NORMAL)
    , m_wait_config_id(-1)
    , m_wait_config_status(REPLICANT_GARBAGE)
    , m_get_config_id(-1)
    , m_get_config_status(REPLICANT_GARBAGE)
    , m_get_config_output(NULL)
    , m_get_config_output_sz(0)
    , m_shutdown1_id(-1)
    , m_shutdown1_status(REPLICANT_GARBAGE)
    , m_shutdown1_output(NULL)
    , m_shutdown1_output_sz(0)
    , m_wait_acked_id(-1)
    , m_wait_acked_status(REPLICANT_GARBAGE)
    , m_shutdown2_id(-1)
    , m_shutdown2_status(REPLICANT_GARBAGE)
    , m_shutdown2_output(NULL)
    , m_shutdown2_output_sz(0)
    , m_acks()
    , m_transfers_go_live()
    , m_transfers_complete()
    , m_tcp_disconnects()
    , m_transfers_go_live_seen()
    , m_transfers_complete_seen()
    , m_tcp_disconnects_seen()
    , m_protect_queues()
    , m_queue_transfers_go_live()
    , m_queue_transfers_complete()
    , m_queue_tcp_disconnects()
{
}

coordinator_link :: ~coordinator_link() throw ()
{
}

void
coordinator_link :: set_coordinator_address(const char* host, uint16_t port)
{
    m_repl.reset(new replicant_client(host, port));
}

// negative indicates fatal error; zero indicates retry; positive indicates success
int
coordinator_link :: register_id(server_id us, const po6::net::location& bind_to)
{
    replicant_returncode sstatus = REPLICANT_GARBAGE;
    std::auto_ptr<e::buffer> data(e::buffer::create(sizeof(uint64_t) + pack_size(bind_to)));
    *data << us.get() << bind_to;
    const char* output;
    size_t output_sz;
    int64_t sid = m_repl->send("hyperdex", "server-register",
                               reinterpret_cast<const char*>(data->data()), data->size(),
                               &sstatus, &output, &output_sz);

    if (sid < 0)
    {
        switch (sstatus)
        {
            case REPLICANT_NEED_BOOTSTRAP:
                LOG(ERROR) << "could not connect to the coordinator to register this instance";
                return -1;
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_TIMEOUT:
            case REPLICANT_BACKOFF:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_INTERRUPTED:
            case REPLICANT_GARBAGE:
            default:
                LOG(ERROR) << "while trying to register with the coordinator "
                           << "we received an error (" << sstatus << ") and "
                           << "don't know what to do (exiting is a safe bet)";
                return -1;
        }
    }

    replicant_returncode lstatus = REPLICANT_GARBAGE;
    int64_t lid = m_repl->loop(sid, -1, &lstatus);

    if (lid < 0)
    {
        switch (sstatus)
        {
            case REPLICANT_NEED_BOOTSTRAP:
                LOG(ERROR) << "could not connect to the coordinator to register this instance";
                return -1;
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_TIMEOUT:
            case REPLICANT_BACKOFF:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_INTERRUPTED:
            case REPLICANT_GARBAGE:
            default:
                LOG(ERROR) << "could not register this instance with the coordinator: " << sstatus;
                return -1;
        }
    }

    assert(sid == lid);

    switch (sstatus)
    {
        case REPLICANT_SUCCESS:
            break;
        case REPLICANT_FUNC_NOT_FOUND:
            LOG(ERROR) << "could not register this instance with the coordinator "
                       << "because the registration function was not found";
            return -1;
        case REPLICANT_OBJ_NOT_FOUND:
            LOG(ERROR) << "could not register this instance with the coordinator "
                       << "because the HyperDex object was not found";
            return -1;
        case REPLICANT_NEED_BOOTSTRAP:
        case REPLICANT_BACKOFF:
            LOG(ERROR) << "could not connect to the coordinator to register this instance";
            return -1;
        case REPLICANT_NAME_TOO_LONG:
        case REPLICANT_OBJ_EXIST:
        case REPLICANT_COND_NOT_FOUND:
        case REPLICANT_COND_DESTROYED:
        case REPLICANT_SERVER_ERROR:
        case REPLICANT_BAD_LIBRARY:
        case REPLICANT_TIMEOUT:
        case REPLICANT_MISBEHAVING_SERVER:
        case REPLICANT_INTERNAL_ERROR:
        case REPLICANT_NONE_PENDING:
        case REPLICANT_INTERRUPTED:
        case REPLICANT_GARBAGE:
        default:
            LOG(ERROR) << "could not register this instance with the coordinator: " << sstatus;
            return -1;
    }

    int ret = 0;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case COORD_SUCCESS:
                ret = 1;
                break;
            case COORD_DUPLICATE:
                ret = 0;
                break;
            case COORD_UNINITIALIZED:
                ret = -1;
                LOG(ERROR) << "could not register this instance with the coordinator "
                           << "because the coordinator is uninitialized";
                break;
            case COORD_MALFORMED:
            case COORD_NOT_FOUND:
            case COORD_INITIALIZED:
            case COORD_TRANSFER_IN_PROGRESS:
            default:
                ret = -1;
                LOG(ERROR) << "could not register this instance with the coordinator "
                           << "because of an internal error";
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return ret;
}

extern int s_interrupts;
extern bool s_alarm;

bool
coordinator_link :: is_shutdown()
{
    return s_interrupts > 1 ||
           m_state == DIRTY_SHUTDOWN ||
           m_state == CLEAN_SHUTDOWN;
}

bool
coordinator_link :: is_clean_shutdown()
{
    return m_state == CLEAN_SHUTDOWN;
}

bool
coordinator_link :: wait_for_config(configuration* config)
{
    uint64_t retry = 1000000UL;
    bool need_to_backoff = false;

    while (!is_shutdown())
    {
        if (s_alarm)
        {
            alarm(30);
            s_alarm = false;
            m_daemon->m_repl.trip_periodic();
            need_to_backoff = false;
        }

        if (need_to_backoff)
        {
            LOG(ERROR) << "connection to the coordinator failed; retrying in " << retry / 1000000. << " milliseconds";
            uint64_t rem = retry;
            int interrupts = s_interrupts;
            timespec ts;

            while (rem > 0)
            {
                ts.tv_sec = 0;
                ts.tv_nsec = std::min(static_cast<uint64_t>(10000000), rem);

                sigset_t empty_signals;
                sigset_t old_signals;
                sigemptyset(&empty_signals); // should never fail
                pthread_sigmask(SIG_SETMASK, &empty_signals, &old_signals); // should never fail
                nanosleep(&ts, NULL); // nothing to gain by checking output
                pthread_sigmask(SIG_SETMASK, &old_signals, NULL); // should never fail

                if (interrupts < s_interrupts)
                {
                    break;
                }

                rem -= ts.tv_nsec;
            }

            need_to_backoff = false;

            if (interrupts < s_interrupts)
            {
                continue;
            }

            retry = std::min(retry * 2, 1000000000UL);
            continue; // so we can pick up on changes to s_interrupts and break;
        }

        need_to_backoff = true;

        if (m_wait_config_id < 0 && m_get_config_id < 0)
        {
            if (initiate_wait_for_config())
            {
                need_to_backoff = false;
            }

            continue;
        }

        if (s_interrupts > 0 && m_shutdown1_id < 0 && m_wait_acked_id < 0)
        {
            char data[sizeof(uint64_t)];
            e::pack64be(m_daemon->m_us.get(), data);
            m_shutdown1_id = m_repl->send("hyperdex", "server-shutdown1", data, sizeof(uint64_t),
                                          &m_shutdown1_status,
                                          &m_shutdown1_output, &m_shutdown1_output_sz);

            if (m_shutdown1_id < 0)
            {
                LOG(ERROR) << "dirty shutdown because shutdown1 failed: "
                           << m_repl->last_error_desc()
                           << "(" << m_get_config_status << ";"
                           << m_repl->last_error_file() << ":"
                           << m_repl->last_error_line() << ")";
                m_state = DIRTY_SHUTDOWN;
            }

            need_to_backoff = false;
            continue;
        }

        {
            po6::threads::mutex::hold hold(&m_protect_queues);

            while (!m_queue_transfers_go_live.empty())
            {
                initiate_transfer_go_live(m_queue_transfers_go_live.front());
                m_queue_transfers_go_live.pop();
            }

            while (!m_queue_transfers_complete.empty())
            {
                initiate_transfer_complete(m_queue_transfers_complete.front());
                m_queue_transfers_complete.pop();
            }

            while (!m_queue_tcp_disconnects.empty())
            {
                initiate_report_tcp_disconnect(m_queue_tcp_disconnects.front());
                m_queue_tcp_disconnects.pop();
            }
        }

        int timeout = s_interrupts > 0 ? 10 * 1000: -1;
        replicant_returncode lstatus = REPLICANT_GARBAGE;
        int64_t lid = m_repl->loop(timeout, &lstatus);

        if (lid < 0)
        {
            switch (lstatus)
            {
                case REPLICANT_TIMEOUT:
                    if (timeout < 0)
                    {
                        LOG(ERROR) << "replicant returned REPLICANT_TIMEOUT when we requested an infinite timeout: "
                                   << m_repl->last_error_desc()
                                   << "(" << lstatus << ";"
                                   << m_repl->last_error_file() << ":"
                                   << m_repl->last_error_line() << ")";
                    }
                    else
                    {
                        LOG(ERROR) << "could not shutdown cleanly within " << (timeout / 1000. ) << " seconds; "
                                   << "performing a dirty shutdown";
                    }
                    m_state = DIRTY_SHUTDOWN;
                    break;
                case REPLICANT_INTERRUPTED:
                    need_to_backoff = false;
                    break;
                case REPLICANT_BACKOFF:
                    need_to_backoff = true;
                    break;
                case REPLICANT_NEED_BOOTSTRAP:
                    LOG(ERROR) << "communication error with the coordinator: "
                               << m_repl->last_error_desc()
                               << "(" << lstatus << ";"
                               << m_repl->last_error_file() << ":"
                               << m_repl->last_error_line() << ")";
                    break;
                case REPLICANT_SUCCESS:
                case REPLICANT_NAME_TOO_LONG:
                case REPLICANT_FUNC_NOT_FOUND:
                case REPLICANT_OBJ_EXIST:
                case REPLICANT_OBJ_NOT_FOUND:
                case REPLICANT_COND_NOT_FOUND:
                case REPLICANT_COND_DESTROYED:
                case REPLICANT_SERVER_ERROR:
                case REPLICANT_BAD_LIBRARY:
                case REPLICANT_MISBEHAVING_SERVER:
                case REPLICANT_INTERNAL_ERROR:
                case REPLICANT_NONE_PENDING:
                case REPLICANT_GARBAGE:
                    LOG(ERROR) << "unexpected error with coordinator connection: "
                               << m_repl->last_error_desc()
                               << "(" << lstatus << ";"
                               << m_repl->last_error_file() << ":"
                               << m_repl->last_error_line() << ")";
                    break;
                default:
                    abort();
            }

            continue;
        }

        need_to_backoff = false;
        std::map<int64_t, std::pair<uint64_t, std::tr1::shared_ptr<replicant_returncode> > >::iterator ack_iter;
        std::map<int64_t, std::pair<transfer_id, std::tr1::shared_ptr<replicant_returncode> > >::iterator xfer_iter;
        std::map<int64_t, std::pair<server_id, std::tr1::shared_ptr<replicant_returncode> > >::iterator tcp_iter;

        if (lid == m_wait_config_id)
        {
            if (initiate_get_config())
            {
                need_to_backoff = false;
            }

            m_wait_config_id = -1;
            m_wait_config_status = REPLICANT_GARBAGE;
            continue;
        }
        else if (lid == m_get_config_id)
        {
            switch (m_get_config_status)
            {
                case REPLICANT_SUCCESS:
                    break;
                case REPLICANT_FUNC_NOT_FOUND:
                    LOG(ERROR) << "coordinator missing \"get-config\" function: "
                               << m_repl->last_error_desc()
                               << "(" << m_get_config_status << ";"
                               << m_repl->last_error_file() << ":"
                               << m_repl->last_error_line() << ")";
                    continue;
                case REPLICANT_OBJ_NOT_FOUND:
                    LOG(ERROR) << "coordinator missing \"hyperdex\" object: "
                               << m_repl->last_error_desc()
                               << "(" << m_get_config_status << ";"
                               << m_repl->last_error_file() << ":"
                               << m_repl->last_error_line() << ")";
                    continue;
                case REPLICANT_TIMEOUT:
                case REPLICANT_BACKOFF:
                case REPLICANT_INTERRUPTED:
                    continue;
                case REPLICANT_NEED_BOOTSTRAP:
                    LOG(ERROR) << "communication error with the coordinator: "
                               << m_repl->last_error_desc()
                               << "(" << m_get_config_status << ";"
                               << m_repl->last_error_file() << ":"
                               << m_repl->last_error_line() << ")";
                    continue;
                case REPLICANT_NAME_TOO_LONG:
                case REPLICANT_OBJ_EXIST:
                case REPLICANT_COND_NOT_FOUND:
                case REPLICANT_COND_DESTROYED:
                case REPLICANT_SERVER_ERROR:
                case REPLICANT_BAD_LIBRARY:
                case REPLICANT_MISBEHAVING_SERVER:
                case REPLICANT_INTERNAL_ERROR:
                case REPLICANT_NONE_PENDING:
                case REPLICANT_GARBAGE:
                    LOG(ERROR) << "unexpected error with coordinator connection: "
                               << m_repl->last_error_desc()
                               << "(" << m_get_config_status << ";"
                               << m_repl->last_error_file() << ":"
                               << m_repl->last_error_line() << ")";
                    continue;
                default:
                    abort();
            }

            e::unpacker up(m_get_config_output, m_get_config_output_sz);
            up = up >> *config;
            m_get_config_id = -1;
            m_get_config_status = REPLICANT_GARBAGE;
            replicant_destroy_output(m_get_config_output, m_get_config_output_sz);

            if (up.error())
            {
                LOG(ERROR) << "configuration does not parse (file a bug); here's some hex: "
                           << e::slice(m_get_config_output, m_get_config_output_sz).hex();
                retry = 10000000000;
                need_to_backoff = true;
                continue;
            }

            if (m_daemon->m_config.cluster() != 0 &&
                m_daemon->m_config.cluster() != config->cluster())
            {
                LOG(ERROR) << "coordinator has changed the cluster identity from "
                           << m_daemon->m_config.cluster() << " to "
                           << config->cluster() << "; treating it as failed";
                retry = 10000000000;
                need_to_backoff = true;
                continue;
            }

            m_transfers_go_live_seen.clear();
            m_transfers_complete_seen.clear();
            m_tcp_disconnects_seen.clear();
            return true;
        }
        else if (lid == m_shutdown1_id)
        {
            m_shutdown1_id = -1;

            if (m_shutdown1_status != REPLICANT_SUCCESS ||
                m_shutdown1_output_sz != sizeof(uint16_t) + sizeof(uint64_t))
            {
                if (m_shutdown1_output)
                {
                    replicant_destroy_output(m_shutdown1_output, m_shutdown1_output_sz);
                }

                LOG(ERROR) << "dirty shutdown because shutdown1 failed";
                m_state = DIRTY_SHUTDOWN;
                continue;
            }

            uint16_t status;
            uint64_t version;
            const char* ptr = m_shutdown1_output;
            ptr = e::unpack16be(ptr, &status);
            ptr = e::unpack64be(ptr, &version);

            if (m_shutdown1_output)
            {
                replicant_destroy_output(m_shutdown1_output, m_shutdown1_output_sz);
            }

            if (static_cast<coordinator_returncode>(status) != COORD_SUCCESS ||
                version == 0)
            {
                LOG(ERROR) << "dirty shutdown because shutdown1 failed";
                m_state = DIRTY_SHUTDOWN;
                continue;
            }

            m_wait_acked_id = m_repl->wait("hyperdex", "acked",
                                           version - 1, &m_wait_acked_status);

            if (m_wait_acked_id < 0)
            {
                LOG(ERROR) << "dirty shutdown because we could not wait on \"acked\" condition";
                m_state = DIRTY_SHUTDOWN;
            }
        }
        else if (lid == m_wait_acked_id)
        {
            m_wait_acked_id = -1;

            if (m_wait_acked_status != REPLICANT_SUCCESS)
            {
                LOG(ERROR) << "dirty shutdown because wait_acked failed";
                m_state = DIRTY_SHUTDOWN;
                continue;
            }

            char data[sizeof(uint64_t)];
            e::pack64be(m_daemon->m_us.get(), data);
            m_shutdown2_id = m_repl->send("hyperdex", "server-shutdown2", data, sizeof(uint64_t),
                                          &m_shutdown2_status,
                                          &m_shutdown2_output, &m_shutdown2_output_sz);

            if (m_shutdown2_id < 0)
            {
                LOG(ERROR) << "dirty shutdown because shutdown2 failed: "
                           << m_repl->last_error_desc()
                           << "(" << m_get_config_status << ";"
                           << m_repl->last_error_file() << ":"
                           << m_repl->last_error_line() << ")";
                m_state = DIRTY_SHUTDOWN;
            }
        }
        else if (lid == m_shutdown2_id)
        {
            m_shutdown2_id = -1;

            if (m_shutdown2_status != REPLICANT_SUCCESS ||
                m_shutdown2_output_sz != sizeof(uint16_t))
            {
                if (m_shutdown2_output)
                {
                    replicant_destroy_output(m_shutdown2_output, m_shutdown2_output_sz);
                }

                LOG(ERROR) << "dirty shutdown because shutdown2 failed";
                m_state = DIRTY_SHUTDOWN;
                continue;
            }

            uint16_t status;
            uint64_t version;
            const char* ptr = m_shutdown2_output;
            ptr = e::unpack16be(ptr, &status);
            ptr = e::unpack64be(ptr, &version);

            if (m_shutdown2_output)
            {
                replicant_destroy_output(m_shutdown2_output, m_shutdown2_output_sz);
            }

            if (static_cast<coordinator_returncode>(status) != COORD_SUCCESS)
            {
                LOG(ERROR) << "dirty shutdown because shutdown2 failed";
                m_state = DIRTY_SHUTDOWN;
                continue;
            }

            m_state = CLEAN_SHUTDOWN;
        }
        else if ((ack_iter = m_acks.find(lid)) != m_acks.end())
        {
            if (*ack_iter->second.second != REPLICANT_SUCCESS)
            {
                LOG(ERROR) << "could not ack config " << ack_iter->second.first
                           << " because " << *ack_iter->second.second;
            }

            m_acks.erase(ack_iter);
        }
        else if ((xfer_iter = m_transfers_go_live.find(lid)) != m_transfers_go_live.end())
        {
            if (*xfer_iter->second.second != REPLICANT_SUCCESS)
            {
                LOG(ERROR) << "could not report live transfer " << xfer_iter->second.first
                           << " because " << *xfer_iter->second.second;
            }

            m_transfers_go_live.erase(xfer_iter);
        }
        else if ((xfer_iter = m_transfers_complete.find(lid)) != m_transfers_complete.end())
        {
            if (*xfer_iter->second.second != REPLICANT_SUCCESS)
            {
                LOG(ERROR) << "could not report complete transfer " << xfer_iter->second.first
                           << " because " << *xfer_iter->second.second;
            }

            m_transfers_complete.erase(xfer_iter);
        }
        else if ((tcp_iter = m_tcp_disconnects.find(lid)) != m_tcp_disconnects.end())
        {
            if (*tcp_iter->second.second != REPLICANT_SUCCESS)
            {
                LOG(ERROR) << "could not report tcp disconnect to " << tcp_iter->second.first
                           << " because " << *tcp_iter->second.second;
            }

            m_tcp_disconnects.erase(tcp_iter);
        }
        else
        {
            LOG(ERROR) << "received event from replicant, but don't know where it came from";
        }
    }

    return false;
}

void
coordinator_link :: ack_config(uint64_t version)
{
    char data[2 * sizeof(uint64_t)];
    char* ptr = data;
    ptr = e::pack64be(m_daemon->m_us.get(), ptr);
    ptr = e::pack64be(version, ptr);
    std::tr1::shared_ptr<replicant_returncode> ret(new replicant_returncode(REPLICANT_GARBAGE));
    int64_t req_id = m_repl->send("hyperdex", "ack-config", data, 2 * sizeof(uint64_t),
                                  ret.get(), NULL, NULL);

    if (req_id < 0)
    {
        LOG(ERROR) << "could ack new config: "
                   << m_repl->last_error_desc()
                   << "(" << ret.get() << ";"
                   << m_repl->last_error_file() << ":"
                   << m_repl->last_error_line() << ")";
    }
    else
    {
        m_acks.insert(std::make_pair(req_id, std::make_pair(version, ret)));
    }
}

void
coordinator_link :: transfer_go_live(const transfer_id& id)
{
    po6::threads::mutex::hold hold(&m_protect_queues);
    m_queue_transfers_go_live.push(id);
    pthread_kill(m_looper, SIGUSR1);
}

void
coordinator_link :: transfer_complete(const transfer_id& id)
{
    po6::threads::mutex::hold hold(&m_protect_queues);
    m_queue_transfers_complete.push(id);
    pthread_kill(m_looper, SIGUSR1);
}

void
coordinator_link :: report_tcp_disconnect(const server_id& id)
{
    po6::threads::mutex::hold hold(&m_protect_queues);
    m_queue_tcp_disconnects.push(id);
    pthread_kill(m_looper, SIGUSR1);
}

bool
coordinator_link :: initiate_wait_for_config()
{
    m_wait_config_id = m_repl->wait("hyperdex", "config",
                                    m_daemon->m_config.version(),
                                    &m_wait_config_status);

    if (m_wait_config_id < 0)
    {
        LOG(ERROR) << "could wait for new config: "
                   << m_repl->last_error_desc()
                   << "(" << m_wait_config_status << ";"
                   << m_repl->last_error_file() << ":"
                   << m_repl->last_error_line() << ")";
        return false;
    }

    return true;
}

bool
coordinator_link :: initiate_get_config()
{
    m_get_config_id = m_repl->send("hyperdex", "get-config", "", 0,
                                   &m_get_config_status,
                                   &m_get_config_output, &m_get_config_output_sz);

    if (m_get_config_id < 0)
    {
        LOG(ERROR) << "could wait for new config: "
                   << m_repl->last_error_desc()
                   << "(" << m_get_config_status << ";"
                   << m_repl->last_error_file() << ":"
                   << m_repl->last_error_line() << ")";
        return false;
    }

    return true;
}

void
coordinator_link :: initiate_transfer_go_live(const transfer_id& id)
{
    if (m_transfers_go_live_seen.find(id) == m_transfers_go_live_seen.end())
    {
        LOG(INFO) << "asking the coordinator to mark " << id << " live";
        m_transfers_go_live_seen.insert(id);
    }

    char buf[sizeof(uint64_t)];
    e::pack64be(id.get(), buf);
    std::tr1::shared_ptr<replicant_returncode> ret(new replicant_returncode(REPLICANT_GARBAGE));
    int64_t req_id = m_repl->send("hyperdex", "xfer-go-live", buf, sizeof(uint64_t),
                                  ret.get(), NULL, NULL);

    if (req_id < 0)
    {
        LOG(ERROR) << "could not tell the coordinator that transfer " << id << " is live";
    }
    else
    {
        m_transfers_go_live.insert(std::make_pair(req_id, std::make_pair(id, ret)));
    }
}

void
coordinator_link :: initiate_transfer_complete(const transfer_id& id)
{
    if (m_transfers_complete_seen.find(id) == m_transfers_complete_seen.end())
    {
        LOG(INFO) << "asking the coordinator to mark " << id << " complete";
        m_transfers_complete_seen.insert(id);
    }

    char buf[sizeof(uint64_t)];
    e::pack64be(id.get(), buf);
    std::tr1::shared_ptr<replicant_returncode> ret(new replicant_returncode(REPLICANT_GARBAGE));
    int64_t req_id = m_repl->send("hyperdex", "xfer-complete", buf, sizeof(uint64_t),
                                  ret.get(), NULL, NULL);

    if (req_id < 0)
    {
        LOG(ERROR) << "could not tell the coordinator that transfer " << id << " is complete";
    }
    else
    {
        m_transfers_complete.insert(std::make_pair(req_id, std::make_pair(id, ret)));
    }
}

void
coordinator_link :: initiate_report_tcp_disconnect(const server_id& id)
{
    if (m_tcp_disconnects_seen.find(id) == m_tcp_disconnects_seen.end())
    {
        LOG(INFO) << "asking the coordinator to record tcp disconnect for " << id;
        m_tcp_disconnects_seen.insert(id);
    }

    char buf[2 * sizeof(uint64_t)];
    e::pack64be(id.get(), buf);
    e::pack64be(m_daemon->m_config.version(), buf + sizeof(uint64_t));
    std::tr1::shared_ptr<replicant_returncode> ret(new replicant_returncode(REPLICANT_GARBAGE));
    int64_t req_id = m_repl->send("hyperdex", "server-suspect", buf, 2 * sizeof(uint64_t),
                                  ret.get(), NULL, NULL);

    if (req_id < 0)
    {
        LOG(ERROR) << "could not tell the coordinator that " << id << " broke a TCP connection";
    }
    else
    {
        m_tcp_disconnects.insert(std::make_pair(req_id, std::make_pair(id, ret)));
    }
}
