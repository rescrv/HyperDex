// Copyright (c) 2015, Cornell University
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

// STL
#include <string>

// Google Log
#include <glog/logging.h>

// e
#include <e/endian.h>
#include <e/guard.h>
#include <e/serialization.h>

// HyperDex
#include "common/coordinator_returncode.h"
#include "daemon/coordinator_link.h"
#include "daemon/daemon.h"

using hyperdex::coordinator_link;

struct coordinator_link::rpc
{
    rpc();
    ~rpc() throw ();

    std::string func;
    std::string input;
    unsigned flags;
    replicant_returncode status;
    char* output;
    size_t output_sz;

    private:
        rpc(const rpc&);
        rpc& operator = (const rpc&);
};

coordinator_link :: rpc :: rpc()
    : func()
    , input()
    , flags(0)
    , status(REPLICANT_GARBAGE)
    , output(NULL)
    , output_sz(0)
{
}

coordinator_link :: rpc :: ~rpc() throw ()
{
}

coordinator_link :: coordinator_link(daemon* d, const char* host, uint16_t port)
    : m_daemon(d)
    , m_sleep_ms(0)
    , m_config()
    , m_config_id(-1)
    , m_config_status()
    , m_config_state(0)
    , m_config_data(NULL)
    , m_config_data_sz(0)
    , m_checkpoint_id(-1)
    , m_checkpoint_status()
    , m_checkpoint_state(0)
    , m_checkpoint_data(NULL)
    , m_checkpoint_data_sz(0)
    , m_value_checkpoint_config_version(0)
    , m_value_checkpoint(0)
    , m_value_checkpoint_stable(0)
    , m_value_checkpoint_gc(0)
    , m_throwaway_status()
    , m_mtx()
    , m_repl(replicant_client_create(host, port))
    , m_rpcs()
{
    if (!m_repl)
    {
        throw std::bad_alloc();
    }
}

coordinator_link :: ~coordinator_link() throw ()
{
}

bool
coordinator_link :: register_server(server_id us, const po6::net::location& bind_to)
{
    std::string register_msg;
    e::packer(&register_msg) << us << bind_to;
    char* output = NULL;
    size_t output_sz = 0;
    std::ostringstream ostr;
    ostr << "register as " << us;

    if (!synchronous_call(ostr.str().c_str(), "server_register",
                          register_msg.data(), register_msg.size(),
                          &output, &output_sz))
    {
        return false;
    }

    assert(output);
    e::guard g_output = e::makeguard(free, output);

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case COORD_SUCCESS:
                return true;
            case COORD_DUPLICATE:
                LOG(ERROR) << "could not register as " << us << ": another server has this ID";
                return false;
            case COORD_UNINITIALIZED:
                LOG(ERROR) << "could not register as " << us << ": coordinator not initialized";
                return false;
            case COORD_MALFORMED:
            case COORD_NOT_FOUND:
            case COORD_NO_CAN_DO:
            default:
                LOG(ERROR) << "could not register as " << us << ": coordinator returned " << rc;
                return false;
        }
    }
    else
    {
        LOG(ERROR) << "could not register as " << us << ": coordinator returned invalid message";
        return false;
    }
}

bool
coordinator_link :: initialize()
{
    char* output = NULL;
    size_t output_sz = 0;

    if (!synchronous_call("get current configuration", "config_get", NULL, 0, &output, &output_sz))
    {
        return false;
    }

    assert(output);
    e::guard g_output = e::makeguard(free, output);

    if (!process_configuration(output, output_sz))
    {
        LOG(ERROR) << "could not get initial configuration; it appears to be invalid";
        return false;
    }

    if (!bring_online())
    {
        return false;
    }

    return true;
}

void
increment_sleep(unsigned* x)
{
    if (*x == 0)
    {
        *x = 100;
    }
    else
    {
        *x = std::min(*x * 2, 20000U);
    }
}

static bool
atomically_allow_pending_blocked_signals()
{
    sigset_t ss;
    sigemptyset(&ss);

    if (sigpending(&ss) == 0 &&
        (sigismember(&ss, SIGHUP) == 1 ||
         sigismember(&ss, SIGINT) == 1 ||
         sigismember(&ss, SIGTERM) == 1 ||
         sigismember(&ss, SIGQUIT) == 1 ||
         sigismember(&ss, SIGALRM) == 1))
    {
        sigemptyset(&ss);
        sigsuspend(&ss);
        return true;
    }

    return false;
}

bool
coordinator_link :: maintain()
{
    while (true)
    {
        if (m_sleep_ms > 0)
        {
            timespec ts;
            ts.tv_sec = m_sleep_ms / 1000;
            ts.tv_nsec = (m_sleep_ms % 1000) * 1000ULL * 1000ULL;
            nanosleep(&ts, NULL);

            if (atomically_allow_pending_blocked_signals())
            {
                return false;
            }
        }

        {
            po6::threads::mutex::hold hold(&m_mtx);

            if (m_config_status != REPLICANT_SUCCESS)
            {
                replicant_client_kill(m_repl, m_config_id);
                m_config_id = -1;
            }

            if (m_config_id < 0)
            {
                m_config_status = REPLICANT_SUCCESS;
                m_config_id = replicant_client_cond_follow(m_repl, "hyperdex", "config",
                                                           &m_config_status, &m_config_state,
                                                           &m_config_data, &m_config_data_sz);
            }

            if (m_checkpoint_status != REPLICANT_SUCCESS)
            {
                replicant_client_kill(m_repl, m_checkpoint_id);
                m_checkpoint_id = -1;
            }

            if (m_checkpoint_id < 0)
            {
                m_checkpoint_status = REPLICANT_SUCCESS;
                m_checkpoint_id = replicant_client_cond_follow(m_repl, "hyperdex", "checkpoint",
                                                               &m_checkpoint_status, &m_checkpoint_state,
                                                               &m_checkpoint_data, &m_checkpoint_data_sz);
            }
        }

        if (replicant_client_block(m_repl, 50) < 0)
        {
            return false;
        }

        int64_t id;
        replicant_returncode rc;
        po6::threads::mutex::hold hold(&m_mtx);
        id = replicant_client_loop(m_repl, 0, &rc);

        if (id < 0)
        {
            if (rc == REPLICANT_INTERRUPTED)
            {
                return false;
            }
            else if (rc != REPLICANT_NONE_PENDING && rc != REPLICANT_TIMEOUT)
            {
                LOG(ERROR) << "coordinator failure: " << replicant_client_error_message(m_repl);
                LOG(INFO) << "error [" << replicant_returncode_to_string(rc)
                          << "] at " << replicant_client_error_location(m_repl);
                increment_sleep(&m_sleep_ms);
                return false;
            }
        }

        m_sleep_ms = 0;

        if (id == m_config_id && m_config_status == REPLICANT_SUCCESS)
        {
            if (!process_configuration(m_config_data, m_config_data_sz))
            {
                increment_sleep(&m_sleep_ms);
                LOG(ERROR) << "received an invalid configuration from the coordinator";
                return false;
            }

            if (m_config.get_state(m_daemon->m_us) == server::NOT_AVAILABLE)
            {
                if (!bring_online())
                {
                    return false;
                }
            }

            return true;
        }
        else if (id == m_checkpoint_id && m_checkpoint_status == REPLICANT_SUCCESS)
        {
            uint64_t value_checkpoint_config_version;
            uint64_t value_checkpoint;
            uint64_t value_checkpoint_stable;
            uint64_t value_checkpoint_gc;
            e::unpacker up(m_checkpoint_data, m_checkpoint_data_sz);
            up = up >> value_checkpoint_config_version
                    >> value_checkpoint
                    >> value_checkpoint_stable
                    >> value_checkpoint_gc;

            if (!up.error())
            {
                // set the values and force the main thread out of here to act
                // on new checkpoints
                m_value_checkpoint_config_version = value_checkpoint_config_version;
                m_value_checkpoint = value_checkpoint;
                m_value_checkpoint_stable = value_checkpoint_stable;
                m_value_checkpoint_gc = value_checkpoint_gc;
                return false;
            }
        }
        else
        {
            std::map<int64_t, e::compat::shared_ptr<rpc> >::iterator rit;
            rit = m_rpcs.find(id);

            if (rit == m_rpcs.end())
            {
                continue;
            }

            e::compat::shared_ptr<rpc> r = rit->second;
            m_rpcs.erase(rit);

            if (r->status != REPLICANT_SUCCESS)
            {
                increment_sleep(&m_sleep_ms);
                LOG(ERROR) << "call to coordinator method \"" << r->func
                           << "\" failed: " << replicant_client_error_message(m_repl);

                if ((r->flags & REPLICANT_CALL_IDEMPOTENT))
                {
                    LOG(ERROR) << "retrying call to coordinator method \"" << r->func << "\"";
                    make_rpc_no_synchro(r);
                }
            }
        }
    }
}

void
coordinator_link :: shutdown()
{
    e::compat::shared_ptr<rpc> r(new rpc());
    r->func = "server_shutdown";
    r->flags = REPLICANT_CALL_IDEMPOTENT;
    e::packer(&r->input) << m_daemon->m_us;
    make_rpc(r);
}

void
coordinator_link :: config_ack(uint64_t config_version)
{
    e::compat::shared_ptr<rpc> r(new rpc());
    r->func = "config_ack";
    r->flags = REPLICANT_CALL_IDEMPOTENT;
    e::packer(&r->input) << m_daemon->m_us << config_version;
    make_rpc(r);
}

void
coordinator_link :: config_stable(uint64_t config_version)
{
    e::compat::shared_ptr<rpc> r(new rpc());
    r->func = "config_stable";
    r->flags = REPLICANT_CALL_IDEMPOTENT;
    e::packer(&r->input) << m_daemon->m_us << config_version;
    make_rpc(r);
}

void
coordinator_link :: checkpoint_report_stable(uint64_t config_version)
{
    e::compat::shared_ptr<rpc> r(new rpc());
    r->func = "checkpoint_stable";
    r->flags = REPLICANT_CALL_IDEMPOTENT;
    e::packer(&r->input) << m_daemon->m_us
                         << m_value_checkpoint_config_version
                         << config_version;
    make_rpc(r);
}

void
coordinator_link :: transfer_go_live(const transfer_id& id)
{
    e::compat::shared_ptr<rpc> r(new rpc());
    r->func = "transfer_go_live";
    r->flags = REPLICANT_CALL_IDEMPOTENT;
    e::packer(&r->input) << id;
    make_rpc(r);
}

void
coordinator_link :: transfer_complete(const transfer_id& id)
{
    e::compat::shared_ptr<rpc> r(new rpc());
    r->func = "transfer_complete";
    r->flags = REPLICANT_CALL_IDEMPOTENT;
    e::packer(&r->input) << id;
    make_rpc(r);
}

void
coordinator_link :: report_tcp_disconnect(uint64_t config_version, const server_id& id)
{
    e::compat::shared_ptr<rpc> r(new rpc());
    r->func = "report_disconnect";
    r->flags = REPLICANT_CALL_IDEMPOTENT;
    e::packer(&r->input) << config_version << id;
    make_rpc(r);
}

void
coordinator_link :: make_rpc(e::compat::shared_ptr<rpc> r)
{
    po6::threads::mutex::hold hold(&m_mtx);
    make_rpc_no_synchro(r);
}

void
coordinator_link :: make_rpc_no_synchro(e::compat::shared_ptr<rpc> r)
{
    int64_t id = replicant_client_call(m_repl, "hyperdex", r->func.c_str(),
                                       r->input.data(), r->input.size(), r->flags,
                                       &r->status, &r->output, &r->output_sz);

    if (id < 0)
    {
        LOG(ERROR) << "call to coordinator method \"" << r->func
                   << "\" failed: " << replicant_client_error_message(m_repl);
    }
    else
    {
        m_rpcs.insert(std::make_pair(id, r));
    }
}

bool
coordinator_link :: synchronous_call(const char* log_action, const char* func,
                                     const char* input, size_t input_sz,
                                     char** output, size_t* output_sz)
{
    po6::threads::mutex::hold hold(&m_mtx);
    replicant_returncode status;
    int64_t rid = replicant_client_call(m_repl, "hyperdex", func, input, input_sz,
                                        REPLICANT_CALL_ROBUST, &status, output, output_sz);

    if (rid < 0)
    {
        LOG(ERROR) << "could not " << log_action << ": "
                   << replicant_client_error_message(m_repl);
        LOG(INFO) << "error [" << replicant_returncode_to_string(status)
                  << "] at " << replicant_client_error_location(m_repl);
        return false;
    }

    replicant_returncode lstatus = REPLICANT_GARBAGE;
    int64_t lid = replicant_client_wait(m_repl, rid, -1, &lstatus);

    if (lid < 0)
    {
        LOG(ERROR) << "could not " << log_action << ": "
                   << replicant_client_error_message(m_repl);
        LOG(INFO) << "error [" << replicant_returncode_to_string(status)
                  << "] at " << replicant_client_error_location(m_repl);
        return false;
    }

    assert(lid == rid);

    if (status != REPLICANT_SUCCESS)
    {
        LOG(ERROR) << "could not " << log_action << ": "
                   << replicant_client_error_message(m_repl);
        LOG(INFO) << "error [" << replicant_returncode_to_string(status)
                  << "] at " << replicant_client_error_location(m_repl);
        return false;
    }

    return true;
}

bool
coordinator_link :: process_configuration(const char* str, size_t str_sz)
{
    e::unpacker up(str, str_sz);
    configuration config;
    up = up >> config;

    if (up.error())
    {
        return false;
    }

    m_config = config;
    return true;
}

bool
coordinator_link :: bring_online()
{
    std::string enter_input;
    e::packer(&enter_input) << m_daemon->m_us << m_daemon->m_bind_to;
    std::string exit_input;
    e::packer(&exit_input) << m_daemon->m_us;
    
    if (replicant_client_defended_call(m_repl, "hyperdex",
                                       "server_online", enter_input.data(), enter_input.size(),
                                       "server_suspect", exit_input.data(), exit_input.size(),
                                       &m_throwaway_status) < 0)
    {
        LOG(ERROR) << "error bringing server online: " << replicant_client_error_message(m_repl);
        LOG(INFO) << "error [" << replicant_returncode_to_string(m_throwaway_status)
                  << "] at " << replicant_client_error_location(m_repl);
        increment_sleep(&m_sleep_ms);
        return false;
    }

    return true;
}
