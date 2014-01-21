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

// C
#include <stdint.h>

// POSIX
#include <signal.h>

// Google Log
#include <glog/logging.h>

// e
#include <e/endian.h>

// HyperDex
#include "common/coordinator_returncode.h"
#include "common/serialization.h"
#include "daemon/coordinator_link_wrapper.h"
#include "daemon/daemon.h"

using hyperdex::configuration;
using hyperdex::coordinator_link_wrapper;

class coordinator_link_wrapper::coord_rpc
{
    public:
        coord_rpc();
        virtual ~coord_rpc() throw ();

    public:
        virtual bool callback(coordinator_link_wrapper* clw);

    public:
        replicant_returncode status;
        const char* output;
        size_t output_sz;
        std::ostringstream msg;

    private:
        coord_rpc(const coord_rpc&);
        coord_rpc& operator = (const coord_rpc&);

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }
        friend class e::intrusive_ptr<coord_rpc>;

    private:
        size_t m_ref;
};

coordinator_link_wrapper :: coord_rpc :: coord_rpc()
    : status(REPLICANT_GARBAGE)
    , output(NULL)
    , output_sz(0)
    , msg()
    , m_ref(0)
{
}

coordinator_link_wrapper :: coord_rpc :: ~coord_rpc() throw ()
{
    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }
}

bool
coordinator_link_wrapper :: coord_rpc :: callback(coordinator_link_wrapper* clw)
{
    if (status != REPLICANT_SUCCESS)
    {
        e::error err = clw->m_coord->error();
        LOG(ERROR) << "coordinator error: " << msg.str()
                   << ": " << err.msg() << " @ " << err.loc();
    }

    if (status == REPLICANT_CLUSTER_JUMP)
    {
        clw->do_sleep();
    }

    return false;
}

coordinator_link_wrapper :: coordinator_link_wrapper(daemon* d)
    : m_daemon(d)
    , m_coord()
    , m_rpcs()
    , m_mtx()
    , m_cond(&m_mtx)
    , m_locked(false)
    , m_kill(false)
    , m_to_kill()
    , m_waiting(0)
    , m_sleep(1000ULL * 1000ULL)
    , m_online_id(-1)
    , m_shutdown_requested(false)
    , m_need_config_ack(false)
    , m_config_ack(0)
    , m_config_ack_id(-1)
    , m_need_config_stable(false)
    , m_config_stable(0)
    , m_config_stable_id(-1)
    , m_checkpoint(0)
    , m_checkpoint_id(-1)
    , m_need_checkpoint_report_stable(false)
    , m_checkpoint_report_stable(0)
    , m_checkpoint_report_stable_id(-1)
    , m_checkpoint_stable(0)
    , m_checkpoint_stable_id(-1)
    , m_checkpoint_gc(0)
    , m_checkpoint_gc_id(-1)
{
}

coordinator_link_wrapper :: ~coordinator_link_wrapper() throw ()
{
}

void
coordinator_link_wrapper :: set_coordinator_address(const char* host, uint16_t port)
{
    assert(!m_coord.get());
    m_coord.reset(new coordinator_link(host, port));
}

bool
coordinator_link_wrapper :: register_id(server_id us, const po6::net::location& bind_to)
{
    std::auto_ptr<e::buffer> buf(e::buffer::create(sizeof(uint64_t) + pack_size(bind_to)));
    e::buffer::packer pa = buf->pack_at(0);
    pa = pa << us << bind_to;
    std::auto_ptr<coord_rpc> rpc(new coord_rpc);
    int64_t rid = m_coord->rpc("server_register",
                               reinterpret_cast<const char*>(buf->data()), buf->size(),
                               &rpc->status,
                               &rpc->output,
                               &rpc->output_sz);

    if (rid < 0)
    {
        e::error err = m_coord->error();
        LOG(ERROR) << "could not register as " << us << ": " << err.msg() << " @ " << err.loc();
        return false;
    }

    replicant_returncode lrc = REPLICANT_GARBAGE;
    int64_t lid = m_coord->loop(-1, &lrc);

    if (lid < 0)
    {
        e::error err = m_coord->error();
        LOG(ERROR) << "could not register as " << us << ": " << err.msg() << " @ " << err.loc();
        return false;
    }

    if (lid != rid)
    {
        LOG(ERROR) << "could not register as " << us << ": coordinator loop malfunction";
        return false;
    }

    if (rpc->status != REPLICANT_SUCCESS)
    {
        e::error err = m_coord->error();
        LOG(ERROR) << "could not register as " << us << ": " << err.msg() << " @ " << err.loc();
        return false;
    }

    if (rpc->output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(rpc->output, &x);
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
coordinator_link_wrapper :: should_exit()
{
    return (!m_coord->config()->exists(m_daemon->m_us) && m_coord->config()->version() > 0) ||
           (m_shutdown_requested && m_coord->config()->get_state(m_daemon->m_us) == server::SHUTDOWN);
}

bool
coordinator_link_wrapper :: maintain_link()
{
    enter_critical_section_killable();
    bool exit_status = false;

    while (true)
    {
        ensure_available();
        ensure_config_ack();
        ensure_config_stable();
        ensure_checkpoint();
        ensure_checkpoint_report_stable();
        ensure_checkpoint_stable();
        ensure_checkpoint_gc();
        replicant_returncode status = REPLICANT_GARBAGE;
        int64_t id = m_coord->loop(1000, &status);

        if (id < 0 &&
            (status == REPLICANT_TIMEOUT ||
             status == REPLICANT_INTERRUPTED))
        {
            reset_sleep();
            exit_status = false;
            break;
        }
        else if (id < 0 && (status == REPLICANT_BACKOFF ||
                            status == REPLICANT_NEED_BOOTSTRAP))
        {
            LOG(ERROR) << "coordinator disconnected: backing off before retrying";
            do_sleep();
            exit_status = false;
            break;
        }
        else if (id < 0 && status == REPLICANT_CLUSTER_JUMP)
        {
            e::error err = m_coord->error();
            LOG(ERROR) << "cluster jump: " << err.msg() << " @ " << err.loc();
            do_sleep();
            exit_status = false;
            break;
        }
        else if (id < 0)
        {
            e::error err = m_coord->error();
            LOG(ERROR) << "coordinator error: " << err.msg() << " @ " << err.loc();
            do_sleep();
            exit_status = false;
            break;
        }

        reset_sleep();

        if (id == INT64_MAX)
        {
            exit_status = m_coord->config()->exists(m_daemon->m_us);
            break;
        }

        rpc_map_t::iterator it = m_rpcs.find(id);

        if (it == m_rpcs.end())
        {
            continue;
        }

        e::intrusive_ptr<coord_rpc> rpc = it->second;
        m_rpcs.erase(it);

        if (rpc->callback(this))
        {
            break;
        }
    }

    exit_critical_section_killable();
    return exit_status;
}

const configuration&
coordinator_link_wrapper :: config()
{
    return *m_coord->config();
}

void
coordinator_link_wrapper :: request_shutdown()
{
    m_shutdown_requested = true;
    char buf[sizeof(uint64_t)];
    e::pack64be(m_daemon->m_us.get(), buf);
    e::intrusive_ptr<coord_rpc> rpc = new coord_rpc();
    rpc->msg << "request shutdown";
    make_rpc("server_shutdown", buf, sizeof(uint64_t), rpc);
}

uint64_t
coordinator_link_wrapper :: checkpoint()
{
    return m_checkpoint;
}

uint64_t
coordinator_link_wrapper :: checkpoint_stable()
{
    return m_checkpoint_stable;
}

uint64_t
coordinator_link_wrapper :: checkpoint_gc()
{
    return m_checkpoint_gc;
}

void
coordinator_link_wrapper :: transfer_go_live(const transfer_id& id)
{
    uint64_t version = m_daemon->m_config.version();
    char buf[2 * sizeof(uint64_t)];
    e::pack64be(id.get(), buf);
    e::pack64be(version, buf + sizeof(uint64_t));
    e::intrusive_ptr<coord_rpc> rpc = new coord_rpc();
    rpc->msg << "transver go_live id=" << id;
    make_rpc("transfer_go_live", buf, 2 * sizeof(uint64_t), rpc);
    LOG(INFO) << "requesting that " << id << " go live";
}

void
coordinator_link_wrapper :: transfer_complete(const transfer_id& id)
{
    uint64_t version = m_daemon->m_config.version();
    char buf[2 * sizeof(uint64_t)];
    e::pack64be(id.get(), buf);
    e::pack64be(version, buf + sizeof(uint64_t));
    e::intrusive_ptr<coord_rpc> rpc = new coord_rpc();
    rpc->msg << "transver complete id=" << id;
    make_rpc("transfer_complete", buf, 2 * sizeof(uint64_t), rpc);
    LOG(INFO) << "requesting that " << id << " complete";
}

void
coordinator_link_wrapper :: report_tcp_disconnect(const server_id& id)
{
    uint64_t version = m_daemon->m_config.version();
    char buf[2 * sizeof(uint64_t)];
    e::pack64be(id.get(), buf);
    e::pack64be(version, buf + sizeof(uint64_t));
    e::intrusive_ptr<coord_rpc> rpc = new coord_rpc();
    rpc->msg << "report TCP disconnect id=" << version;
    make_rpc("report_disconnect", buf, 2 * sizeof(uint64_t), rpc);
}

void
coordinator_link_wrapper :: config_ack(uint64_t version)
{
    enter_critical_section();

    if (m_config_ack < version)
    {
        m_need_config_ack = true;
        m_config_ack = version;
        m_config_ack_id = -1;
        ensure_config_ack();
    }

    exit_critical_section();
}

void
coordinator_link_wrapper :: config_stable(uint64_t version)
{
    enter_critical_section();

    if (m_config_stable < version)
    {
        m_need_config_stable = true;
        m_config_stable = version;
        m_config_stable_id = -1;
        ensure_config_stable();
    }

    exit_critical_section();
}

void
coordinator_link_wrapper :: checkpoint_report_stable(uint64_t seq)
{
    enter_critical_section();

    if (m_checkpoint_report_stable < seq)
    {
        m_need_checkpoint_report_stable = true;
        m_checkpoint_report_stable = seq;
        m_checkpoint_report_stable_id = -1;
        ensure_checkpoint_report_stable();
    }

    exit_critical_section();
}

void
coordinator_link_wrapper :: do_sleep()
{
    uint64_t sleep = m_sleep;
    timespec ts;

    while (sleep > 0)
    {
        ts.tv_sec = 0;
        ts.tv_nsec = std::min(static_cast<uint64_t>(1000ULL * 1000ULL), sleep);
        sigset_t empty_signals;
        sigset_t old_signals;
        sigemptyset(&empty_signals); // should never fail
        pthread_sigmask(SIG_SETMASK, &empty_signals, &old_signals); // should never fail
        nanosleep(&ts, NULL); // nothing to gain by checking output
        pthread_sigmask(SIG_SETMASK, &old_signals, NULL); // should never fail
        sleep -= ts.tv_nsec;
    }

    m_sleep = std::min(static_cast<uint64_t>(1000ULL * 1000ULL * 1000ULL), m_sleep * 2);
}

void
coordinator_link_wrapper :: reset_sleep()
{
    uint64_t start_sleep = 1000ULL * 1000ULL;

    if (m_sleep != start_sleep)
    {
        m_sleep = start_sleep;
        LOG(INFO) << "connection to coordinator reestablished";
    }
}

void
coordinator_link_wrapper :: enter_critical_section()
{
    po6::threads::mutex::hold hold(&m_mtx);

    while (m_locked)
    {
        if (m_kill)
        {
            pthread_kill(m_to_kill, SIGUSR1);
        }

        ++m_waiting;
        m_cond.wait();
        --m_waiting;
    }

    m_locked = true;
}

void
coordinator_link_wrapper :: exit_critical_section()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_locked = false;

    if (m_waiting > 0)
    {
        m_cond.signal();
    }
}

void
coordinator_link_wrapper :: enter_critical_section_killable()
{
    po6::threads::mutex::hold hold(&m_mtx);

    while (m_locked)
    {
        if (m_kill)
        {
            pthread_kill(m_to_kill, SIGUSR1);
        }

        ++m_waiting;
        m_cond.wait();
        --m_waiting;
    }

    m_locked = true;
    m_kill = true;
    m_to_kill = pthread_self();
}

void
coordinator_link_wrapper :: exit_critical_section_killable()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_locked = false;
    m_kill = false;

    if (m_waiting > 0)
    {
        m_cond.signal();
    }
}

class coordinator_link_wrapper::coord_rpc_available : public coord_rpc
{
    public:
        coord_rpc_available() {}
        virtual ~coord_rpc_available() throw () {}

    public:
        virtual bool callback(coordinator_link_wrapper* clw);
};

bool
coordinator_link_wrapper :: coord_rpc_available :: callback(coordinator_link_wrapper* clw)
{
    coord_rpc::callback(clw);
    clw->m_online_id = -1;
    return false;
}

void
coordinator_link_wrapper :: ensure_available()
{
    if (m_online_id >= 0 || m_shutdown_requested)
    {
        return;
    }

    if (m_coord->config()->get_address(m_daemon->m_us) == m_daemon->m_bind_to &&
        m_coord->config()->get_state(m_daemon->m_us) == server::AVAILABLE)
    {
        return;
    }

    size_t sz = sizeof(uint64_t) + pack_size(m_daemon->m_bind_to);
    std::auto_ptr<e::buffer> buf(e::buffer::create(sz));
    *buf << m_daemon->m_us << m_daemon->m_bind_to;
    e::intrusive_ptr<coord_rpc> rpc = new coord_rpc_available();
    rpc->msg << "server online";
    m_online_id = make_rpc_nosync("server_online",
                                  reinterpret_cast<const char*>(buf->data()), buf->size(),
                                  rpc);
}

class coordinator_link_wrapper::coord_rpc_config_ack : public coord_rpc
{
    public:
        coord_rpc_config_ack() {}
        virtual ~coord_rpc_config_ack() throw () {}

    public:
        virtual bool callback(coordinator_link_wrapper* clw);
};

bool
coordinator_link_wrapper :: coord_rpc_config_ack :: callback(coordinator_link_wrapper* clw)
{
    coord_rpc::callback(clw);
    clw->m_config_ack_id = -1;
    clw->m_need_config_ack = status != REPLICANT_SUCCESS;
    return false;
}

void
coordinator_link_wrapper :: ensure_config_ack()
{
    if (m_config_ack_id >= 0 || !m_need_config_ack)
    {
        return;
    }

    char buf[2 * sizeof(uint64_t)];
    e::pack64be(m_daemon->m_us.get(), buf);
    e::pack64be(m_config_ack, buf + sizeof(uint64_t));
    e::intrusive_ptr<coord_rpc> rpc = new coord_rpc_config_ack();
    rpc->msg << "ack config version=" << m_config_ack;
    m_config_ack_id = make_rpc_nosync("config_ack", buf, 2 * sizeof(uint64_t), rpc);
}

class coordinator_link_wrapper::coord_rpc_config_stable : public coord_rpc
{
    public:
        coord_rpc_config_stable() {}
        virtual ~coord_rpc_config_stable() throw () {}

    public:
        virtual bool callback(coordinator_link_wrapper* clw);
};

bool
coordinator_link_wrapper :: coord_rpc_config_stable :: callback(coordinator_link_wrapper* clw)
{
    coord_rpc::callback(clw);
    clw->m_config_stable_id = -1;
    clw->m_need_config_stable = status != REPLICANT_SUCCESS;
    return false;
}

void
coordinator_link_wrapper :: ensure_config_stable()
{
    if (m_config_stable_id >= 0 || !m_need_config_stable)
    {
        return;
    }

    char buf[2 * sizeof(uint64_t)];
    e::pack64be(m_daemon->m_us.get(), buf);
    e::pack64be(m_config_stable, buf + sizeof(uint64_t));
    e::intrusive_ptr<coord_rpc> rpc = new coord_rpc_config_stable();
    rpc->msg << "stable config version=" << m_config_stable;
    m_config_stable_id = make_rpc_nosync("config_stable", buf, 2 * sizeof(uint64_t), rpc);
}

class coordinator_link_wrapper::coord_rpc_checkpoint : public coord_rpc
{
    public:
        coord_rpc_checkpoint() {}
        virtual ~coord_rpc_checkpoint() throw () {}

    public:
        virtual bool callback(coordinator_link_wrapper* clw);
};

bool
coordinator_link_wrapper :: coord_rpc_checkpoint :: callback(coordinator_link_wrapper* clw)
{
    coord_rpc::callback(clw);
    clw->m_checkpoint_id = -1;

    if (status != REPLICANT_SUCCESS)
    {
        return false;
    }

    ++clw->m_checkpoint;
    return true;
}

void
coordinator_link_wrapper :: ensure_checkpoint()
{
    if (m_checkpoint_id >= 0)
    {
        return;
    }

    e::intrusive_ptr<coord_rpc> rpc = new coord_rpc_checkpoint();
    rpc->msg << "track checkpoint=" << m_checkpoint;
    m_checkpoint_id = wait_nosync("checkp", m_checkpoint + 1, rpc);
}

class coordinator_link_wrapper::coord_rpc_checkpoint_report_stable : public coord_rpc
{
    public:
        coord_rpc_checkpoint_report_stable() {}
        virtual ~coord_rpc_checkpoint_report_stable() throw () {}

    public:
        virtual bool callback(coordinator_link_wrapper* clw);
};

bool
coordinator_link_wrapper :: coord_rpc_checkpoint_report_stable :: callback(coordinator_link_wrapper* clw)
{
    coord_rpc::callback(clw);
    clw->m_checkpoint_report_stable_id = -1;
    clw->m_need_checkpoint_report_stable = true;

    if (status != REPLICANT_SUCCESS)
    {
        return false;
    }

    if (output_sz != 2)
    {
        return false;
    }

    uint16_t x;
    e::unpack16be(output, &x);
    coordinator_returncode rc = static_cast<coordinator_returncode>(x);

    if (rc != COORD_SUCCESS && rc != COORD_NO_CAN_DO)
    {
        LOG(ERROR) << "coordinator error: " << msg.str()
                   << ": " << rc;
    }

    clw->m_need_checkpoint_report_stable = rc != COORD_SUCCESS;
    return false;
}

void
coordinator_link_wrapper :: ensure_checkpoint_report_stable()
{
    if (m_checkpoint_report_stable_id >= 0 || !m_need_checkpoint_report_stable)
    {
        return;
    }

    char buf[3 * sizeof(uint64_t)];
    e::pack64be(m_daemon->m_us.get(), buf);
    e::pack64be(m_config_ack, buf + 1 * sizeof(uint64_t));
    e::pack64be(m_checkpoint_report_stable, buf + 2 * sizeof(uint64_t));
    e::intrusive_ptr<coord_rpc> rpc = new coord_rpc_checkpoint_report_stable();
    rpc->msg << "checkpoint_report_stable=" << m_checkpoint_report_stable;
    m_checkpoint_report_stable_id = make_rpc_nosync("checkpoint_stable", buf, 3 * sizeof(uint64_t), rpc);
}

class coordinator_link_wrapper::coord_rpc_checkpoint_stable : public coord_rpc
{
    public:
        coord_rpc_checkpoint_stable() {}
        virtual ~coord_rpc_checkpoint_stable() throw () {}

    public:
        virtual bool callback(coordinator_link_wrapper* clw);
};

bool
coordinator_link_wrapper :: coord_rpc_checkpoint_stable :: callback(coordinator_link_wrapper* clw)
{
    coord_rpc::callback(clw);
    clw->m_checkpoint_stable_id = -1;

    if (status != REPLICANT_SUCCESS)
    {
        return false;
    }

    ++clw->m_checkpoint_stable;
    return true;
}

void
coordinator_link_wrapper :: ensure_checkpoint_stable()
{
    if (m_checkpoint_stable_id >= 0)
    {
        return;
    }

    e::intrusive_ptr<coord_rpc> rpc = new coord_rpc_checkpoint_stable();
    rpc->msg << "track checkpoint_stable=" << m_checkpoint_stable;
    m_checkpoint_stable_id = wait_nosync("checkps", m_checkpoint_stable + 1, rpc);
}

class coordinator_link_wrapper::coord_rpc_checkpoint_gc : public coord_rpc
{
    public:
        coord_rpc_checkpoint_gc() {}
        virtual ~coord_rpc_checkpoint_gc() throw () {}

    public:
        virtual bool callback(coordinator_link_wrapper* clw);
};

bool
coordinator_link_wrapper :: coord_rpc_checkpoint_gc :: callback(coordinator_link_wrapper* clw)
{
    coord_rpc::callback(clw);
    clw->m_checkpoint_gc_id = -1;

    if (status != REPLICANT_SUCCESS)
    {
        return false;
    }

    ++clw->m_checkpoint_gc;
    return true;
}

void
coordinator_link_wrapper :: ensure_checkpoint_gc()
{
    if (m_checkpoint_gc_id >= 0)
    {
        return;
    }

    e::intrusive_ptr<coord_rpc> rpc = new coord_rpc_checkpoint_gc();
    rpc->msg << "track checkpoint_gc=" << m_checkpoint_gc;
    m_checkpoint_gc_id = wait_nosync("checkpgc", m_checkpoint_gc + 1, rpc);
}

void
coordinator_link_wrapper :: make_rpc(const char* func,
                                     const char* data, size_t data_sz,
                                     e::intrusive_ptr<coord_rpc> rpc)
{
    enter_critical_section();
    make_rpc_nosync(func, data, data_sz, rpc);
    exit_critical_section();
}

int64_t
coordinator_link_wrapper :: make_rpc_nosync(const char* func,
                                            const char* data, size_t data_sz,
                                            e::intrusive_ptr<coord_rpc> rpc)
{
    int64_t id = m_coord->rpc(func, data, data_sz,
                              &rpc->status,
                              &rpc->output,
                              &rpc->output_sz);

    if (id < 0)
    {
        e::error err = m_coord->error();
        LOG(ERROR) << "coordinator error: " << rpc->msg.str()
                   << ": " << err.msg() << " @ " << err.loc();
    }
    else
    {
        m_rpcs.insert(std::make_pair(id, rpc));
    }

    return id;
}

int64_t
coordinator_link_wrapper :: wait_nosync(const char* cond, uint64_t state,
                                        e::intrusive_ptr<coord_rpc> rpc)
{
    int64_t id = m_coord->wait(cond, state, &rpc->status);

    if (id < 0)
    {
        e::error err = m_coord->error();
        LOG(ERROR) << "coordinator error: " << rpc->msg.str()
                   << ": " << err.msg() << " @ " << err.loc();
    }
    else
    {
        m_rpcs.insert(std::make_pair(id, rpc));
    }

    return id;
}
