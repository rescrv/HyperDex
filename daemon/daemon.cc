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

// POSIX
#include <dirent.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

// STL
#include <sstream>

// Google Log
#include <glog/logging.h>
#include <glog/raw_logging.h>

// e
#include <e/endian.h>
#include <e/strescape.h>
#include <e/time.h>

// HyperDex
#include "common/coordinator_returncode.h"
#include "common/serialization.h"
#include "daemon/daemon.h"

#ifdef __APPLE__
#include <mach/mach.h>
#endif

#define ALARM_INTERVAL 30

using po6::threads::make_thread_wrapper;
using hyperdex::daemon;

int s_interrupts = 0;
bool s_alarm = false;
bool s_debug = false;

static void
exit_on_signal(int /*signum*/)
{
    if (__sync_fetch_and_add(&s_interrupts, 1) == 0)
    {
        RAW_LOG(ERROR, "interrupted: initiating shutdown; we'll try for up to 10 seconds (interrupt again to exit immediately)");
    }
    else
    {
        RAW_LOG(ERROR, "interrupted again: exiting immediately");
    }
}

static void
exit_after_timeout(int /*signum*/)
{
    __sync_fetch_and_add(&s_interrupts, 1);
    RAW_LOG(ERROR, "took too long to shutdown; just exiting");
}

static void
handle_alarm(int /*signum*/)
{
    s_alarm = true;
}

static void
handle_debug(int /*signum*/)
{
    s_debug = true;
}

static void
dummy(int /*signum*/)
{
}

daemon :: daemon()
    : m_us()
    , m_bind_to()
    , m_threads()
    , m_gc()
    , m_gc_ts()
    , m_coord(this)
    , m_data_dir()
    , m_data(this)
    , m_comm(this)
    , m_repl(this)
    , m_stm(this)
    , m_sm(this)
    , m_config()
    , m_protect_pause()
    , m_can_pause(&m_protect_pause)
    , m_paused(false)
    , m_perf_req_get()
    , m_perf_req_get_partial()
    , m_perf_req_atomic()
    , m_perf_req_search_start()
    , m_perf_req_search_next()
    , m_perf_req_search_stop()
    , m_perf_req_sorted_search()
    , m_perf_req_group_del()
    , m_perf_req_count()
    , m_perf_req_search_describe()
    , m_perf_chain_op()
    , m_perf_chain_subspace()
    , m_perf_chain_ack()
    , m_perf_chain_gc()
    , m_perf_xfer_handshake_syn()
    , m_perf_xfer_handshake_synack()
    , m_perf_xfer_handshake_ack()
    , m_perf_xfer_handshake_wiped()
    , m_perf_xfer_op()
    , m_perf_xfer_ack()
    , m_perf_backup()
    , m_perf_perf_counters()
    , m_block_stat_path()
    , m_stat_collector(make_thread_wrapper(&daemon::collect_stats, this))
    , m_protect_stats()
    , m_stats_start(0)
    , m_stats()
{
    m_gc.register_thread(&m_gc_ts);
}

daemon :: ~daemon() throw ()
{
    m_gc.deregister_thread(&m_gc_ts);
}

static bool
install_signal_handler(int signum, void (*f)(int))
{
    struct sigaction handle;
    handle.sa_handler = f;
    sigfillset(&handle.sa_mask);
    handle.sa_flags = SA_RESTART;
    return sigaction(signum, &handle, NULL) >= 0;
}

static bool
generate_token(uint64_t* token)
{
    po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

    if (sysrand.get() < 0)
    {
        return false;
    }

    if (sysrand.read(token, sizeof(*token)) != sizeof(*token))
    {
        return false;
    }

    return true;
}

int
daemon :: run(bool daemonize,
              po6::pathname data,
              po6::pathname log,
              po6::pathname pidfile,
              bool has_pidfile,
              bool set_bind_to,
              po6::net::location bind_to,
              bool set_coordinator,
              po6::net::hostname coordinator,
              unsigned threads)
{
    if (!install_signal_handler(SIGHUP, exit_on_signal))
    {
        std::cerr << "could not install SIGHUP handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGINT, exit_on_signal))
    {
        std::cerr << "could not install SIGINT handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGTERM, exit_on_signal))
    {
        std::cerr << "could not install SIGTERM handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGALRM, handle_alarm))
    {
        std::cerr << "could not install SIGALRM handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGUSR1, dummy))
    {
        std::cerr << "could not install SIGUSR1 handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGUSR2, handle_debug))
    {
        std::cerr << "could not install SIGUSR2 handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        return EXIT_FAILURE;
    }

    sigdelset(&ss, SIGPROF);

    if (pthread_sigmask(SIG_SETMASK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        return EXIT_FAILURE;
    }

    google::LogToStderr();

    if (daemonize)
    {
        struct stat x;

        if (lstat(log.get(), &x) < 0 || !S_ISDIR(x.st_mode))
        {
            LOG(ERROR) << "cannot fork off to the background because "
                       << log.get() << " does not exist or is not writable";
            return EXIT_FAILURE;
        }

        if (!has_pidfile)
        {
            LOG(INFO) << "forking off to the background";
            LOG(INFO) << "you can find the log at " << log.get() << "/hyperdex-daemon-YYYYMMDD-HHMMSS.sssss";
            LOG(INFO) << "provide \"--foreground\" on the command-line if you want to run in the foreground";
        }

        google::SetLogSymlink(google::INFO, "");
        google::SetLogSymlink(google::WARNING, "");
        google::SetLogSymlink(google::ERROR, "");
        google::SetLogSymlink(google::FATAL, "");
        log = po6::join(log, "hyperdex-daemon-");
        google::SetLogDestination(google::INFO, log.get());

        if (::daemon(1, 0) < 0)
        {
            PLOG(ERROR) << "could not daemonize";
            return EXIT_FAILURE;
        }

        if (has_pidfile)
        {
            char buf[21];
            ssize_t buf_sz = sprintf(buf, "%d\n", getpid());
            assert(buf_sz < static_cast<ssize_t>(sizeof(buf)));
            po6::io::fd pid(open(pidfile.get(), O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR));

            if (pid.get() < 0 || pid.xwrite(buf, buf_sz) != buf_sz)
            {
                PLOG(ERROR) << "could not create pidfile";
                return EXIT_FAILURE;
            }
        }
    }
    else
    {
        LOG(INFO) << "running in the foreground";
        LOG(INFO) << "no log will be generated; instead, the log messages will print to the terminal";
        LOG(INFO) << "provide \"--daemon\" on the command-line if you want to run in the background";
    }

    bool saved = false;
    server_id saved_us;
    po6::net::location saved_bind_to;
    po6::net::hostname saved_coordinator;
    LOG(INFO) << "initializing local storage";
    m_data_dir = data.get();

    if (!m_data.initialize(data, &saved, &saved_us, &saved_bind_to, &saved_coordinator))
    {
        return EXIT_FAILURE;
    }

    if (saved)
    {
        if (set_bind_to && bind_to != saved_bind_to)
        {
            LOG(INFO) << "changing bind address from "
                      << saved_bind_to << " to " << bind_to;
        }
        else
        {
            bind_to = saved_bind_to;
        }

        if (set_coordinator && coordinator != saved_coordinator)
        {
            LOG(INFO) << "changing coordinator address from "
                      << saved_coordinator << " to " << coordinator;
        }
        else
        {
            coordinator = saved_coordinator;
        }

        m_us = saved_us;
    }

    m_bind_to = bind_to;
    m_coord.set_coordinator_address(coordinator.address.c_str(), coordinator.port);

    if (!saved)
    {
        uint64_t sid;

        if (!generate_token(&sid))
        {
            PLOG(ERROR) << "could not read random token from /dev/urandom";
            return EXIT_FAILURE;
        }

        LOG(INFO) << "generated new random token:  " << sid;

        if (!m_coord.register_id(server_id(sid), bind_to))
        {
            return EXIT_FAILURE;
        }

        m_us = server_id(sid);
    }

    if (!m_data.save_state(m_us, bind_to, coordinator))
    {
        return EXIT_FAILURE;
    }

    uint64_t checkpoint = 0;
    uint64_t checkpoint_stable = 0;
    uint64_t checkpoint_gc = 0;

    if (!m_coord.initialize_checkpoints(&checkpoint, &checkpoint_stable, &checkpoint_gc))
    {
        LOG(ERROR) << "could not initialize our session with the coordinator; shutting down";
        return EXIT_FAILURE;
    }

    LOG(INFO) << "starting checkpoint process with"
              << " checkpoint=" << checkpoint
              << " checkpoint_stable=" << checkpoint_stable
              << " checkpoint_gc=" << checkpoint_gc;

    determine_block_stat_path(data);
    m_comm.setup(bind_to, threads);
    m_repl.setup();
    m_stm.setup();
    m_sm.setup();

    for (size_t i = 0; i < threads; ++i)
    {
        using namespace po6::threads;
        e::compat::shared_ptr<thread> t(new thread(make_thread_wrapper(&daemon::loop, this, i)));
        m_threads.push_back(t);
        t->start();
    }

    m_stat_collector.start();
    alarm(ALARM_INTERVAL);
    bool cluster_jump = false;
    bool requested_exit = false;

    while (__sync_fetch_and_add(&s_interrupts, 0) < 2 &&
           !m_coord.should_exit())
    {
        if (s_alarm)
        {
            s_alarm = false;
            alarm(ALARM_INTERVAL);
            m_repl.trip_periodic();
        }

        if (s_debug)
        {
            s_debug = false;
            LOG(INFO) << "recieved SIGUSR2; dumping internal tables";
            // XXX m_coord.debug_dump();
            m_data.debug_dump();
            // XXX m_comm.debug_dump();
            m_repl.debug_dump();
            m_stm.debug_dump();
            // XXX m_sm.debug_dump();
            LOG(INFO) << "end debug dump";
        }

        if (s_interrupts > 0 && !requested_exit)
        {
            if (!install_signal_handler(SIGALRM, exit_after_timeout))
            {
                __sync_fetch_and_add(&s_interrupts, 2);
                break;
            }

            alarm(10);
            m_coord.request_shutdown();
            requested_exit = true;
        }

        if (m_config.version() > 0 &&
            m_config.version() == m_coord.config_version() &&
            checkpoint < m_coord.checkpoint())
        {
            checkpoint = m_coord.checkpoint();
            m_repl.begin_checkpoint(checkpoint);
        }

        if (m_config.version() > 0 &&
            m_config.version() == m_coord.config_version() &&
            checkpoint_stable < m_coord.checkpoint_stable())
        {
            checkpoint_stable = m_coord.checkpoint_stable();
            m_repl.end_checkpoint(checkpoint_stable);
        }

        if (m_config.version() > 0 &&
            m_config.version() == m_coord.config_version() &&
            checkpoint_gc < m_coord.checkpoint_gc())
        {
            checkpoint_gc = m_coord.checkpoint_gc();
            m_data.set_checkpoint_gc(checkpoint_gc);
        }

        m_gc.quiescent_state(&m_gc_ts);
        m_gc.offline(&m_gc_ts);

        if (!m_coord.maintain_link())
        {
            m_gc.online(&m_gc_ts);
            continue;
        }

        m_gc.online(&m_gc_ts);
        const configuration& old_config(m_config);
        configuration new_config;
        m_coord.copy_config(&new_config);

        if (old_config.cluster() != 0 &&
            old_config.cluster() != new_config.cluster())
        {
            cluster_jump = true;
            break;
        }

        if (old_config.version() > new_config.version())
        {
            LOG(ERROR) << "received new configuration version=" << new_config.version()
                       << " that's older than our current configuration version="
                       << old_config.version();
            continue;
        }
        else if (old_config.version() >= new_config.version())
        {
            continue;
        }

        LOG(INFO) << "moving to configuration version=" << new_config.version()
                  << "; pausing all activity while we reconfigure";
        this->pause();
        m_comm.reconfigure(old_config, new_config, m_us);
        m_data.reconfigure(old_config, new_config, m_us);
        m_repl.reconfigure(old_config, new_config, m_us);
        m_stm.reconfigure(old_config, new_config, m_us);
        m_sm.reconfigure(old_config, new_config, m_us);
        m_config = new_config;
        this->unpause();
        LOG(INFO) << "reconfiguration complete; resuming normal operation";

        // let the coordinator know we've moved to this config
        m_coord.config_ack(new_config.version());
    }

    if (cluster_jump)
    {
        LOG(INFO) << "\n================================================================================\n"
                  << "Exiting because the coordinator changed on us.\n"
                  << "This is most likely an operations error.  Did you deploy a new HyperDex\n"
                  << "cluster at the same address as the old cluster?\n"
                  << "================================================================================";
    }
    else if (m_coord.should_exit() && !m_config.exists(m_us))
    {
        LOG(INFO) << "\n================================================================================\n"
                  << "Exiting because the coordinator says it doesn't know about this node.\n"
                  << "Check the coordinator logs for details, but it's most likely the case that\n"
                  << "this server was killed, or this server tried reconnecting to a different\n"
                  << "coordinator.  You may just have to restart the daemon with a different \n"
                  << "coordinator address or this node may be dead and you can simply erase it.\n"
                  << "================================================================================";
    }

    __sync_fetch_and_add(&s_interrupts, 2);
    m_stat_collector.join();
    m_comm.shutdown();

    for (size_t i = 0; i < m_threads.size(); ++i)
    {
        m_threads[i]->join();
    }

    m_sm.teardown();
    m_stm.teardown();
    m_repl.teardown();
    m_comm.teardown();
    m_data.teardown();
    m_coord.teardown();
    LOG(INFO) << "hyperdex-daemon will now terminate";
    return EXIT_SUCCESS;
}

void
daemon :: pause()
{
    po6::threads::mutex::hold hold(&m_protect_pause);

    while (m_paused)
    {
        m_can_pause.wait();
    }

    m_paused = true;
    m_sm.pause();
    m_stm.pause();
    m_repl.pause();
    m_data.pause();
    m_comm.pause();
}

void
daemon :: unpause()
{
    po6::threads::mutex::hold hold(&m_protect_pause);
    m_comm.unpause();
    m_data.unpause();
    m_repl.unpause();
    m_stm.unpause();
    m_sm.unpause();
    assert(m_paused);
    m_paused = false;
    m_can_pause.signal();
}

void
daemon :: loop(size_t thread)
{
    sigset_t ss;

    size_t core = thread % sysconf(_SC_NPROCESSORS_ONLN);
#ifdef __LINUX__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_t cur = pthread_self();
    int x = pthread_setaffinity_np(cur, sizeof(cpu_set_t), &cpuset);
    assert(x == 0);
#elif defined(__APPLE__)
    thread_affinity_policy_data_t policy;
    policy.affinity_tag = 0;
    thread_policy_set(mach_thread_self(),
                      THREAD_AFFINITY_POLICY,
                      (thread_policy_t)&policy,
                      THREAD_AFFINITY_POLICY_COUNT);
#endif

    LOG(INFO) << "network thread " << thread << " started on core " << core;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        return;
    }

    sigdelset(&ss, SIGPROF);

    if (pthread_sigmask(SIG_SETMASK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        return;
    }

    e::garbage_collector::thread_state ts;
    m_gc.register_thread(&ts);

    server_id from;
    virtual_server_id vfrom;
    virtual_server_id vto;
    network_msgtype type;
    std::auto_ptr<e::buffer> msg;
    e::unpacker up;

    while (m_comm.recv(&from, &vfrom, &vto, &type, &msg, &up))
    {
        assert(from != server_id());
        assert(vto != virtual_server_id());

        switch (type)
        {
            case REQ_GET:
                process_req_get(from, vfrom, vto, msg, up);
                m_perf_req_get.tap();
                break;
            case REQ_GET_PARTIAL:
                process_req_get_partial(from, vfrom, vto, msg, up);
                m_perf_req_get_partial.tap();
                break;
            case REQ_ATOMIC:
                process_req_atomic(from, vfrom, vto, msg, up);
                m_perf_req_atomic.tap();
                break;
            case REQ_SEARCH_START:
                process_req_search_start(from, vfrom, vto, msg, up);
                m_perf_req_search_start.tap();
                break;
            case REQ_SEARCH_NEXT:
                process_req_search_next(from, vfrom, vto, msg, up);
                m_perf_req_search_next.tap();
                break;
            case REQ_SEARCH_STOP:
                process_req_search_stop(from, vfrom, vto, msg, up);
                m_perf_req_search_stop.tap();
                break;
            case REQ_SORTED_SEARCH:
                process_req_sorted_search(from, vfrom, vto, msg, up);
                m_perf_req_sorted_search.tap();
                break;
            case REQ_GROUP_DEL:
                process_req_group_del(from, vfrom, vto, msg, up);
                m_perf_req_group_del.tap();
                break;
            case REQ_COUNT:
                process_req_count(from, vfrom, vto, msg, up);
                m_perf_req_count.tap();
                break;
            case REQ_SEARCH_DESCRIBE:
                process_req_search_describe(from, vfrom, vto, msg, up);
                m_perf_req_search_describe.tap();
                break;
            case CHAIN_OP:
                process_chain_op(from, vfrom, vto, msg, up);
                m_perf_chain_op.tap();
                break;
            case CHAIN_SUBSPACE:
                process_chain_subspace(from, vfrom, vto, msg, up);
                m_perf_chain_subspace.tap();
                break;
            case CHAIN_ACK:
                process_chain_ack(from, vfrom, vto, msg, up);
                m_perf_chain_ack.tap();
                break;
            case CHAIN_GC:
                process_chain_gc(from, vfrom, vto, msg, up);
                m_perf_chain_gc.tap();
                break;
            case XFER_HS:
                process_xfer_handshake_syn(from, vfrom, vto, msg, up);
                m_perf_xfer_handshake_syn.tap();
                break;
            case XFER_HSA:
                process_xfer_handshake_synack(from, vfrom, vto, msg, up);
                m_perf_xfer_handshake_synack.tap();
                break;
            case XFER_HA:
                process_xfer_handshake_ack(from, vfrom, vto, msg, up);
                m_perf_xfer_handshake_ack.tap();
                break;
            case XFER_HW:
                process_xfer_handshake_wiped(from, vfrom, vto, msg, up);
                m_perf_xfer_handshake_wiped.tap();
                break;
            case XFER_OP:
                process_xfer_op(from, vfrom, vto, msg, up);
                m_perf_xfer_op.tap();
                break;
            case XFER_ACK:
                process_xfer_ack(from, vfrom, vto, msg, up);
                m_perf_xfer_ack.tap();
                break;
            case BACKUP:
                process_backup(from, vfrom, vto, msg, up);
                m_perf_backup.tap();
            case PERF_COUNTERS:
                process_perf_counters(from, vfrom, vto, msg, up);
                m_perf_perf_counters.tap();
                break;
            case RESP_GET:
            case RESP_GET_PARTIAL:
            case RESP_ATOMIC:
            case RESP_SEARCH_ITEM:
            case RESP_SEARCH_DONE:
            case RESP_SORTED_SEARCH:
            case RESP_GROUP_DEL:
            case RESP_COUNT:
            case RESP_SEARCH_DESCRIBE:
            case CONFIGMISMATCH:
            case PACKET_NOP:
            default:
                LOG(INFO) << "received " << type << " message which servers do not process";
                break;
        }

        m_gc.quiescent_state(&ts);
    }

    m_gc.deregister_thread(&ts);
    LOG(INFO) << "network thread shutting down";
}

void
daemon :: process_req_get(server_id from,
                          virtual_server_id,
                          virtual_server_id vto,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up)
{
    uint64_t nonce;
    e::slice key;

    if ((up >> nonce >> key).error())
    {
        LOG(WARNING) << "unpack of REQ_GET failed; here's some hex:  " << msg->hex();
        return;
    }

    std::vector<e::slice> value;
    uint64_t version;
    datalayer::reference ref;
    network_returncode result;

    switch (m_data.get(m_config.get_region_id(vto), key, &value, &version, &ref))
    {
        case datalayer::SUCCESS:
            result = NET_SUCCESS;
            break;
        case datalayer::NOT_FOUND:
            result = NET_NOTFOUND;
            break;
        case datalayer::BAD_ENCODING:
        case datalayer::CORRUPTION:
        case datalayer::IO_ERROR:
        case datalayer::LEVELDB_ERROR:
        default:
            LOG(ERROR) << "GET returned unacceptable error code.";
            result = NET_SERVERERROR;
            break;
    }

    size_t sz = HYPERDEX_HEADER_SIZE_VC
              + sizeof(uint64_t)
              + sizeof(uint16_t)
              + pack_size(value);
    msg.reset(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(HYPERDEX_HEADER_SIZE_VC);
    pa = pa << nonce << static_cast<uint16_t>(result);

    if (result == NET_SUCCESS)
    {
        pa = pa << value;
    }

    m_comm.send_client(vto, from, RESP_GET, msg);
}

void
daemon :: process_req_get_partial(server_id from,
                                  virtual_server_id,
                                  virtual_server_id vto,
                                  std::auto_ptr<e::buffer> msg,
                                  e::unpacker up)
{
    uint64_t nonce;
    e::slice key;
    std::vector<uint16_t> attrs;

    if ((up >> nonce >> key >> attrs).error())
    {
        LOG(WARNING) << "unpack of REQ_GET_PARTIAL failed; here's some hex:  " << msg->hex();
        return;
    }

    std::sort(attrs.begin(), attrs.end());
    std::vector<e::slice> value;
    uint64_t version;
    datalayer::reference ref;
    network_returncode result;

    switch (m_data.get(m_config.get_region_id(vto), key, &value, &version, &ref))
    {
        case datalayer::SUCCESS:
            result = NET_SUCCESS;
            break;
        case datalayer::NOT_FOUND:
            result = NET_NOTFOUND;
            break;
        case datalayer::BAD_ENCODING:
        case datalayer::CORRUPTION:
        case datalayer::IO_ERROR:
        case datalayer::LEVELDB_ERROR:
        default:
            LOG(ERROR) << "GET returned unacceptable error code.";
            result = NET_SERVERERROR;
            break;
    }

    size_t sz = HYPERDEX_HEADER_SIZE_VC
              + sizeof(uint64_t)
              + sizeof(uint16_t)
              + pack_size(value)
              + value.size() * sizeof(uint16_t);
    msg.reset(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(HYPERDEX_HEADER_SIZE_VC);
    pa = pa << nonce << static_cast<uint16_t>(result);

    if (result == NET_SUCCESS)
    {
        for (size_t i = 0; i < value.size(); ++i)
        {
            uint16_t attr = i + 1;

            if (std::binary_search(attrs.begin(), attrs.end(), attr))
            {
                pa = pa << attr << value[i];
            }
        }
    }

    m_comm.send_client(vto, from, RESP_GET_PARTIAL, msg);
}

void
daemon :: process_req_atomic(server_id from,
                             virtual_server_id,
                             virtual_server_id vto,
                             std::auto_ptr<e::buffer> msg,
                             e::unpacker up)
{
    uint64_t nonce;
    uint8_t flags;
    e::slice key;
    std::vector<attribute_check> checks;
    std::vector<funcall> funcs;
    up = up >> nonce >> key >> flags >> checks >> funcs;

    if (up.error())
    {
        LOG(WARNING) << "unpack of REQ_ATOMIC failed; here's some hex:  " << msg->hex();
        return;
    }

    bool erase = !(flags & 128);
    bool fail_if_not_found = flags & 1;
    bool fail_if_found = flags & 2;
    m_repl.client_atomic(from, vto, nonce, erase, fail_if_not_found, fail_if_found, key, checks, funcs);
}

void
daemon :: process_req_search_start(server_id from,
                                   virtual_server_id,
                                   virtual_server_id vto,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up)
{
    uint64_t nonce;
    uint64_t search_id;
    std::vector<attribute_check> checks;

    if ((up >> nonce >> search_id >> checks).error())
    {
        LOG(WARNING) << "unpack of REQ_SEARCH_START failed; here's some hex:  " << msg->hex();
        return;
    }

    m_sm.start(from, vto, msg, nonce, search_id, &checks);
}

void
daemon :: process_req_search_next(server_id from,
                                  virtual_server_id,
                                  virtual_server_id vto,
                                  std::auto_ptr<e::buffer> msg,
                                  e::unpacker up)
{
    uint64_t nonce;
    uint64_t search_id;

    if ((up >> nonce >> search_id).error())
    {
        LOG(WARNING) << "unpack of REQ_SEARCH_NEXT failed; here's some hex:  " << msg->hex();
        return;
    }

    m_sm.next(from, vto, nonce, search_id);
}

void
daemon :: process_req_search_stop(server_id from,
                                  virtual_server_id,
                                  virtual_server_id vto,
                                  std::auto_ptr<e::buffer> msg,
                                  e::unpacker up)
{
    uint64_t nonce;
    uint64_t search_id;

    if ((up >> nonce >> search_id).error())
    {
        LOG(WARNING) << "unpack of REQ_SEARCH_STOP failed; here's some hex:  " << msg->hex();
        return;
    }

    m_sm.stop(from, vto, search_id);
}

void
daemon :: process_req_sorted_search(server_id from,
                                    virtual_server_id,
                                    virtual_server_id vto,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up)
{
    uint64_t nonce;
    std::vector<attribute_check> checks;
    uint64_t limit;
    uint16_t sort_by;
    uint8_t flags;

    if ((up >> nonce >> checks >> limit >> sort_by >> flags).error())
    {
        LOG(WARNING) << "unpack of REQ_SORTED_SEARCH failed; here's some hex:  " << msg->hex();
        return;
    }

    m_sm.sorted_search(from, vto, nonce, &checks, limit, sort_by, flags & 0x1);
}

void
daemon :: process_req_group_del(server_id from,
                                virtual_server_id,
                                virtual_server_id vto,
                                std::auto_ptr<e::buffer> msg,
                                e::unpacker up)
{
    uint64_t nonce;
    std::vector<attribute_check> checks;

    if ((up >> nonce >> checks).error())
    {
        LOG(WARNING) << "unpack of REQ_GROUP_DEL failed; here's some hex:  " << msg->hex();
        return;
    }

    e::slice sl("\x01\x00\x00\x00\x00\x00\x00\x00\x00", 9);
    m_sm.group_keyop(from, vto, nonce, &checks, REQ_ATOMIC, sl, RESP_GROUP_DEL);
}

void
daemon :: process_req_count(server_id from,
                            virtual_server_id,
                            virtual_server_id vto,
                            std::auto_ptr<e::buffer> msg,
                            e::unpacker up)
{
    uint64_t nonce;
    std::vector<attribute_check> checks;

    if ((up >> nonce >> checks).error())
    {
        LOG(WARNING) << "unpack of REQ_COUNT failed; here's some hex:  " << msg->hex();
        return;
    }

    m_sm.count(from, vto, nonce, &checks);
}

void
daemon :: process_req_search_describe(server_id from,
                                      virtual_server_id,
                                      virtual_server_id vto,
                                      std::auto_ptr<e::buffer> msg,
                                      e::unpacker up)
{
    uint64_t nonce;
    std::vector<attribute_check> checks;

    if ((up >> nonce >> checks).error())
    {
        LOG(WARNING) << "unpack of REQ_SEARCH_DESCRIBE failed; here's some hex:  " << msg->hex();
        return;
    }

    m_sm.search_describe(from, vto, nonce, &checks);
}

void
daemon :: process_chain_op(server_id,
                           virtual_server_id vfrom,
                           virtual_server_id vto,
                           std::auto_ptr<e::buffer> msg,
                           e::unpacker up)
{
    uint8_t flags;
    uint64_t reg_id;
    uint64_t seq_id;
    uint64_t version;
    e::slice key;
    std::vector<e::slice> value;

    if ((up >> flags >> reg_id >> seq_id >> version >> key >> value).error())
    {
        LOG(WARNING) << "unpack of CHAIN_OP failed; here's some hex:  " << msg->hex();
        return;
    }

    bool fresh = flags & 1;
    bool has_value = flags & 2;
    bool retransmission = flags & 128;
    m_repl.chain_op(vfrom, vto, retransmission, region_id(reg_id), seq_id, version, fresh, has_value, msg, key, value);
}

void
daemon :: process_chain_subspace(server_id,
                                 virtual_server_id vfrom,
                                 virtual_server_id vto,
                                 std::auto_ptr<e::buffer> msg,
                                 e::unpacker up)
{
    uint8_t flags;
    uint64_t reg_id;
    uint64_t seq_id;
    uint64_t version;
    e::slice key;
    std::vector<e::slice> value;
    std::vector<uint64_t> hashes;

    if ((up >> flags >> reg_id >> seq_id >> version >> key >> value >> hashes).error())
    {
        LOG(WARNING) << "unpack of CHAIN_SUBSPACE failed; here's some hex:  " << msg->hex();
        return;
    }

    bool retransmission = flags & 128;
    m_repl.chain_subspace(vfrom, vto, retransmission, region_id(reg_id), seq_id, version, msg, key, value, hashes);
}

void
daemon :: process_chain_gc(server_id,
                           virtual_server_id vfrom,
                           virtual_server_id,
                           std::auto_ptr<e::buffer> msg,
                           e::unpacker up)
{
    uint64_t seq_id;

    if ((up >> seq_id).error())
    {
        LOG(WARNING) << "unpack of CHAIN_GC failed; here's some hex:  " << msg->hex();
        return;
    }

    region_id ri = m_config.get_region_id(vfrom);
    m_repl.chain_gc(ri, seq_id);
}

void
daemon :: process_chain_ack(server_id,
                            virtual_server_id vfrom,
                            virtual_server_id vto,
                            std::auto_ptr<e::buffer> msg,
                            e::unpacker up)
{
    uint8_t flags;
    uint64_t reg_id;
    uint64_t seq_id;
    uint64_t version;
    e::slice key;

    if ((up >> flags >> reg_id >> seq_id >> version >> key).error())
    {
        LOG(WARNING) << "unpack of CHAIN_ACK failed; here's some hex:  " << msg->hex();
        return;
    }

    bool retransmission = flags & 128;
    m_repl.chain_ack(vfrom, vto, retransmission, region_id(reg_id), seq_id, version, key);
}

void
daemon :: process_xfer_handshake_syn(server_id,
                                     virtual_server_id vfrom,
                                     virtual_server_id,
                                     std::auto_ptr<e::buffer> msg,
                                     e::unpacker up)
{
    transfer_id xid;

    if ((up >> xid).error())
    {
        LOG(WARNING) << "unpack of XFER_HS failed; here's some hex:  " << msg->hex();
        return;
    }

    m_stm.handshake_syn(vfrom, xid);
}

void
daemon :: process_xfer_handshake_synack(server_id from,
                                        virtual_server_id,
                                        virtual_server_id to,
                                        std::auto_ptr<e::buffer> msg,
                                        e::unpacker up)
{
    transfer_id xid;
    uint64_t timestamp;

    if ((up >> xid >> timestamp).error())
    {
        LOG(WARNING) << "unpack of XFER_HSA failed; here's some hex:  " << msg->hex();
        return;
    }

    m_stm.handshake_synack(from, to, xid, timestamp);
}

void
daemon :: process_xfer_handshake_ack(server_id,
                                     virtual_server_id vfrom,
                                     virtual_server_id,
                                     std::auto_ptr<e::buffer> msg,
                                     e::unpacker up)
{
    transfer_id xid;
    uint8_t flags;

    if ((up >> xid >> flags).error())
    {
        LOG(WARNING) << "unpack of XFER_HA failed; here's some hex:  " << msg->hex();
        return;
    }

    bool wipe = flags & 0x1;
    m_stm.handshake_ack(vfrom, xid, wipe);
}

void
daemon :: process_xfer_handshake_wiped(server_id from,
                                       virtual_server_id,
                                       virtual_server_id to,
                                       std::auto_ptr<e::buffer> msg,
                                       e::unpacker up)
{
    transfer_id xid;

    if ((up >> xid).error())
    {
        LOG(WARNING) << "unpack of XFER_HW failed; here's some hex:  " << msg->hex();
        return;
    }

    m_stm.handshake_wiped(from, to, xid);
}

void
daemon :: process_xfer_op(server_id,
                          virtual_server_id vfrom,
                          virtual_server_id,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up)
{
    uint8_t flags;
    uint64_t xid;
    uint64_t seq_no;
    uint64_t version;
    e::slice key;
    std::vector<e::slice> value;

    if ((up >> flags >> xid >> seq_no >> version >> key >> value).error())
    {
        LOG(WARNING) << "unpack of XFER_OP failed; here's some hex:  " << msg->hex();
        return;
    }

    bool has_value = flags & 1;
    m_stm.xfer_op(vfrom, transfer_id(xid), seq_no, has_value, version, msg, key, value);
}

void
daemon :: process_xfer_ack(server_id from,
                           virtual_server_id,
                           virtual_server_id vto,
                           std::auto_ptr<e::buffer> msg,
                           e::unpacker up)
{
    uint8_t flags;
    uint64_t xid;
    uint64_t seq_no;

    if ((up >> flags >> xid >> seq_no).error())
    {
        LOG(WARNING) << "unpack of XFER_ACK failed; here's some hex:  " << msg->hex();
        return;
    }

    m_stm.xfer_ack(from, vto, transfer_id(xid), seq_no);
}

void
daemon :: process_backup(server_id from,
                         virtual_server_id,
                         virtual_server_id vto,
                         std::auto_ptr<e::buffer> msg,
                         e::unpacker up)
{
    uint64_t nonce;
    e::slice _name;

    if ((up >> nonce >> _name).error() ||
        strnlen(reinterpret_cast<const char*>(_name.data()), _name.size()) == _name.size())
    {
        LOG(WARNING) << "unpack of BACKUP failed; here's some hex:  " << msg->hex();
        return;
    }

    std::string name(reinterpret_cast<const char*>(_name.data()));
    network_returncode result = NET_SUCCESS;

    if (!m_data.backup(e::slice(name)))
    {
        result = NET_SERVERERROR;
    }
    else
    {
        LOG(INFO) << "Backup succeeded and is available in the directory \"backup-"
                  << e::strescape(name) << "\" within the data directory."
                  << "  Copy the complete directory to elsewhere so your backup is safe.";
    }

    using po6::join;
    using po6::pathname;
    pathname _path(join(pathname(m_data_dir),
                        pathname(("backup-" + name).c_str())).get());
    _path = _path.realpath();
    e::slice path(_path.get());

    size_t sz = HYPERDEX_HEADER_SIZE_VC
              + sizeof(uint64_t)
              + sizeof(uint16_t)
              + pack_size(path);
    msg.reset(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(HYPERDEX_HEADER_SIZE_VC);
    pa = pa << nonce << static_cast<uint16_t>(result) << path;
    m_comm.send_client(vto, from, BACKUP, msg);
}

void
daemon :: process_perf_counters(server_id from,
                                virtual_server_id,
                                virtual_server_id vto,
                                std::auto_ptr<e::buffer> msg,
                                e::unpacker up)
{
    uint64_t nonce;
    uint64_t when;
    up = up >> nonce >> when;

    if (up.error())
    {
        LOG(WARNING) << "unpack of PERF_COUNTERS failed; here's some hex:  " << msg->hex();
        return;
    }

    std::string out;

    {
        po6::threads::mutex::hold hold(&m_protect_stats);
        std::list<std::pair<uint64_t, std::string> >::iterator it;
        it = m_stats.begin();

        while (it != m_stats.end() && it->first <= when)
        {
            ++it;
        }

        if (it == m_stats.begin())
        {
            std::ostringstream ret;
            ret << m_stats_start << " reset=" << m_stats_start << "\n";
            out += ret.str();
        }

        while (it != m_stats.end())
        {
            out += it->second;
            ++it;
        }
    }

    size_t sz = HYPERDEX_HEADER_SIZE_VC
              + sizeof(uint64_t)
              + out.size() + 1;
    msg.reset(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(HYPERDEX_HEADER_SIZE_VC);
    pa = pa << nonce;
    pa.copy(e::slice(out.data(), out.size() + 1));
    m_comm.send_client(vto, from, PERF_COUNTERS, msg);
}

#define INTERVAL 100000000ULL

void
daemon :: collect_stats()
{
    uint64_t target = e::time();
    target = target - (target % INTERVAL) + INTERVAL;

    {
        po6::threads::mutex::hold hold(&m_protect_stats);
        m_stats_start = target;
    }

    while (__sync_fetch_and_add(&s_interrupts, 0) == 0)
    {
        // every INTERVAL nanoseconds collect stats
        uint64_t now = e::time();

        if (now < target)
        {
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = std::min(target - now, (uint64_t)50000000UL);
            nanosleep(&ts, NULL);
            continue;
        }

        // collect the stats
        std::ostringstream ret;
        ret << target;
        collect_stats_msgs(&ret);
        collect_stats_leveldb(&ret);
        collect_stats_io(&ret);
        ret << "\n";
        std::string out = ret.str();

        po6::threads::mutex::hold hold(&m_protect_stats);
        m_stats.push_back(std::make_pair(target, out));

        if (m_stats.size() > 600)
        {
            m_stats.pop_front();
        }

        // next interval
        target += INTERVAL;
    }
}

void
daemon :: collect_stats_msgs(std::ostringstream* ret)
{
    *ret << " msgs.req_get=" << m_perf_req_get.read();
    *ret << " msgs.req_get_partial=" << m_perf_req_get_partial.read();
    *ret << " msgs.req_atomic=" << m_perf_req_atomic.read();
    *ret << " msgs.req_search_start=" << m_perf_req_search_start.read();
    *ret << " msgs.req_search_next=" << m_perf_req_search_next.read();
    *ret << " msgs.req_search_stop=" << m_perf_req_search_stop.read();
    *ret << " msgs.req_sorted_search=" << m_perf_req_sorted_search.read();
    *ret << " msgs.req_group_del=" << m_perf_req_group_del.read();
    *ret << " msgs.req_count=" << m_perf_req_count.read();
    *ret << " msgs.req_search_describe=" << m_perf_req_search_describe.read();
    *ret << " msgs.chain_op=" << m_perf_chain_op.read();
    *ret << " msgs.chain_subspace=" << m_perf_chain_subspace.read();
    *ret << " msgs.chain_ack=" << m_perf_chain_ack.read();
    *ret << " msgs.chain_gc=" << m_perf_chain_gc.read();
    *ret << " msgs.xfer_op=" << m_perf_xfer_op.read();
    *ret << " msgs.xfer_ack=" << m_perf_xfer_ack.read();
    *ret << " msgs.perf_counters=" << m_perf_perf_counters.read();
}

namespace
{

struct leveldb_stat
{
    leveldb_stat() : files(0), size(0), time(0), read(0), write(0) {}
    uint64_t files;
    uint64_t size;
    uint64_t time;
    uint64_t read;
    uint64_t write;
};

} // namespace

void
daemon :: collect_stats_leveldb(std::ostringstream* ret)
{
    *ret << " leveldb.size=" << m_data.approximate_size();
    std::string tmp;

    if (m_data.get_property(e::slice("leveldb.stats"), &tmp))
    {
        std::vector<char> lines(tmp.c_str(), tmp.c_str() + tmp.size() + 1);
        char* ptr = &lines.front();
        char* end = ptr + lines.size() - 1;
        leveldb_stat stats[7];

        while (ptr < end)
        {
            char* eol = strchr(ptr, '\n');
            eol = eol ? eol : end;
            *eol = 0;
            int level;
            int files;
            double size;
            double time;
            double read;
            double write;

            if (sscanf(ptr, "%d %d %lf %lf %lf %lf",
                            &level, &files, &size, &time, &read, &write) == 6 &&
                level < 7)
            {
                stats[level].files = files;
                stats[level].size = size * 1048576ULL;
                stats[level].time = time;
                stats[level].read = read * 1048576ULL;
                stats[level].write = write * 1048576ULL;
            }

            ptr = eol + 1;
        }

        for (size_t i = 0; i < 7; ++i)
        {
            *ret << " leveldb.files" << i << "=" << stats[i].files;
            *ret << " leveldb.size" << i << "=" << stats[i].size;
            *ret << " leveldb.time" << i << "=" << stats[i].time;
            *ret << " leveldb.read" << i << "=" << stats[i].read;
            *ret << " leveldb.write" << i << "=" << stats[i].write;
        }
    }
}

namespace
{

bool
read_sys_block_star(std::vector<std::string>* blocks)
{
    DIR* dir = opendir("/sys/block");
    struct dirent* ent = NULL;

    if (dir == NULL)
    {
        return false;
    }

    errno = 0;

    while ((ent = readdir(dir)) != NULL)
    {
        blocks->push_back(ent->d_name);
    }

    closedir(dir);
    return errno == 0;
}

} // namespace

void
daemon :: determine_block_stat_path(const po6::pathname& data)
{
    po6::pathname dir;

    try
    {
       dir = data.realpath();
    }
    catch (po6::error& e)
    {
        LOG(ERROR) << "could not resolve true path for data: " << e.what();
        LOG(ERROR) << "iostat-like statistics will not be reported";
        return;
    }

    std::vector<std::string> block_devs;

    if (!read_sys_block_star(&block_devs))
    {
        LOG(ERROR) << "could not ls /sys/block";
        LOG(ERROR) << "iostat-like statistics will not be reported";
        return;
    }

    FILE* mounts = fopen("/proc/mounts", "r");

    if (!mounts)
    {
        LOG(ERROR) << "could not open /proc/mounts: " << po6::error(errno).what();
        LOG(ERROR) << "iostat-like statistics will not be reported";
        return;
    }

    char* line = NULL;
    size_t line_sz = 0;
    size_t max_mnt_sz = 0;

    while (true)
    {
        ssize_t amt = getline(&line, &line_sz, mounts);

        if (amt < 0)
        {
            if (ferror(mounts) != 0)
            {
                LOG(WARNING) << "could not read from /proc/mounts: " << po6::error(errno).what();
                break;
            }

            if (feof(mounts) != 0)
            {
                break;
            }

            LOG(WARNING) << "unknown error when reading from /proc/mounts\n";
            break;
        }

        char dev[4096];
        char mnt[4096];
        int pts = sscanf(line, "%4095s %4095s", dev, mnt);

        if (pts != 2)
        {
            continue;
        }

        size_t msz = strlen(mnt);

        if (strncmp(mnt, dir.get(), msz) != 0 ||
            msz < max_mnt_sz)
        {
            continue;
        }

        std::string stat_path = po6::pathname(dev).basename().get();

        for (size_t i = 0; i < block_devs.size(); ++i)
        {
            size_t dsz = std::min(stat_path.size(), block_devs[i].size());

            if (strncmp(block_devs[i].c_str(), stat_path.c_str(), dsz) == 0)
            {
                max_mnt_sz = msz;
                m_block_stat_path = std::string("/sys/block/") + block_devs[i] + "/stat";
            }
        }
    }

    if (!m_block_stat_path.empty())
    {
        LOG(INFO) << "using " << m_block_stat_path << " for reporting io.* stats";
    }
    else
    {
        LOG(WARNING) << "cannot determine device name for reporting io.* stats";
        LOG(WARNING) << "iostat-like statistics will not be reported";
    }

    if (line)
    {
        free(line);
    }

    fclose(mounts);
}

void
daemon :: collect_stats_io(std::ostringstream* ret)
{
    if (m_block_stat_path.empty())
    {
        return;
    }

    FILE* fin = fopen(m_block_stat_path.c_str(), "r");

    if (!fin)
    {
        LOG(ERROR) << "could not open " << m_block_stat_path << " for reading block-device stats; io.* stats will not be reported";
        m_block_stat_path = "";
        return;
    }

    uint64_t read_ios;
    uint64_t read_merges;
    uint64_t read_sectors;
    uint64_t read_ticks;
    uint64_t write_ios;
    uint64_t write_merges;
    uint64_t write_sectors;
    uint64_t write_ticks;
    uint64_t in_flight;
    uint64_t io_ticks;
    uint64_t time_in_queue;
    int x = fscanf(fin, "%lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu",
                   &read_ios, &read_merges, &read_sectors, &read_ticks,
                   &write_ios, &write_merges, &write_sectors, &write_ticks,
                   &in_flight, &io_ticks, &time_in_queue);
    fclose(fin);

    if (x == 11)
    {
        *ret << " io.read_ios=" << read_ios;
        *ret << " io.read_merges=" << read_merges;
        *ret << " io.read_bytes=" << read_sectors * 512;
        *ret << " io.read_ticks=" << read_ticks;
        *ret << " io.write_ios=" << write_ios;
        *ret << " io.write_merges=" << write_merges;
        *ret << " io.write_bytes=" << write_sectors * 512;
        *ret << " io.write_ticks=" << write_ticks;
        *ret << " io.in_flight=" << in_flight;
        *ret << " io.io_ticks=" << io_ticks;
        *ret << " io.time_in_queue=" << time_in_queue;
    }
}
