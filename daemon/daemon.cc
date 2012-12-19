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

// STL
#include <sstream>

// Google Log
#include <glog/logging.h>
#include <glog/raw_logging.h>

// e
#include <e/endian.h>
#include <e/timer.h>

// HyperDex
#include "common/coordinator_returncode.h"
#include "common/serialization.h"
#include "daemon/daemon.h"

using hyperdex::daemon;

static bool s_continue = true;

static void
exit_on_signal(int /*signum*/)
{
    RAW_LOG(ERROR, "signal received; triggering exit");
    s_continue = false;
}

daemon :: daemon()
    : m_us()
    , m_threads()
    , m_coord()
    , m_data(this)
    , m_comm(this)
    , m_repl(this)
    , m_sm(this)
    , m_config()
{
}

daemon :: ~daemon() throw ()
{
}

static bool
install_signal_handler(int signum)
{
    struct sigaction handle;
    handle.sa_handler = exit_on_signal;
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
              bool set_bind_to,
              po6::net::location bind_to,
              bool set_coordinator,
              po6::net::hostname coordinator,
              unsigned threads)
{
    if (!install_signal_handler(SIGHUP))
    {
        std::cerr << "could not install SIGHUP handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGINT))
    {
        std::cerr << "could not install SIGINT handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGTERM))
    {
        std::cerr << "could not install SIGTERM handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    google::LogToStderr();
    bool saved = false;
    server_id saved_us;
    po6::net::location saved_bind_to;
    po6::net::hostname saved_coordinator;
    LOG(INFO) << "initializing persistent storage";

    if (!m_data.setup(data, &saved, &saved_us, &saved_bind_to, &saved_coordinator))
    {
        return EXIT_FAILURE;
    }

    if (saved)
    {
        LOG(INFO) << "starting daemon from state found in the persistent storage";

        if (set_bind_to && bind_to != saved_bind_to)
        {
            LOG(ERROR) << "cannot bind to address; it conflicts with our previous address at " << saved_bind_to;
            return EXIT_FAILURE;
        }

        if (set_coordinator && coordinator != saved_coordinator)
        {
            LOG(ERROR) << "cannot connect to coordinator; it conflicts with our previous coordinator at " << saved_coordinator;
            return EXIT_FAILURE;
        }

        m_us = saved_us;
        bind_to = saved_bind_to;
        coordinator = saved_coordinator;
        m_coord.reset(new replicant_client(coordinator.address.c_str(), coordinator.port));
    }
    else
    {
        LOG(INFO) << "starting new daemon from command-line arguments";
        m_coord.reset(new replicant_client(coordinator.address.c_str(), coordinator.port));

        while (true)
        {
            uint64_t sid;

            if (!generate_token(&sid))
            {
                PLOG(ERROR) << "could not read random token from /dev/urandom";
                return EXIT_FAILURE;
            }

            LOG(INFO) << "generated new random token:  " << sid;

            int verify = register_id(server_id(sid), bind_to);

            if (verify < 0)
            {
                // reason logged by register_id
                return EXIT_FAILURE;
            }
            else if (verify > 0)
            {
                m_us = server_id(sid);
                break;
            }
            else
            {
                LOG(INFO) << "generated token already in use; you should buy a lotto ticket (we're retrying)";
            }
        }
    }

    if (daemonize)
    {
        LOG(INFO) << "forking off to the background; goodbye!";
        google::SetLogDestination(google::INFO, "hyperdex-");

        if (::daemon(1, 0) < 0)
        {
            PLOG(ERROR) << "could not daemonize";
            return EXIT_FAILURE;
        }
    }

    m_comm.setup(bind_to, threads);
    m_repl.setup();
    m_sm.setup();

    for (size_t i = 0; i < threads; ++i)
    {
        std::tr1::shared_ptr<po6::threads::thread> t(new po6::threads::thread(std::tr1::bind(&daemon::loop, this)));
        m_threads.push_back(t);
        t->start();
    }

    while (s_continue)
    {
        configuration old_config = m_config;
        configuration new_config;

        if (!wait_for_config(&new_config))
        {
            continue;
        }

        if (old_config.version() >= new_config.version())
        {
            LOG(INFO) << "received new configuration version=" << new_config.version()
                      << " that's not newer than our current configuration version="
                      << old_config.version();
            // XXX don't sleep; use notify/wait instead
            sleep(1);
            continue;
        }

        // XXX we really should check the reconfigure_returncode even though
        // nothing right now fails when reconfiguring

        LOG(INFO) << "received new configuration version=" << new_config.version();
        // prepare
        m_data.prepare(old_config, new_config, m_us);
        m_comm.prepare(old_config, new_config, m_us);
        m_repl.prepare(old_config, new_config, m_us);
        m_sm.prepare(old_config, new_config, m_us);
        // reconfigure
        LOG(INFO) << "preparations for reconfiguration done; pausing network communication";
        // this line to the "unpause" below are mutually exclusive with network workers
        m_comm.pause();
        m_data.reconfigure(old_config, new_config, m_us);
        m_comm.reconfigure(old_config, new_config, m_us);
        m_repl.reconfigure(old_config, new_config, m_us);
        m_sm.reconfigure(old_config, new_config, m_us);
        m_config = new_config;
        m_comm.unpause();
        LOG(INFO) << "reconfiguration complete; unpausing network communication";
        // cleanup
        m_sm.cleanup(old_config, new_config, m_us);
        m_repl.cleanup(old_config, new_config, m_us);
        m_comm.cleanup(old_config, new_config, m_us);
        m_data.cleanup(old_config, new_config, m_us);
        // XXX let the coordinator know we've moved to this config
    }

    LOG(INFO) << "hyperdex-daemon is gracefully shutting down";
    m_comm.shutdown();

    for (size_t i = 0; i < m_threads.size(); ++i)
    {
        m_threads[i]->join();
    }

    m_sm.teardown();
    m_repl.teardown();
    m_comm.teardown();
    m_data.teardown();
    LOG(INFO) << "hyperdex-daemon will now terminate";
    return EXIT_SUCCESS;
}

// negative indicates fatal error; zero indicates retry; positive indicates success
int
daemon :: register_id(server_id us, const po6::net::location& bind_to)
{
    replicant_returncode sstatus = REPLICANT_GARBAGE;
    std::auto_ptr<e::buffer> data(e::buffer::create(sizeof(uint64_t) + pack_size(bind_to)));
    *data << us.get() << bind_to;
    const char* output;
    size_t output_sz;
    int64_t sid = m_coord->send("hyperdex", 8, "register",
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
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_TIMEOUT:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                LOG(ERROR) << "while trying to register with the coordinator "
                           << "we received an error (" << sstatus << ") and "
                           << "don't know what to do (exiting is a safe bet)";
                return -1;
        }
    }

    replicant_returncode lstatus = REPLICANT_GARBAGE;
    int64_t lid = m_coord->loop(sid, -1, &lstatus);

    if (lid < 0)
    {
        switch (sstatus)
        {
            case REPLICANT_NEED_BOOTSTRAP:
                LOG(ERROR) << "could not connect to the coordinator to register this instance";
                return -1;
            case REPLICANT_SUCCESS:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_TIMEOUT:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
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
            LOG(ERROR) << "could not connect to the coordinator to register this instance";
            return -1;
        case REPLICANT_OBJ_EXIST:
        case REPLICANT_SERVER_ERROR:
        case REPLICANT_BAD_LIBRARY:
        case REPLICANT_TIMEOUT:
        case REPLICANT_MISBEHAVING_SERVER:
        case REPLICANT_INTERNAL_ERROR:
        case REPLICANT_NONE_PENDING:
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
            case COORD_MALFORMED:
            case COORD_NOT_FOUND:
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

bool
daemon :: wait_for_config(configuration* config)
{
    uint64_t retry = 1000000UL;
    bool need_to_retry = false;

    while (s_continue)
    {
        if (need_to_retry)
        {
            LOG(ERROR) << "connection to the coordinator failed; retrying in " << retry / 1000000. << " milliseconds";
            timespec ts;
            ts.tv_sec = retry / 1000000000UL;
            ts.tv_nsec = retry % 1000000000UL;
            nanosleep(&ts, NULL); // don't check the error
            retry = std::min(retry * 2, 1000000000UL);
            need_to_retry = false;
            continue; // so we can pick up on changes to s_continue and break;
        }

        need_to_retry = true;

        replicant_returncode sstatus = REPLICANT_GARBAGE;
        const char* output;
        size_t output_sz;
        int64_t sid = m_coord->send("hyperdex", 8, "get-config", "", 0,
                                    &sstatus, &output, &output_sz);

        if (sid < 0)
        {
            switch (sstatus)
            {
                case REPLICANT_TIMEOUT:
                    need_to_retry = false;
                    continue;
                case REPLICANT_NEED_BOOTSTRAP:
                    LOG(ERROR) << "communication error with the coordinator: "
                               << m_coord->last_error_desc()
                               << "(" << sstatus << ";"
                               << m_coord->last_error_file() << ":"
                               << m_coord->last_error_line() << ")";
                    continue;
                case REPLICANT_SUCCESS:
                case REPLICANT_FUNC_NOT_FOUND:
                case REPLICANT_OBJ_EXIST:
                case REPLICANT_OBJ_NOT_FOUND:
                case REPLICANT_SERVER_ERROR:
                case REPLICANT_BAD_LIBRARY:
                case REPLICANT_MISBEHAVING_SERVER:
                case REPLICANT_INTERNAL_ERROR:
                case REPLICANT_NONE_PENDING:
                case REPLICANT_GARBAGE:
                    LOG(ERROR) << "unexpected error with coordinator connection: "
                               << m_coord->last_error_desc()
                               << "(" << sstatus << ";"
                               << m_coord->last_error_file() << ":"
                               << m_coord->last_error_line() << ")";
                    continue;
                default:
                    abort();
            }
        }

        replicant_returncode lstatus = REPLICANT_GARBAGE;
        int64_t lid = m_coord->loop(sid, -1, &lstatus);

        if (lid < 0)
        {
            m_coord->kill(sid);

            switch (lstatus)
            {
                case REPLICANT_TIMEOUT:
                    need_to_retry = false;
                    continue;
                case REPLICANT_NEED_BOOTSTRAP:
                    LOG(ERROR) << "communication error with the coordinator: "
                               << m_coord->last_error_desc()
                               << "(" << sstatus << ";"
                               << m_coord->last_error_file() << ":"
                               << m_coord->last_error_line() << ")";
                    continue;
                case REPLICANT_SUCCESS:
                case REPLICANT_FUNC_NOT_FOUND:
                case REPLICANT_OBJ_EXIST:
                case REPLICANT_OBJ_NOT_FOUND:
                case REPLICANT_SERVER_ERROR:
                case REPLICANT_BAD_LIBRARY:
                case REPLICANT_MISBEHAVING_SERVER:
                case REPLICANT_INTERNAL_ERROR:
                case REPLICANT_NONE_PENDING:
                case REPLICANT_GARBAGE:
                    LOG(ERROR) << "unexpected error with coordinator connection: "
                               << m_coord->last_error_desc()
                               << "(" << sstatus << ";"
                               << m_coord->last_error_file() << ":"
                               << m_coord->last_error_line() << ")";
                    continue;
                default:
                    abort();
            }
        }

        assert(sid == lid);

        switch (sstatus)
        {
            case REPLICANT_SUCCESS:
                break;
            case REPLICANT_FUNC_NOT_FOUND:
                LOG(ERROR) << "coordinator missing \"get-config\" function: "
                           << m_coord->last_error_desc()
                           << "(" << sstatus << ";"
                           << m_coord->last_error_file() << ":"
                           << m_coord->last_error_line() << ")";
                continue;
            case REPLICANT_OBJ_NOT_FOUND:
                LOG(ERROR) << "coordinator missing \"hyperdex\" object: "
                           << m_coord->last_error_desc()
                           << "(" << sstatus << ";"
                           << m_coord->last_error_file() << ":"
                           << m_coord->last_error_line() << ")";
                continue;
            case REPLICANT_TIMEOUT:
                continue;
            case REPLICANT_NEED_BOOTSTRAP:
                LOG(ERROR) << "communication error with the coordinator: "
                           << m_coord->last_error_desc()
                           << "(" << sstatus << ";"
                           << m_coord->last_error_file() << ":"
                           << m_coord->last_error_line() << ")";
                continue;
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
                LOG(ERROR) << "unexpected error with coordinator connection: "
                           << m_coord->last_error_desc()
                           << "(" << sstatus << ";"
                           << m_coord->last_error_file() << ":"
                           << m_coord->last_error_line() << ")";
                continue;
            default:
                abort();
        }

        e::guard g = e::makeguard(replicant_destroy_output, output, output_sz);
        g.use_variable();
        e::unpacker up(output, output_sz);
        up = up >> *config;

        if (up.error())
        {
            LOG(ERROR) << "configuration does not parse (file a bug); here's some hex: " << e::slice(output, output_sz).hex();
            continue;
        }

        return true;
    }

    return false;
}

void
daemon :: loop()
{
    LOG(INFO) << "network thread started";
    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        return;
    }

    if (pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "pthread_sigmask";
        return;
    }

    server_id from;
    virtual_server_id vfrom;
    virtual_server_id vto;
    network_msgtype type;
    std::auto_ptr<e::buffer> msg;
    e::unpacker up;

    while (s_continue && m_comm.recv(&from, &vfrom, &vto, &type, &msg, &up))
    {
        assert(from != server_id());
        assert(vto != virtual_server_id());

        switch (type)
        {
            case REQ_GET:
                process_req_get(from, vfrom, vto, msg, up);
                break;
            case REQ_ATOMIC:
                process_req_atomic(from, vfrom, vto, msg, up);
                break;
            case REQ_SEARCH_START:
                process_req_search_start(from, vfrom, vto, msg, up);
                break;
            case REQ_SEARCH_NEXT:
                process_req_search_next(from, vfrom, vto, msg, up);
                break;
            case REQ_SEARCH_STOP:
                process_req_search_stop(from, vfrom, vto, msg, up);
                break;
            case REQ_SORTED_SEARCH:
                process_req_sorted_search(from, vfrom, vto, msg, up);
                break;
            case REQ_GROUP_DEL:
                process_req_group_del(from, vfrom, vto, msg, up);
                break;
            case REQ_COUNT:
                process_req_count(from, vfrom, vto, msg, up);
                break;
            case CHAIN_PUT:
                process_chain_put(from, vfrom, vto, msg, up);
                break;
            case CHAIN_DEL:
                process_chain_del(from, vfrom, vto, msg, up);
                break;
            case CHAIN_SUBSPACE:
                process_chain_subspace(from, vfrom, vto, msg, up);
                break;
            case CHAIN_ACK:
                process_chain_ack(from, vfrom, vto, msg, up);
                break;
            case RESP_GET:
            case RESP_ATOMIC:
            case RESP_SEARCH_ITEM:
            case RESP_SEARCH_DONE:
            case RESP_SORTED_SEARCH:
            case RESP_GROUP_DEL:
            case RESP_COUNT:
            case CONFIGMISMATCH:
            case PACKET_NOP:
            default:
                LOG(INFO) << "received " << type << " message which servers do not process";
                break;
        }
    }

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

    size_t sz = HYPERDEX_HEADER_SIZE_VS
              + sizeof(uint64_t)
              + sizeof(uint16_t)
              + pack_size(value);
    msg.reset(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(HYPERDEX_HEADER_SIZE_VS);
    pa = pa << nonce << static_cast<uint16_t>(result) << value;
    m_comm.send(vto, from, RESP_GET, msg);
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

    bool fail_if_not_found = flags & 1;
    bool fail_if_found = flags & 2;
    bool has_funcalls = flags & 128;
    m_repl.client_atomic(from, vto, nonce, fail_if_not_found, fail_if_found, !has_funcalls, key, &checks, &funcs);
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
daemon :: process_chain_put(server_id,
                            virtual_server_id vfrom,
                            virtual_server_id vto,
                            std::auto_ptr<e::buffer> msg,
                            e::unpacker up)
{
    uint64_t version;
    uint8_t fresh;
    e::slice key;
    std::vector<e::slice> value;

    if ((up >> version >> fresh >> key >> value).error())
    {
        LOG(WARNING) << "unpack of CHAIN_PUT failed; here's some hex:  " << msg->hex();
        return;
    }

    m_repl.chain_put(vfrom, vto, version, fresh == 1, msg, key, value);
}

void
daemon :: process_chain_del(server_id,
                            virtual_server_id vfrom,
                            virtual_server_id vto,
                            std::auto_ptr<e::buffer> msg,
                            e::unpacker up)
{
    uint64_t version;
    e::slice key;

    if ((up >> version >> key).error())
    {
        LOG(WARNING) << "unpack of CHAIN_DEL failed; here's some hex:  " << msg->hex();
        return;
    }

    m_repl.chain_del(vfrom, vto, version, msg, key);
}

void
daemon :: process_chain_subspace(server_id,
                                 virtual_server_id vfrom,
                                 virtual_server_id vto,
                                 std::auto_ptr<e::buffer> msg,
                                 e::unpacker up)
{
    uint64_t version;
    e::slice key;
    std::vector<e::slice> value;
    std::vector<uint64_t> hashes;

    if ((up >> version >> key >> value >> hashes).error())
    {
        LOG(WARNING) << "unpack of CHAIN_SUBSPACE failed; here's some hex:  " << msg->hex();
        return;
    }

    m_repl.chain_subspace(vfrom, vto, version, msg, key, value, hashes);
}

void
daemon :: process_chain_ack(server_id,
                            virtual_server_id vfrom,
                            virtual_server_id vto,
                            std::auto_ptr<e::buffer> msg,
                            e::unpacker up)
{
    uint64_t version;
    e::slice key;

    if ((up >> version >> key).error())
    {
        LOG(WARNING) << "unpack of CHAIN_ACK failed; here's some hex:  " << msg->hex();
        return;
    }

    m_repl.chain_ack(vfrom, vto, version, key);
}
