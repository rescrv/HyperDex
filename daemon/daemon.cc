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

int s_interrupts = 0;
bool s_alarm = false;

static void
exit_on_signal(int /*signum*/)
{
    if (s_interrupts == 0)
    {
        RAW_LOG(ERROR, "interrupted: initiating shutdown (interrupt again to exit immediately)");
    }
    else
    {
        RAW_LOG(ERROR, "interrupted again: exiting immediately");
    }

    ++s_interrupts;
}

static void
handle_alarm(int /*signum*/)
{
    s_alarm = true;
}

static void
dummy(int /*signum*/)
{
}

daemon :: daemon()
    : m_us()
    , m_threads()
    , m_coord(this)
    , m_data(this)
    , m_comm(this)
    , m_repl(this)
    , m_stm(this)
    , m_sm(this)
    , m_config()
{
}

daemon :: ~daemon() throw ()
{
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
        std::cerr << "could not install SIGUSR1 handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGUSR1, dummy))
    {
        std::cerr << "could not install SIGUSR1 handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        return EXIT_FAILURE;
    }

    if (pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        return EXIT_FAILURE;
    }

    alarm(30);
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
        m_coord.set_coordinator_address(coordinator.address.c_str(), coordinator.port);

        if (!m_coord.reregister_id(m_us, bind_to))
        {
            return EXIT_FAILURE;
        }
    }
    else
    {
        LOG(INFO) << "starting new daemon from command-line arguments";
        m_coord.set_coordinator_address(coordinator.address.c_str(), coordinator.port);
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

        if (!m_data.initialize())
        {
            return EXIT_FAILURE;
        }
    }

    if (!m_data.save_state(m_us, bind_to, coordinator))
    {
        return EXIT_FAILURE;
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
    m_stm.setup();
    m_sm.setup();

    for (size_t i = 0; i < threads; ++i)
    {
        std::tr1::shared_ptr<po6::threads::thread> t(new po6::threads::thread(std::tr1::bind(&daemon::loop, this)));
        m_threads.push_back(t);
        t->start();
    }

    while (!m_coord.exit_wait_loop())
    {
        configuration old_config = m_config;
        configuration new_config;

        if (!m_coord.wait_for_config(&new_config))
        {
            continue;
        }

        if (old_config.version() >= new_config.version())
        {
            LOG(INFO) << "received new configuration version=" << new_config.version()
                      << " that's not newer than our current configuration version="
                      << old_config.version();
            continue;
        }

        // XXX we really should check the reconfigure_returncode even though
        // nothing right now fails when reconfiguring

        LOG(INFO) << "received new configuration version=" << new_config.version();
        // prepare
        m_data.prepare(old_config, new_config, m_us);
        m_comm.prepare(old_config, new_config, m_us);
        m_repl.prepare(old_config, new_config, m_us);
        m_stm.prepare(old_config, new_config, m_us);
        m_sm.prepare(old_config, new_config, m_us);
        // reconfigure
        LOG(INFO) << "preparations for reconfiguration done; pausing network communication";
        // this line to the "unpause" below are mutually exclusive with network workers
        m_comm.pause();
        m_data.reconfigure(old_config, new_config, m_us);
        m_comm.reconfigure(old_config, new_config, m_us);
        m_repl.reconfigure(old_config, new_config, m_us);
        m_stm.reconfigure(old_config, new_config, m_us);
        m_sm.reconfigure(old_config, new_config, m_us);
        m_config = new_config;
        m_comm.unpause();
        LOG(INFO) << "reconfiguration complete; unpausing network communication";
        // cleanup
        m_sm.cleanup(old_config, new_config, m_us);
        m_stm.cleanup(old_config, new_config, m_us);
        m_repl.cleanup(old_config, new_config, m_us);
        m_comm.cleanup(old_config, new_config, m_us);
        m_data.cleanup(old_config, new_config, m_us);
        // let the coordinator know we've moved to this config
        m_coord.ack_config(new_config.version());
    }

    m_comm.shutdown();

    for (size_t i = 0; i < m_threads.size(); ++i)
    {
        m_threads[i]->join();
    }

    m_coord.shutdown();

    if (m_coord.is_clean_shutdown())
    {
        LOG(INFO) << "hyperdex-daemon is gracefully shutting down";
    }

    m_sm.teardown();
    m_stm.teardown();
    m_repl.teardown();
    m_comm.teardown();
    m_data.teardown();
    int exit_status = EXIT_SUCCESS;

    if (m_coord.is_clean_shutdown())
    {
        if (!m_data.clear_dirty())
        {
            LOG(ERROR) << "unable to cleanly close the database";
            exit_status = EXIT_FAILURE;
        }
    }

    LOG(INFO) << "hyperdex-daemon will now terminate";
    return exit_status;
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
        PLOG(ERROR) << "could not block signals";
        return;
    }

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
            case CHAIN_OP:
                process_chain_op(from, vfrom, vto, msg, up);
                break;
            case CHAIN_SUBSPACE:
                process_chain_subspace(from, vfrom, vto, msg, up);
                break;
            case CHAIN_ACK:
                process_chain_ack(from, vfrom, vto, msg, up);
                break;
            case CHAIN_GC:
                process_chain_gc(from, vfrom, vto, msg, up);
                break;
            case XFER_OP:
                process_xfer_op(from, vfrom, vto, msg, up);
                break;
            case XFER_ACK:
                process_xfer_ack(from, vfrom, vto, msg, up);
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
        case datalayer::BAD_SEARCH:
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
    pa = pa << nonce << static_cast<uint16_t>(result) << value;
    m_comm.send_client(vto, from, RESP_GET, msg);
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
