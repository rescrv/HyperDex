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
#include <e/timer.h>

// HyperDex
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

daemon :: daemon(po6::net::location bind_to,
                 po6::net::hostname coordinator,
                 po6::pathname datadir,
                 uint16_t num_threads,
                 bool _daemonize)
    : m_token(0x4141414141414141ULL)
    , m_us(bind_to.address, bind_to.port, 1, 6999, 1)
    , m_threads(num_threads)
    , m_coordinator(coordinator)
    , m_datadir(datadir)
    , m_daemonize(_daemonize)
    , m_cl(coordinator)
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

int
daemon :: run()
{
    google::InitGoogleLogging("hyperdex-daemon");
    google::InstallFailureSignalHandler();

    if (chdir(m_datadir.get()) < 0)
    {
        PLOG(ERROR) << "could not change to the data directory";
        return EXIT_FAILURE;
    }

    if (m_daemonize)
    {
        if (!daemonize())
        {
            return EXIT_FAILURE;
        }
    }
    else
    {
        google::LogToStderr();
    }

    if (!install_signal_handlers())
    {
        return EXIT_FAILURE;
    }

    if (!generate_identifier_token())
    {
        return EXIT_FAILURE;
    }

    for (size_t i = 0; i < m_threads.size(); ++i)
    {
        m_threads[i].reset(new po6::threads::thread(std::tr1::bind(&daemon::loop, this)));
        m_threads[i]->start();
    }

    m_data.open(m_datadir);
    m_us.inbound_port = m_comm.in_port();
    m_us.outbound_port = m_comm.out_port();

    // Create our announce string.
    std::ostringstream announce;
    announce << "instance\t" << m_us.address << "\t"
                             << m_us.inbound_port << "\t"
                             << 6999 << "\t"
                             << getpid() << "\t"
                             << m_token;
    m_cl.set_announce(announce.str());

    LOG(INFO) << "Network workers started.";
    uint64_t now;
    uint64_t disconnected_at = e::time();
    uint64_t nanos_to_wait = 5000000;
    // Reconnect right away, then 10ms, then 20ms, .... 1s

    while (s_continue)
    {
        switch (m_cl.poll(1, -1))
        {
            case hyperdex::coordinatorlink::SUCCESS:
                break;
            case hyperdex::coordinatorlink::CONNECTFAIL:
                now = e::time();
                PLOG(ERROR) << "Coordinator connection failed";

                if (now - disconnected_at < nanos_to_wait)
                {
                    e::sleep_ns(0, nanos_to_wait - (now - disconnected_at));
                }

                disconnected_at = e::time();
                nanos_to_wait = std::min(nanos_to_wait * 2, static_cast<uint64_t>(1000000000));
                continue;;
            default:
                abort();
        }

        if (m_cl.unacknowledged())
        {
            configuration new_config = m_cl.config();

            LOG(INFO) << "Installing new configuration version " << m_cl.config().version();
            // Prepare for our new configuration.
            // These operations should assume that there will be network
            // activity, and that the network threads will be in full force.
            m_comm.prepare(m_config, new_config, m_us);
            m_data.prepare(m_config, new_config, m_us);
            m_repl.prepare(m_config, new_config, m_us);
            m_sm.prepare(m_config, new_config, m_us);

            // Protect ourself against exceptions.
            e::guard g1 = e::makeobjguard(m_comm, &hyperdaemon::logical::unpause);
            e::guard g2 = e::makeobjguard(m_comm, &hyperdaemon::logical::shutdown);

            LOG(INFO) << "Pausing communication for reconfiguration.";

            // Make sure that we wait until everyone is paused.
            configuration old_config = m_config;
            m_comm.pause();

            // Here is the critical section.  This is is mutually exclusive with the
            // network workers' loop.
            m_comm.reconfigure(old_config, new_config, m_us);
            m_data.reconfigure(old_config, new_config, m_us);
            m_repl.reconfigure(old_config, new_config, m_us);
            m_sm.reconfigure(old_config, new_config, m_us);
            m_config = new_config;
            m_comm.unpause();
            g1.dismiss();
            g2.dismiss();
            LOG(INFO) << "Reconfiguration complete; unpausing communication.";

            // Cleanup anything not specified by our new configuration.
            // These operations should assume that there will be network
            // activity, and that the network threads will be in full force..
            m_sm.cleanup(old_config, m_config, m_us);
            m_repl.cleanup(old_config, m_config, m_us);
            m_data.cleanup(old_config, m_config, m_us);
            m_comm.cleanup(old_config, m_config, m_us);
            m_cl.acknowledge();
        }
    }

    LOG(INFO) << "Exiting daemon.";
    // XXX
    m_data.close();

    for (size_t i = 0; i < m_threads.size(); ++i)
    {
        m_threads[i]->join();
    }

    return EXIT_SUCCESS;
}

bool
daemon :: install_signal_handlers()
{
    if (!install_signal_handler(SIGHUP, exit_on_signal))
    {
        return false;
    }

    if (!install_signal_handler(SIGINT, exit_on_signal))
    {
        return false;
    }

    if (!install_signal_handler(SIGTERM, exit_on_signal))
    {
        return false;
    }

    return true;
}

bool
daemon :: install_signal_handler(int signum, void (*func)(int))
{
    struct sigaction handle;
    handle.sa_handler = func;
    sigfillset(&handle.sa_mask);
    handle.sa_flags = SA_RESTART;
    return sigaction(signum, &handle, NULL) >= 0;
}

bool
daemon :: generate_identifier_token()
{
    LOG(INFO) << "generating random token";
    po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

    if (sysrand.get() < 0)
    {
        PLOG(ERROR) << "could not open /dev/urandom";
        return false;
    }

    if (sysrand.read(&m_token, sizeof(m_token)) != sizeof(m_token))
    {
        PLOG(ERROR) << "could not read from /dev/urandom";
        return false;
    }

    LOG(INFO) << "random token:  " << m_token;
    return true;
}

bool
daemon :: daemonize()
{
    google::SetLogDestination(google::INFO, po6::join(m_datadir, po6::pathname("hyperdex-")).get());

    if (::daemon(1, 0) < 0)
    {
        PLOG(ERROR) << "could not daemonize";
        return false;
    }

    return true;
}

void
daemon :: loop()
{
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

    entityid from;
    entityid to;
    network_msgtype type;
    std::auto_ptr<e::buffer> msg;

    while (s_continue && m_comm.recv(&from, &to, &type, &msg))
    {
        e::buffer::unpacker up = msg->unpack_from(m_comm.header_size());

        switch (type)
        {
            case REQ_GET:
                process_req_get(from, to, msg, up);
                break;
            case REQ_ATOMIC:
                process_req_atomic(from, to, msg, up);
                break;
            case REQ_SEARCH_START:
                process_req_search_start(from, to, msg, up);
                break;
            case REQ_SEARCH_NEXT:
                process_req_search_next(from, to, msg, up);
                break;
            case REQ_SEARCH_STOP:
                process_req_search_stop(from, to, msg, up);
                break;
            case REQ_SORTED_SEARCH:
                process_req_sorted_search(from, to, msg, up);
                break;
            case REQ_GROUP_DEL:
                process_req_group_del(from, to, msg, up);
                break;
            case REQ_COUNT:
                process_req_count(from, to, msg, up);
                break;
            case CHAIN_PUT:
                process_chain_put(from, to, msg, up);
                break;
            case CHAIN_DEL:
                process_chain_del(from, to, msg, up);
                break;
            case CHAIN_SUBSPACE:
                process_chain_subspace(from, to, msg, up);
                break;
            case CHAIN_ACK:
                process_chain_ack(from, to, msg, up);
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
}

void
daemon :: process_req_get(entityid from,
                          entityid to,
                          std::auto_ptr<e::buffer> msg,
                          e::buffer::unpacker up)
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

    switch (m_data.get(to.get_region(), key, &value, &version, &ref))
    {
        case hyperdex::datalayer::SUCCESS:
            result = hyperdex::NET_SUCCESS;
            break;
        case hyperdex::datalayer::NOT_FOUND:
            result = hyperdex::NET_NOTFOUND;
            break;
        case hyperdex::datalayer::BAD_ENCODING:
        case hyperdex::datalayer::CORRUPTION:
        case hyperdex::datalayer::IO_ERROR:
        case hyperdex::datalayer::LEVELDB_ERROR:
        default:
            LOG(ERROR) << "GET returned unacceptable error code.";
            result = hyperdex::NET_SERVERERROR;
            break;
    }

    size_t sz = m_comm.header_size() + sizeof(uint64_t)
              + sizeof(uint16_t) + pack_size(value);
    msg.reset(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(m_comm.header_size());
    pa = pa << nonce << static_cast<uint16_t>(result) << value;
    m_comm.send(to, from, hyperdex::RESP_GET, msg);
}

void
daemon :: process_req_atomic(entityid from,
                             entityid to,
                             std::auto_ptr<e::buffer> msg,
                             e::buffer::unpacker up)
{
    uint64_t nonce;
    uint8_t flags;
    e::slice key;
    std::vector<attribute_check> checks;
    std::vector<funcall> funcalls;
    up = up >> nonce >> key >> flags >> checks >> funcalls;

    if (up.error())
    {
        LOG(WARNING) << "unpack of REQ_ATOMIC failed; here's some hex:  " << msg->hex();
        return;
    }

    bool fail_if_not_found = flags & 1;
    bool fail_if_found = flags & 2;
    bool has_funcalls = flags & 128;

    if (has_funcalls)
    {
        m_repl.client_atomic(hyperdex::RESP_ATOMIC, from, to, nonce, msg,
                             fail_if_not_found, fail_if_found, key, &checks, &funcalls);
    }
    else
    {
        m_repl.client_del(hyperdex::RESP_ATOMIC, from, to, nonce, msg,
                          key, &checks);
    }
}

void
daemon :: process_req_search_start(entityid from,
                                   entityid to,
                                   std::auto_ptr<e::buffer> msg,
                                   e::buffer::unpacker up)
{
    LOG(FATAL) << "NOT IMPLEMENTED";
}

void
daemon :: process_req_search_next(entityid from,
                                  entityid to,
                                  std::auto_ptr<e::buffer> msg,
                                  e::buffer::unpacker up)
{
    LOG(FATAL) << "NOT IMPLEMENTED";
}

void
daemon :: process_req_search_stop(entityid from,
                                  entityid to,
                                  std::auto_ptr<e::buffer> msg,
                                  e::buffer::unpacker up)
{
    LOG(FATAL) << "NOT IMPLEMENTED";
}

void
daemon :: process_req_sorted_search(entityid from,
                                    entityid to,
                                    std::auto_ptr<e::buffer> msg,
                                    e::buffer::unpacker up)
{
    LOG(FATAL) << "NOT IMPLEMENTED";
}

void
daemon :: process_req_group_del(entityid from,
                                entityid to,
                                std::auto_ptr<e::buffer> msg,
                                e::buffer::unpacker up)
{
    LOG(FATAL) << "NOT IMPLEMENTED";
}

void
daemon :: process_req_count(entityid from,
                            entityid to,
                            std::auto_ptr<e::buffer> msg,
                            e::buffer::unpacker up)
{
    LOG(FATAL) << "NOT IMPLEMENTED";
}

void
daemon :: process_chain_put(entityid from,
                            entityid to,
                            std::auto_ptr<e::buffer> msg,
                            e::buffer::unpacker up)
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

    m_repl.chain_put(from, to, version, fresh == 1, msg, key, value);
}

void
daemon :: process_chain_del(entityid from,
                            entityid to,
                            std::auto_ptr<e::buffer> msg,
                            e::buffer::unpacker up)
{
    uint64_t version;
    e::slice key;

    if ((up >> version >> key).error())
    {
        LOG(WARNING) << "unpack of CHAIN_DEL failed; here's some hex:  " << msg->hex();
        return;
    }

    m_repl.chain_del(from, to, version, msg, key);
}

void
daemon :: process_chain_subspace(entityid from,
                             entityid to,
                             std::auto_ptr<e::buffer> msg,
                             e::buffer::unpacker up)
{
    uint64_t version;
    e::slice key;
    std::vector<e::slice> value;
    uint64_t nextpoint;

    if ((up >> version >> key >> value >> nextpoint).error())
    {
        LOG(WARNING) << "unpack of CHAIN_SUBSPACE failed; here's some hex:  " << msg->hex();
        return;
    }

    m_repl.chain_subspace(from, to, version, msg, key, value, nextpoint);
}

void
daemon :: process_chain_ack(entityid from,
                             entityid to,
                             std::auto_ptr<e::buffer> msg,
                             e::buffer::unpacker up)
{
    uint64_t version;
    e::slice key;

    if ((up >> version >> key).error())
    {
        LOG(WARNING) << "unpack of CHAIN_ACK failed; here's some hex:  " << msg->hex();
        return;
    }

    m_repl.chain_ack(from, to, version, msg, key);
}

#if 0
        else if (type == hyperdex::REQ_SEARCH_START)
        {
            uint64_t searchid;
            hyperspacehashing::search s(0);

            if ((up >> nonce >> searchid >> s).error())
            {
                LOG(WARNING) << "unpack of REQ_SEARCH_START failed; here's some hex:  " << msg->hex();
                continue;
            }

            if (s.sanity_check())
            {
                m_ssss->start(to, from, searchid, nonce, msg, s);
            }
            else
            {
                LOG(INFO) << "Dropping search which fails sanity_check.";
            }
        }
        else if (type == hyperdex::REQ_SEARCH_NEXT)
        {
            uint64_t searchid;

            if ((up >> nonce >> searchid).error())
            {
                LOG(WARNING) << "unpack of REQ_SEARCH_NEXT failed; here's some hex:  " << msg->hex();
                continue;
            }

            m_ssss->next(to, from, searchid, nonce);
        }
        else if (type == hyperdex::REQ_SEARCH_STOP)
        {
            uint64_t searchid;

            if ((up >> nonce >> searchid).error())
            {
                LOG(WARNING) << "unpack of REQ_SEARCH_STOP failed; here's some hex:  " << msg->hex();
                continue;
            }

            m_ssss->stop(to, from, searchid);
        }
        else if (type == hyperdex::REQ_SORTED_SEARCH)
        {
            hyperspacehashing::search s(0);
            uint64_t limit = 0;
            uint16_t attrno = 0;
            int8_t max = 0;

            if ((up >> nonce >> s >> limit >> attrno >> max).error())
            {
                LOG(WARNING) << "unpack of REQ_SEARCH_STOP failed; here's some hex:  " << msg->hex();
                continue;
            }

            if (s.sanity_check())
            {
                m_ssss->sorted_search(to, from, nonce, s, limit, attrno, max != 0);
            }
            else
            {
                LOG(INFO) << "Dropping sorted_search which fails sanity_check.";
            }
        }
        else if (type == hyperdex::REQ_GROUP_DEL)
        {
            hyperspacehashing::search s(0);

            if ((up >> nonce >> s).error())
            {
                LOG(WARNING) << "unpack of REQ_GROUP_DEL failed; here's some hex:  " << msg->hex();
                continue;
            }

            hyperdex::network_msgtype mt(hyperdex::REQ_ATOMIC);
            e::slice sl("\x01\x00\x00\x00\x00\x00\x00\x00\x00", 9);

            if (s.sanity_check())
            {
                m_ssss->group_keyop(to, from, nonce, s, mt, sl);
            }
            else
            {
                LOG(INFO) << "Dropping group_del which fails sanity_check.";
            }
        }
        else if (type == hyperdex::REQ_COUNT)
        {
            hyperspacehashing::search s(0);

            if ((up >> nonce >> s).error())
            {
                LOG(WARNING) << "unpack of REQ_COUNT failed; here's some hex:  " << msg->hex();
                continue;
            }

            if (s.sanity_check())
            {
                m_ssss->count(to, from, nonce, s);
            }
            else
            {
                LOG(INFO) << "Dropping count which fails sanity_check.";
            }
        }
#endif
