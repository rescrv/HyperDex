// Copyright (c) 2011, Cornell University
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
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/net/location.h>

// HyperDex
#include <hyperdex/buffer.h>
#include <hyperdex/limits.h>
#include <hyperdex/network_constants.h>
#include <hyperdex/network_worker.h>

hyperdex :: network_worker :: network_worker(datalayer* data,
                                             logical* comm,
                                             searches* ssss,
                                             replication_manager* repl)
    : m_continue(true)
    , m_data(data)
    , m_comm(comm)
    , m_ssss(ssss)
    , m_repl(repl)
{
}

hyperdex :: network_worker :: ~network_worker()
{
    if (m_continue)
    {
        m_continue = false;
        LOG(INFO) << "Network worker object not cleanly shutdown.";
    }
}

void
hyperdex :: network_worker :: run()
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

    hyperdex::entityid from;
    hyperdex::entityid to;
    network_msgtype type;
    e::buffer msg;
    uint32_t nonce;

    while (m_continue && m_comm->recv(&from, &to, &type, &msg))
    {
        try
        {
            if (type == REQ_GET)
            {
                e::buffer key;
                std::vector<e::buffer> value;
                uint64_t version;
                e::unpacker up(msg.unpack());
                up >> nonce;
                up.leftovers(&key);
                network_returncode result;

                switch (m_data->get(to.get_region(), key, &value, &version))
                {
                    case hyperdisk::SUCCESS:
                        result = NET_SUCCESS;
                        break;
                    case hyperdisk::NOTFOUND:
                        result = NET_NOTFOUND;
                        break;
                    case hyperdisk::WRONGARITY:
                        result = NET_WRONGARITY;
                        break;
                    case hyperdisk::MISSINGDISK:
                        LOG(ERROR) << "GET caused a MISSINGDISK at the data layer.";
                        result = NET_SERVERERROR;
                        break;
                    case hyperdisk::HASHFULL:
                    case hyperdisk::DATAFULL:
                    case hyperdisk::SEARCHFULL:
                    case hyperdisk::SYNCFAILED:
                    case hyperdisk::DROPFAILED:
                    default:
                        LOG(ERROR) << "GET returned unacceptable error code.";
                        result = NET_SERVERERROR;
                        break;
                }

                msg.clear();
                msg.pack() << nonce << static_cast<uint16_t>(result) << value;
                m_comm->send(to, from, RESP_GET, msg);
            }
            else if (type == REQ_PUT)
            {
                e::buffer key;
                std::vector<e::buffer> value;
                msg.unpack() >> nonce >> key >> value;
                m_repl->client_put(from, to.get_region(), nonce, key, value);
            }
            else if (type == REQ_DEL)
            {
                e::buffer key;
                e::unpacker up(msg.unpack());
                up >> nonce;
                up.leftovers(&key);
                m_repl->client_del(from, to.get_region(), nonce, key);
            }
            else if (type == REQ_SEARCH_START)
            {
                search s;
                msg.unpack() >> nonce >> s;
                m_ssss->start(from, nonce, to.get_region(), s);
            }
            else if (type == REQ_SEARCH_NEXT)
            {
                msg.unpack() >> nonce;
                m_ssss->next(from, nonce);
            }
            else if (type == REQ_SEARCH_STOP)
            {
                msg.unpack() >> nonce;
                m_ssss->stop(from, nonce);
            }
            else if (type == CHAIN_PUT)
            {
                e::buffer key;
                std::vector<e::buffer> value;
                uint64_t version;
                uint8_t fresh;
                msg.unpack() >> version >> fresh >> key >> value;
                m_repl->chain_put(from, to, version, fresh == 1, key, value);
            }
            else if (type == CHAIN_DEL)
            {
                e::buffer key;
                uint64_t version;
                msg.unpack() >> version >> key;
                m_repl->chain_del(from, to, version, key);
            }
            else if (type == CHAIN_PENDING)
            {
                e::buffer key;
                uint64_t version;
                msg.unpack() >> version >> key;
                m_repl->chain_pending(from, to, version, key);
            }
            else if (type == CHAIN_ACK)
            {
                e::buffer key;
                uint64_t version;
                msg.unpack() >> version >> key;
                m_repl->chain_ack(from, to, version, key);
            }
            else if (type == XFER_MORE)
            {
                m_repl->region_transfer(from, to);
            }
            else if (type == XFER_DONE)
            {
                m_repl->region_transfer_done(from, to);
            }
            else if (type == XFER_DATA)
            {
                uint64_t xfer_num;
                uint8_t op;
                uint64_t version;
                e::buffer key;
                std::vector<e::buffer> value;
                msg.unpack() >> xfer_num >> op >> version >> key >> value;
                m_repl->region_transfer(from, to.subspace, xfer_num,
                                        op == 1, version, key, value);
            }
            else
            {
                LOG(INFO) << "Message of unknown type received.";
            }
        }
        catch (std::out_of_range& e)
        {
            // Unpack error
        }
    }
}

void
hyperdex :: network_worker :: shutdown()
{
    // TODO:  This is not the proper shutdown method.  Proper shutdown is a
    // two-stage process, and requires global coordination.
    m_continue = false;
}
