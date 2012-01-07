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

// HyperDisk
#include "hyperdisk/hyperdisk/reference.h"

// HyperDex
#include "hyperdex/hyperdex/network_constants.h"
#include "hyperdex/hyperdex/packing.h"

// HyperDaemon
#include "hyperdaemon/datalayer.h"
#include "hyperdaemon/logical.h"
#include "hyperdaemon/network_worker.h"
#include "hyperdaemon/ongoing_state_transfers.h"
#include "hyperdaemon/replication_manager.h"
#include "hyperdaemon/searches.h"

using hyperdex::entityid;
using hyperdex::network_msgtype;
using hyperdex::network_returncode;

hyperdaemon :: network_worker :: network_worker(datalayer* data,
                                                logical* comm,
                                                searches* ssss,
                                                ongoing_state_transfers* ost,
                                                replication_manager* repl)
    : m_continue(true)
    , m_data(data)
    , m_comm(comm)
    , m_ssss(ssss)
    , m_ost(ost)
    , m_repl(repl)
{
}

hyperdaemon :: network_worker :: ~network_worker() throw ()
{
    if (m_continue)
    {
        m_continue = false;
        LOG(INFO) << "Network worker object not cleanly shutdown.";
    }
}

void
hyperdaemon :: network_worker :: run()
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
    unsigned int seed = pthread_self();

    while (m_continue && m_comm->recv(&from, &to, &type, &msg))
    {
        e::buffer::unpacker up = msg->unpack_from(m_comm->header_size());
        uint64_t nonce;

        if (type == hyperdex::REQ_GET)
        {
            e::slice key;

            if ((up >> nonce >> key).error())
            {
                LOG(WARNING) << "unpack of REQ_GET failed; here's some hex:  " << msg->hex();
                continue;
            }

            std::vector<e::slice> value;
            uint64_t version;
            hyperdisk::reference ref;
            network_returncode result;

            switch (m_data->get(to.get_region(), key, &value, &version, &ref))
            {
                case hyperdisk::SUCCESS:
                    result = hyperdex::NET_SUCCESS;
                    break;
                case hyperdisk::NOTFOUND:
                    result = hyperdex::NET_NOTFOUND;
                    break;
                case hyperdisk::WRONGARITY:
                    result = hyperdex::NET_WRONGARITY;
                    break;
                case hyperdisk::MISSINGDISK:
                    LOG(ERROR) << "GET caused a MISSINGDISK at the data layer.";
                    result = hyperdex::NET_SERVERERROR;
                    break;
                case hyperdisk::DATAFULL:
                case hyperdisk::SEARCHFULL:
                case hyperdisk::SYNCFAILED:
                case hyperdisk::DROPFAILED:
                case hyperdisk::SPLITFAILED:
                case hyperdisk::DIDNOTHING:
                default:
                    LOG(ERROR) << "GET returned unacceptable error code.";
                    result = hyperdex::NET_SERVERERROR;
                    break;
            }

            size_t sz = m_comm->header_size() + sizeof(uint64_t)
                      + sizeof(uint16_t) + hyperdex::packspace(value);
            msg.reset(e::buffer::create(sz));
            e::buffer::packer pa = msg->pack_at(m_comm->header_size());
            pa = pa << nonce << static_cast<uint16_t>(result) << value;
            assert(!pa.error());
            m_comm->send(to, from, hyperdex::RESP_GET, msg);
        }
        else if (type == hyperdex::REQ_PUT)
        {
            uint32_t attrs_sz;
            e::slice key;
            std::vector<std::pair<uint16_t, e::slice> > attrs;
            up = up >> nonce >> key >> attrs_sz;

            for (uint32_t i = 0; i < attrs_sz; ++i)
            {
                uint16_t dimnum;
                e::slice val;
                up = up >> dimnum >> val;
                attrs.push_back(std::make_pair(dimnum, val));
            }

            m_repl->client_put(from, to, nonce, msg, key, attrs);
        }
        else if (type == hyperdex::REQ_DEL)
        {
            up = up >> nonce;

            if (up.error())
            {
                LOG(WARNING) << "unpack of REQ_DEL failed; here's some hex:  " << msg->hex();
                continue;
            }

            e::slice key = up.as_slice();
            m_repl->client_del(from, to.get_region(), nonce, msg, key);
        }
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
                m_ssss->start(to.get_region(), from, searchid, nonce, s);
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

            m_ssss->next(to.get_region(), from, searchid, nonce);
        }
        else if (type == hyperdex::REQ_SEARCH_STOP)
        {
            uint64_t searchid;

            if ((up >> nonce >> searchid).error())
            {
                LOG(WARNING) << "unpack of REQ_SEARCH_STOP failed; here's some hex:  " << msg->hex();
                continue;
            }

            m_ssss->stop(to.get_region(), from, searchid);
        }
        else if (type == hyperdex::CHAIN_PUT)
        {
            uint64_t version;
            uint8_t fresh;
            e::slice key;
            std::vector<e::slice> value;

            if ((up >> version >> fresh >> key >> value).error())
            {
                LOG(WARNING) << "unpack of CHAIN_PUT failed; here's some hex:  " << msg->hex();
                continue;
            }

            m_repl->chain_put(from, to, version, fresh == 1, msg, key, value);
        }
        else if (type == hyperdex::CHAIN_DEL)
        {
            uint64_t version;
            e::slice key;

            if ((up >> version >> key).error())
            {
                LOG(WARNING) << "unpack of CHAIN_DEL failed; here's some hex:  " << msg->hex();
                continue;
            }

            m_repl->chain_del(from, to, version, msg, key);
        }
        else if (type == hyperdex::CHAIN_PENDING)
        {
            uint64_t version;
            e::slice key;

            if ((up >> version >> key).error())
            {
                LOG(WARNING) << "unpack of CHAIN_PENDING failed; here's some hex:  " << msg->hex();
                continue;
            }

            m_repl->chain_pending(from, to, version, msg, key);
        }
        else if (type == hyperdex::CHAIN_SUBSPACE)
        {
            uint64_t version;
            e::slice key;
            std::vector<e::slice> value;
            uint64_t nextpoint;

            if ((up >> version >> key >> value >> nextpoint).error())
            {
                LOG(WARNING) << "unpack of CHAIN_SUBSPACE failed; here's some hex:  " << msg->hex();
                continue;
            }

            m_repl->chain_subspace(from, to, version, msg, key, value, nextpoint);
        }
        else if (type == hyperdex::CHAIN_ACK)
        {
            uint64_t version;
            e::slice key;

            if ((up >> version >> key).error())
            {
                LOG(WARNING) << "unpack of CHAIN_ACK failed; here's some hex:  " << msg->hex();
                continue;
            }

            m_repl->chain_ack(from, to, version, msg, key);

            if (rand_r(&seed) < (0.01 * RAND_MAX))
            {
                m_data->flush(to.get_region(), 200);
            }
        }
#if 0
        else if (type == hyperdex::XFER_MORE)
        {
            m_ost->region_transfer(from, to);
        }
        else if (type == hyperdex::XFER_DONE)
        {
            m_ost->region_transfer_done(from, to);
        }
        else if (type == hyperdex::XFER_DATA)
        {
            uint64_t xfer_num;
            uint8_t op;
            uint64_t version;
            e::slice key;
            std::vector<e::slice> value;

            if ((up >> xfer_num >> op >> version >> key >> value).error())
            {
                LOG(WARNING) << "unpack of XFER_DATA failed; here's some hex:  " << msg->hex();
                continue;
            }

            m_ost->region_transfer(from, to.subspace, xfer_num,
                                   op == 1, version, msg, key, value);
        }
#endif
        else
        {
            LOG(INFO) << "Message of unknown type received.";
        }
    }
}

void
hyperdaemon :: network_worker :: shutdown()
{
    // TODO:  This is not the proper shutdown method.  Proper shutdown is a
    // two-stage process, and requires global coordination.
    m_continue = false;
}
