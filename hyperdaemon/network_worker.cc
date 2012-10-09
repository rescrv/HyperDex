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
#include "disk/disk.h"
#include "common/funcall.h"


#include "daemon/datalayer.h"
#include "datatypes/microcheck.h"
#include "hyperdex/hyperdex/network_constants.h"
#include "hyperdex/hyperdex/packing.h"
#include "hyperdaemon/logical.h"
#include "hyperdaemon/network_worker.h"
#include "hyperdaemon/ongoing_state_transfers.h"
#include "hyperdaemon/replication_manager.h"
#include "hyperdaemon/searches.h"

using hyperdex::datalayer;
using hyperdex::disk_reference;
using hyperdex::entityid;
using hyperdex::funcall;
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
            disk_reference ref;
            network_returncode result;

            switch (m_data->get(to.get_region(), key, &value, &version, &ref))
            {
                case hyperdex::DISK_SUCCESS:
                    result = hyperdex::NET_SUCCESS;
                    break;
                case hyperdex::DISK_NOT_FOUND:
                    result = hyperdex::NET_NOTFOUND;
                    break;
                case hyperdex::DISK_WRONGARITY:
                    result = hyperdex::NET_BADDIMSPEC;
                    break;
                case hyperdex::DISK_CORRUPT:
                    LOG(ERROR) << "GET caused a CORRUPT at the data layer.";
                    result = hyperdex::NET_SERVERERROR;
                    break;
                case hyperdex::DISK_FATAL_ERROR:
                    LOG(ERROR) << "GET caused a FATAL_ERROR at the data layer.";
                    result = hyperdex::NET_SERVERERROR;
                    break;
                case hyperdex::DISK_MISSINGDISK:
                    LOG(ERROR) << "GET caused a MISSINGDISK at the data layer.";
                    result = hyperdex::NET_SERVERERROR;
                    break;
                case hyperdex::DISK_DATAFULL:
                case hyperdex::DISK_SEARCHFULL:
                case hyperdex::DISK_SYNCFAILED:
                case hyperdex::DISK_DROPFAILED:
                case hyperdex::DISK_SPLITFAILED:
                case hyperdex::DISK_DIDNOTHING:
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
            m_comm->send(to, from, hyperdex::RESP_GET, msg);
        }
        else if (type == hyperdex::REQ_ATOMIC)
        {
            uint8_t flags;
            e::slice key;
            std::vector<microcheck> checks;
            std::vector<funcall> funcalls;
            up = up >> nonce >> key >> flags >> checks >> funcalls;

            if (up.error())
            {
                LOG(WARNING) << "unpack of REQ_ATOMIC failed; here's some hex:  " << msg->hex();
                continue;
            }

            bool fail_if_not_found = flags & 1;
            bool fail_if_found = flags & 2;
            bool has_funcalls = flags & 128;

            if (has_funcalls)
            {
                m_repl->client_atomic(hyperdex::RESP_ATOMIC, from, to, nonce, msg,
                                      fail_if_not_found, fail_if_found, key, &checks, &funcalls);
            }
            else
            {
                m_repl->client_del(hyperdex::RESP_ATOMIC, from, to, nonce, msg,
                                   key, &checks);
            }
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
        }
#if 0
        else if (type == hyperdex::XFER_MORE)
        {
            m_ost->region_transfer_send(from, to);
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

            if ((up = up >> xfer_num >> version >> key >> op).error())
            {
                LOG(WARNING) << "unpack of XFER_DATA failed; here's some hex:  " << msg->hex();
                continue;
            }

            if (op)
            {
                if ((up >> value).error())
                {
                    LOG(WARNING) << "unpack of XFER_DATA failed; here's some hex:  " << msg->hex();
                    continue;
                }
            }

            m_ost->region_transfer_recv(from, to.subspace, xfer_num,
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
