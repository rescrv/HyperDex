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
#include <algorithm>

// Google Log
#include <glog/logging.h>

// HyperDex
#include "common/serialization.h"
#include "daemon/daemon.h"
#include "daemon/datalayer_iterator.h"
#include "daemon/state_transfer_manager.h"
#include "daemon/state_transfer_manager_pending.h"
#include "daemon/state_transfer_manager_transfer_in_state.h"
#include "daemon/state_transfer_manager_transfer_out_state.h"

using hyperdex::reconfigure_returncode;
using hyperdex::state_transfer_manager;
using hyperdex::transfer_id;

state_transfer_manager :: state_transfer_manager(daemon* d)
    : m_daemon(d)
    , m_transfers_in()
    , m_transfers_out()
    , m_kickstarter(std::tr1::bind(&state_transfer_manager::kickstarter, this))
    , m_block_kickstarter()
    , m_wakeup_kickstarter(&m_block_kickstarter)
    , m_wakeup_reconfigurer(&m_block_kickstarter)
    , m_need_kickstart(false)
    , m_shutdown(true)
    , m_need_pause(false)
    , m_paused(false)
{
}

state_transfer_manager :: ~state_transfer_manager() throw ()
{
    shutdown();
}

bool
state_transfer_manager :: setup()
{
    po6::threads::mutex::hold hold(&m_block_kickstarter);
    m_kickstarter.start();
    m_shutdown = false;
    return true;
}

void
state_transfer_manager :: teardown()
{
    m_transfers_in.clear();
    m_transfers_out.clear();
    shutdown();
}

void
state_transfer_manager :: pause()
{
    po6::threads::mutex::hold hold(&m_block_kickstarter);
    assert(!m_need_pause);
    m_need_pause = true;
}

void
state_transfer_manager :: unpause()
{
    po6::threads::mutex::hold hold(&m_block_kickstarter);
    assert(m_need_pause);
    m_wakeup_kickstarter.broadcast();
    m_need_pause = false;
    m_need_kickstart = true;
}

template <class S>
static void
setup_transfer_state(const char* desc,
                     hyperdex::datalayer* data,
                     hyperdex::datalayer::snapshot snap,
                     const std::vector<hyperdex::transfer> transfers,
                     std::vector<std::pair<transfer_id, e::intrusive_ptr<S> > >* transfer_states)
{
    std::vector<std::pair<transfer_id, e::intrusive_ptr<S> > > tmp;
    tmp.reserve(transfers.size());
    size_t t_idx = 0;
    size_t ts_idx = 0;

    while (t_idx < transfers.size() && ts_idx < transfer_states->size())
    {
        if (transfers[t_idx].id == (*transfer_states)[ts_idx].first)
        {
            tmp.push_back((*transfer_states)[ts_idx]);
            ++t_idx;
            ++ts_idx;
        }
        else if (transfers[t_idx].id < (*transfer_states)[ts_idx].first)
        {
            LOG(INFO) << "initiating " << desc << " " << transfers[t_idx];
            e::intrusive_ptr<S> new_state(new S(transfers[t_idx], data, snap));
            tmp.push_back(std::make_pair(transfers[t_idx].id, new_state));
            ++t_idx;
        }
        else if (transfers[t_idx].id > (*transfer_states)[ts_idx].first)
        {
            LOG(INFO) << "ending " << desc << " transfer " << (*transfer_states)[ts_idx].first;
            ++ts_idx;
        }
    }

    while (t_idx < transfers.size())
    {
        LOG(INFO) << "initiating " << desc << " " << transfers[t_idx];
        e::intrusive_ptr<S> new_state(new S(transfers[t_idx], data, snap));
        tmp.push_back(std::make_pair(transfers[t_idx].id, new_state));
        ++t_idx;
    }

    while (ts_idx < transfer_states->size())
    {
        LOG(INFO) << "ending " << desc << " " << (*transfer_states)[ts_idx].first;
        ++ts_idx;
    }

    tmp.swap(*transfer_states);
}

void
state_transfer_manager :: reconfigure(const configuration&,
                                      const configuration& new_config,
                                      const server_id&)
{
    {
        po6::threads::mutex::hold hold(&m_block_kickstarter);
        assert(m_need_pause);

        while (!m_paused)
        {
            m_wakeup_reconfigurer.wait();
        }
    }

    datalayer::snapshot snap = m_daemon->m_data.make_snapshot();

    // Setup transfers in
    std::vector<transfer> transfers_in;
    new_config.transfer_in_regions(m_daemon->m_us, &transfers_in);
    std::sort(transfers_in.begin(), transfers_in.end());
    setup_transfer_state("incoming", &m_daemon->m_data, snap, transfers_in, &m_transfers_in);

    // Setup transfers out
    std::vector<transfer> transfers_out;
    new_config.transfer_out_regions(m_daemon->m_us, &transfers_out);
    std::sort(transfers_out.begin(), transfers_out.end());
    setup_transfer_state("outgoing", &m_daemon->m_data, snap, transfers_out, &m_transfers_out);
}

void
state_transfer_manager :: xfer_op(const virtual_server_id& from,
                                  const transfer_id& xid,
                                  uint64_t seq_no,
                                  bool has_value,
                                  uint64_t version,
                                  std::auto_ptr<e::buffer> msg,
                                  const e::slice& key,
                                  const std::vector<e::slice>& value)
{
    std::vector<std::pair<transfer_id, e::intrusive_ptr<transfer_in_state> > >::iterator it;
    it = std::lower_bound(m_transfers_in.begin(),
                          m_transfers_in.end(),
                          std::make_pair(xid, e::intrusive_ptr<transfer_in_state>()));

    if (it == m_transfers_in.end() || it->first != xid)
    {
        LOG(INFO) << "dropping XFER_OP for transfer we don't know about";
        return;
    }

    transfer_in_state* tis = it->second.get();

    if (!tis)
    {
        return;
    }

    po6::threads::mutex::hold hold(&tis->mtx);

    if (tis->xfer.vsrc != from || tis->xfer.id != xid)
    {
        LOG(INFO) << "dropping XFER_OP that came from the wrong host";
        return;
    }

    if (seq_no < tis->upper_bound_acked)
    {
        return send_ack(tis->xfer, seq_no);
    }

    std::list<e::intrusive_ptr<pending> >::iterator where_to_put_it;

    for (where_to_put_it = tis->queued.begin();
            where_to_put_it != tis->queued.end(); ++where_to_put_it)
    {
        if ((*where_to_put_it)->seq_no == seq_no)
        {
            // silently drop it
            return;
        }

        if ((*where_to_put_it)->seq_no > seq_no)
        {
            break;
        }
    }

    e::intrusive_ptr<pending> op(new pending());
    op->seq_no = seq_no;
    op->has_value = has_value;
    op->version = version;
    op->key = key;
    op->value = value;
    op->msg = msg;
    tis->queued.insert(where_to_put_it, op);

    if (!tis->cleared_capture)
    {
        capture_id cid = m_daemon->m_config.capture_for(tis->xfer.rid);
        m_daemon->m_data.request_wipe(cid);
        return;
    }

    put_to_disk_and_send_acks(tis);
}

void
state_transfer_manager :: xfer_ack(const server_id& from,
                                   const virtual_server_id& to,
                                   const transfer_id& xid,
                                   uint64_t seq_no)
{
    std::vector<std::pair<transfer_id, e::intrusive_ptr<transfer_out_state> > >::iterator _it;
    _it = std::lower_bound(m_transfers_out.begin(),
                          m_transfers_out.end(),
                          std::make_pair(xid, e::intrusive_ptr<transfer_out_state>()));

    if (_it == m_transfers_out.end() || _it->first != xid)
    {
        LOG(INFO) << "dropping XFER_OP for transfer we don't know about";
        return;
    }

    transfer_out_state* tos = _it->second.get();

    if (!tos)
    {
        return;
    }

    po6::threads::mutex::hold hold(&tos->mtx);

    if (tos->xfer.dst != from || tos->xfer.vsrc != to || tos->xfer.id != xid)
    {
        LOG(INFO) << "dropping XFER_OP that came from the wrong host";
        return;
    }

    std::list<e::intrusive_ptr<pending> >::iterator it;

    for (it = tos->window.begin(); it != tos->window.end(); ++it)
    {
        if ((*it)->seq_no == seq_no)
        {
            break;
        }
    }

    if (it != tos->window.end())
    {
        (*it)->acked = true;

        if (tos->window_sz < 1024)
        {
            ++tos->window_sz;
        }
    }

    while (!tos->window.empty() && (*tos->window.begin())->acked)
    {
        tos->window.pop_front();
    }

    transfer_more_state(tos);
}

void
state_transfer_manager :: retransmit(const server_id& id)
{
    po6::threads::mutex::hold hold(&m_block_kickstarter);
    size_t idx = 0;

    while (true)
    {
        if (idx >= m_transfers_out.size())
        {
            break;
        }

        po6::threads::mutex::hold hold2(&m_transfers_out[idx].second->mtx);

        if (m_transfers_out[idx].second->xfer.dst == id)
        {
            retransmit(m_transfers_out[idx].second.get());
        }

        ++idx;
    }
}

void
state_transfer_manager :: report_wiped(const capture_id& cid)
{
    po6::threads::mutex::hold hold(&m_block_kickstarter);
    size_t idx = 0;

    while (true)
    {
        if (idx >= m_transfers_in.size())
        {
            break;
        }

        po6::threads::mutex::hold hold2(&m_transfers_in[idx].second->mtx);
        transfer_in_state* tis = m_transfers_in[idx].second.get();

        if (!tis->cleared_capture &&
            m_daemon->m_config.capture_for(tis->xfer.rid) == cid)
        {
            tis->cleared_capture = true;
            put_to_disk_and_send_acks(tis);
        }

        ++idx;
    }
}

void
state_transfer_manager :: transfer_more_state(transfer_out_state* tos)
{
    while (tos->window.size() < tos->window_sz)
    {
        if (tos->state == transfer_out_state::SNAPSHOT_TRANSFER)
        {
            if (tos->iter->valid())
            {
                e::intrusive_ptr<pending> op(new pending());
                op->seq_no = tos->next_seq_no;
                ++tos->next_seq_no;
                op->has_value = true;
                m_daemon->m_data.get_from_iterator(tos->xfer.rid, tos->iter.get(), &op->key, &op->value, &op->version, &op->ref);

                tos->window.push_back(op);
                send_object(tos->xfer, op.get());
                tos->iter->next();
            }
            else
            {
                tos->state = transfer_out_state::LOG_TRANSFER;
            }
        }
        else if (tos->state == transfer_out_state::LOG_TRANSFER)
        {
            e::intrusive_ptr<pending> op(new pending());
            datalayer::returncode rc;
            rc = m_daemon->m_data.get_transfer(tos->xfer.rid, tos->log_seq_no,
                                               &op->has_value,
                                               &op->key,
                                               &op->value,
                                               &op->version,
                                               &op->ref);
            bool done = false;

            switch (rc)
            {
                case datalayer::SUCCESS:
                    break;
                case datalayer::NOT_FOUND:
                    done = true;
                    break;
                case datalayer::BAD_ENCODING:
                case datalayer::CORRUPTION:
                case datalayer::IO_ERROR:
                case datalayer::LEVELDB_ERROR:
                    LOG(ERROR) << "state transfer caused error " << rc;
                    break;
                default:
                    LOG(ERROR) << "state transfer caused unknown error";
                    break;
            }

            if (done)
            {
                m_daemon->m_coord.transfer_go_live(tos->xfer.id);
                break;
            }

            op->seq_no = tos->next_seq_no;
            ++tos->next_seq_no;
            tos->window.push_back(op);
            send_object(tos->xfer, op.get());
            ++tos->log_seq_no;
        }
        else
        {
            abort();
        }
    }

    if (tos->window.empty() && m_daemon->m_config.is_transfer_live(tos->xfer.id))
    {
        m_daemon->m_coord.transfer_complete(tos->xfer.id);
    }
    else if (tos->window.empty())
    {
        m_daemon->m_coord.transfer_go_live(tos->xfer.id);
    }
}

void
state_transfer_manager :: retransmit(transfer_out_state* tos)
{
    for (std::list<e::intrusive_ptr<pending> >::iterator it = tos->window.begin();
            it != tos->window.end(); ++it)
    {
        send_object(tos->xfer, it->get());
    }
}

void
state_transfer_manager :: put_to_disk_and_send_acks(transfer_in_state* tis)
{
    if (!tis->cleared_capture)
    {
        return;
    }

    while (!tis->queued.empty() &&
           tis->queued.front()->seq_no == tis->upper_bound_acked)
    {
        e::intrusive_ptr<pending> op = tis->queued.front();

        // if we are still processing keys in sorted order, then delete
        // everything less than op->key
        if (tis->need_del &&
            (!tis->prev || tis->prev->key < op->key))
        {
            while (tis->iter->valid() && tis->iter->key() < op->key)
            {
                m_daemon->m_data.uncertain_del(tis->xfer.rid, tis->iter->key());
                tis->iter->next();
            }
        }
        else
        {
            // We've hit a point where we're now going out of order.  Save this
            // to avoid expensive comparison above, and delete everything
            // leftover in iter after this point.
            tis->need_del = false;

            while (tis->iter->valid())
            {
                datalayer::returncode rc = m_daemon->m_data.uncertain_del(tis->xfer.rid, tis->iter->key());
                tis->iter->next();

                switch (rc)
                {
                    case datalayer::SUCCESS:
                    case datalayer::NOT_FOUND:
                        break;
                    case datalayer::BAD_ENCODING:
                    case datalayer::CORRUPTION:
                    case datalayer::IO_ERROR:
                    case datalayer::LEVELDB_ERROR:
                        LOG(ERROR) << "state transfer caused error " << rc;
                        break;
                    default:
                        LOG(ERROR) << "state transfer caused unknown error";
                        break;
                }
            }
        }

        if (op->has_value)
        {
            datalayer::returncode rc = m_daemon->m_data.uncertain_put(tis->xfer.rid, op->key, op->value, op->version);

            switch (rc)
            {
                case datalayer::SUCCESS:
                    break;
                case datalayer::NOT_FOUND:
                case datalayer::BAD_ENCODING:
                case datalayer::CORRUPTION:
                case datalayer::IO_ERROR:
                case datalayer::LEVELDB_ERROR:
                    LOG(ERROR) << "state transfer caused error " << rc;
                    break;
                default:
                    LOG(ERROR) << "state transfer caused unknown error";
                    break;
            }
        }
        else
        {
            datalayer::returncode rc = m_daemon->m_data.uncertain_del(tis->xfer.rid, op->key);

            switch (rc)
            {
                case datalayer::SUCCESS:
                case datalayer::NOT_FOUND:
                    break;
                case datalayer::BAD_ENCODING:
                case datalayer::CORRUPTION:
                case datalayer::IO_ERROR:
                case datalayer::LEVELDB_ERROR:
                    LOG(ERROR) << "state transfer caused error " << rc;
                    break;
                default:
                    LOG(ERROR) << "state transfer caused unknown error";
                    break;
            }
        }

        send_ack(tis->xfer, op->seq_no);
        tis->upper_bound_acked = std::max(tis->upper_bound_acked, op->seq_no + 1);

        tis->prev = tis->queued.front();
        tis->queued.pop_front();
    }
}

void
state_transfer_manager :: send_object(const transfer& xfer,
                                      pending* op)
{
    uint8_t flags = (op->has_value ? 1 : 0);
    size_t sz = HYPERDEX_HEADER_SIZE_VV
              + sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint64_t)
              + sizeof(uint64_t)
              + sizeof(uint32_t) + op->key.size()
              + pack_size(op->value);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << flags << xfer.id.get() << op->seq_no
                                          << op->version << op->key << op->value;
    m_daemon->m_comm.send(xfer.vsrc, xfer.vdst, XFER_OP, msg);
}

void
state_transfer_manager :: send_ack(const transfer& xfer, uint64_t seq_no)
{
    uint8_t flags = 0;
    size_t sz = HYPERDEX_HEADER_SIZE_VV
              + sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << flags << xfer.id.get() << seq_no;
    m_daemon->m_comm.send(xfer.vdst, xfer.vsrc, XFER_ACK, msg);
}

void
state_transfer_manager :: kickstarter()
{
    LOG(INFO) << "state transfer thread started";
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

    while (true)
    {
        {
            po6::threads::mutex::hold hold(&m_block_kickstarter);

            while ((!m_need_kickstart && !m_shutdown) || m_need_pause)
            {
                m_paused = true;

                if (m_need_pause)
                {
                    m_wakeup_reconfigurer.signal();
                }

                m_wakeup_kickstarter.wait();
                m_paused = false;
            }

            if (m_shutdown)
            {
                break;
            }

            m_need_kickstart = false;
        }

        size_t idx = 0;

        while (true)
        {
            po6::threads::mutex::hold hold(&m_block_kickstarter);

            if (idx >= m_transfers_out.size())
            {
                break;
            }

            po6::threads::mutex::hold hold2(&m_transfers_out[idx].second->mtx);
            transfer_more_state(m_transfers_out[idx].second.get());
            ++idx;
        }

        idx = 0;

        while (true)
        {
            po6::threads::mutex::hold hold(&m_block_kickstarter);

            if (idx >= m_transfers_in.size())
            {
                break;
            }

            po6::threads::mutex::hold hold2(&m_transfers_in[idx].second->mtx);
            put_to_disk_and_send_acks(m_transfers_in[idx].second.get());
            ++idx;
        }
    }

    LOG(INFO) << "state transfer thread shutting down";
}

void
state_transfer_manager :: shutdown()
{
    bool is_shutdown;

    {
        po6::threads::mutex::hold hold(&m_block_kickstarter);
        m_wakeup_kickstarter.broadcast();
        is_shutdown = m_shutdown;
        m_shutdown = true;
    }

    if (!is_shutdown)
    {
        m_kickstarter.join();
    }
}
