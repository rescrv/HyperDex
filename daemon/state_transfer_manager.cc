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

using po6::threads::make_thread_wrapper;
using hyperdex::reconfigure_returncode;
using hyperdex::state_transfer_manager;
using hyperdex::transfer_id;

class state_transfer_manager::background_thread : public ::hyperdex::background_thread
{
    public:
        background_thread(state_transfer_manager* stm);
        ~background_thread() throw ();

    public:
        virtual const char* thread_name();
        virtual bool have_work();
        virtual void copy_work();
        virtual void do_work();

    public:
        void kick();

    private:
        background_thread(const background_thread&);
        background_thread& operator = (const background_thread&);

    private:
        state_transfer_manager* m_stm;
        bool m_need_kickstart;
};

state_transfer_manager :: state_transfer_manager(daemon* d)
    : m_daemon(d)
    , m_transfers_in()
    , m_transfers_out()
    , m_background_thread(new background_thread(this))
{
}

state_transfer_manager :: ~state_transfer_manager() throw ()
{
    m_background_thread->shutdown();
}

bool
state_transfer_manager :: setup()
{
    m_background_thread->start();
    return true;
}

void
state_transfer_manager :: teardown()
{
    m_background_thread->shutdown();
    m_transfers_in.clear();
    m_transfers_out.clear();
}

void
state_transfer_manager :: pause()
{
    m_background_thread->initiate_pause();
}

void
state_transfer_manager :: unpause()
{
    m_background_thread->kick();
    m_background_thread->unpause();
}

template <class S>
static void
setup_transfer_state(const char* desc,
                     const std::vector<hyperdex::transfer> transfers,
                     std::vector<e::intrusive_ptr<S> >* transfer_states)
{
    std::vector<e::intrusive_ptr<S> > tmp;
    tmp.reserve(transfers.size());
    size_t t_idx = 0;
    size_t ts_idx = 0;

    while (t_idx < transfers.size() && ts_idx < transfer_states->size())
    {
        if (transfers[t_idx].id == (*transfer_states)[ts_idx]->xfer.id)
        {
            tmp.push_back((*transfer_states)[ts_idx]);
            ++t_idx;
            ++ts_idx;
        }
        else if (transfers[t_idx].id < (*transfer_states)[ts_idx]->xfer.id)
        {
            LOG(INFO) << "initiating " << desc << " " << transfers[t_idx];
            e::intrusive_ptr<S> new_state(new S(transfers[t_idx]));
            tmp.push_back(new_state);
            ++t_idx;
        }
        else if (transfers[t_idx].id > (*transfer_states)[ts_idx]->xfer.id)
        {
            LOG(INFO) << "ending " << desc << " " << (*transfer_states)[ts_idx]->xfer.id;
            ++ts_idx;
        }
    }

    while (t_idx < transfers.size())
    {
        LOG(INFO) << "initiating " << desc << " " << transfers[t_idx];
        e::intrusive_ptr<S> new_state(new S(transfers[t_idx]));
        tmp.push_back(new_state);
        ++t_idx;
    }

    while (ts_idx < transfer_states->size())
    {
        LOG(INFO) << "ending " << desc << " " << (*transfer_states)[ts_idx]->xfer.id;
        ++ts_idx;
    }

    tmp.swap(*transfer_states);
}

void
state_transfer_manager :: reconfigure(const configuration&,
                                      const configuration& new_config,
                                      const server_id&)
{
    m_background_thread->wait_until_paused();

    // Setup transfers in
    std::vector<transfer> transfers_in;
    new_config.transfers_in(m_daemon->m_us, &transfers_in);
    std::sort(transfers_in.begin(), transfers_in.end());
    setup_transfer_state("incoming", transfers_in, &m_transfers_in);

    // Setup transfers out
    std::vector<transfer> transfers_out;
    new_config.transfers_out(m_daemon->m_us, &transfers_out);
    std::sort(transfers_out.begin(), transfers_out.end());
    setup_transfer_state("outgoing", transfers_out, &m_transfers_out);
}

void
state_transfer_manager :: debug_dump()
{
    LOG(INFO) << "state transfer manager ========================================================";
    pause();
    m_background_thread->wait_until_paused();
    LOG(INFO) << "transfer-in states ============================================================";

    for (size_t i = 0; i < m_transfers_in.size(); ++i)
    {
        m_transfers_in[i]->debug_dump();
    }

    LOG(INFO) << "transfer-out states ===========================================================";

    for (size_t i = 0; i < m_transfers_out.size(); ++i)
    {
        m_transfers_out[i]->debug_dump();
    }

    unpause();
}

void
state_transfer_manager :: handshake_syn(const virtual_server_id& from,
                                        const transfer_id& xid)
{
    transfer_in_state* tis = get_tis(xid);

    if (!tis)
    {
        LOG(INFO) << "dropping XFER_HS for " << xid << " which we don't know about";
        return;
    }

    po6::threads::mutex::hold hold(&tis->mtx);

    if (tis->xfer.vsrc != from || tis->xfer.id != xid)
    {
        LOG(INFO) << "dropping XFER_HS that came from the wrong host";
        return;
    }

    uint64_t timestamp = 0;
    m_daemon->m_data.largest_checkpoint_for(tis->xfer.rid, &timestamp);
    send_handshake_synack(tis->xfer, timestamp);
    LOG(INFO) << "received handshake_syn for " << xid;
}

void
state_transfer_manager :: handshake_synack(const server_id& from,
                                           const virtual_server_id& to,
                                           const transfer_id& xid,
                                           uint64_t timestamp)
{
    transfer_out_state* tos = get_tos(xid);

    if (!tos)
    {
        LOG(INFO) << "dropping XFER_HSA for " << xid << " which we don't know about";
        return;
    }

    po6::threads::mutex::hold hold(&tos->mtx);

    if (tos->xfer.dst != from || tos->xfer.vsrc != to || tos->xfer.id != xid)
    {
        LOG(INFO) << "dropping XFER_HSA that came from the wrong host";
        return;
    }

    bool wipe = false;
    std::auto_ptr<datalayer::replay_iterator> iter;
    iter.reset(m_daemon->m_data.replay_region_from_checkpoint(tos->xfer.rid, timestamp, &wipe));
    tos->handshake_syn = true;
    tos->wipe = wipe;
    tos->iter = iter;
    send_handshake_ack(tos->xfer, tos->wipe);
    transfer_more_state(tos);
    LOG(INFO) << "received handshake_synack for " << xid << " @ " << timestamp;
}

void
state_transfer_manager :: handshake_ack(const virtual_server_id& from,
                                        const transfer_id& xid,
                                        bool wipe)
{
    transfer_in_state* tis = get_tis(xid);

    if (!tis)
    {
        LOG(INFO) << "dropping XFER_HA for " << xid << " which we don't know about";
        return;
    }

    po6::threads::mutex::hold hold(&tis->mtx);

    if (tis->xfer.vsrc != from || tis->xfer.id != xid)
    {
        LOG(INFO) << "dropping XFER_HA that came from the wrong host";
        return;
    }

    if (!tis->handshake_complete)
    {
        tis->handshake_complete = true;
        tis->wipe = wipe;
        LOG(INFO) << "received handshake_ack for " << xid << (wipe ? " (and we must wipe our previous state)" : "");
    }

    put_to_disk_and_send_acks(tis);
}

void
state_transfer_manager :: handshake_wiped(const server_id& from,
                                          const virtual_server_id& to,
                                          const transfer_id& xid)
{
    transfer_out_state* tos = get_tos(xid);

    if (!tos)
    {
        LOG(INFO) << "dropping XFER_HW for " << xid << " which we don't know about";
        return;
    }

    po6::threads::mutex::hold hold(&tos->mtx);

    if (tos->xfer.dst != from || tos->xfer.vsrc != to || tos->xfer.id != xid)
    {
        LOG(INFO) << "dropping XFER_HW that came from the wrong host";
        return;
    }

    tos->handshake_ack = true;
    transfer_more_state(tos);
    LOG(INFO) << "received handshake_wiped for " << xid;
}

void
state_transfer_manager :: report_wiped(const transfer_id& xid)
{
    transfer_in_state* tis = get_tis(xid);

    if (!tis)
    {
        return;
    }

    m_daemon->m_data.inhibit_wiping();

    if (!m_daemon->m_data.region_will_be_wiped(tis->xfer.rid))
    {
        po6::threads::mutex::hold hold(&tis->mtx);
        tis->wiped = true;
        put_to_disk_and_send_acks(tis);
        LOG(INFO) << "we've wiped our state for " << xid;
    }

    m_daemon->m_data.permit_wiping();
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
    transfer_in_state* tis = get_tis(xid);

    if (!tis)
    {
        LOG(INFO) << "dropping XFER_OP for " << xid << " which we don't know about";
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
    put_to_disk_and_send_acks(tis);
}

void
state_transfer_manager :: xfer_ack(const server_id& from,
                                   const virtual_server_id& to,
                                   const transfer_id& xid,
                                   uint64_t seq_no)
{
    transfer_out_state* tos = get_tos(xid);

    if (!tos)
    {
        LOG(INFO) << "dropping XFER_ACK for " << xid << " which we don't know about";
        return;
    }

    po6::threads::mutex::hold hold(&tos->mtx);

    if (tos->xfer.dst != from || tos->xfer.vsrc != to || tos->xfer.id != xid)
    {
        LOG(INFO) << "dropping XFER_ACK that came from the wrong host";
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
        tos->handshake_ack = true;

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

state_transfer_manager::transfer_in_state*
state_transfer_manager :: get_tis(const transfer_id& xid)
{
    for (size_t i = 0; i < m_transfers_in.size(); ++i)
    {
        if (m_transfers_in[i]->xfer.id == xid)
        {
            return m_transfers_in[i].get();
        }
    }

    return NULL;
}

state_transfer_manager::transfer_out_state*
state_transfer_manager :: get_tos(const transfer_id& xid)
{
    for (size_t i = 0; i < m_transfers_out.size(); ++i)
    {
        if (m_transfers_out[i]->xfer.id == xid)
        {
            return m_transfers_out[i].get();
        }
    }

    return NULL;
}

void
state_transfer_manager :: transfer_more_state(transfer_out_state* tos)
{
    if (!tos->handshake_syn)
    {
        send_handshake_syn(tos->xfer);
        return;
    }

    if (!tos->handshake_ack)
    {
        send_handshake_ack(tos->xfer, tos->wipe);
    }

    assert(tos->iter.get());

    while (tos->window.size() < tos->window_sz && tos->iter->valid())
    {
        e::intrusive_ptr<pending> op(new pending());
        op->seq_no = tos->next_seq_no;
        ++tos->next_seq_no;
        op->kref.assign(reinterpret_cast<const char*>(tos->iter->key().data()), tos->iter->key().size());
        op->key = e::slice(op->kref);

        if (tos->iter->has_value())
        {
            op->has_value = true;

            if (tos->iter->unpack_value(&op->value, &op->version, &op->vref) != datalayer::SUCCESS)
            {
                LOG(ERROR) << "error doing state transfer";
                break;
            }
        }
        else
        {
            op->has_value = false;
            op->version = 0;
        }

        tos->window.push_back(op);
        send_object(tos->xfer, op.get());
        tos->iter->next();
    }

    if (!tos->handshake_ack)
    {
        // pass!  we need the other end to give us some sign that it's ready,
        // otherwise we cannot consider moving forward, even if we're ready.
    }
    else if (tos->window.empty() && m_daemon->m_config.is_transfer_live(tos->xfer.id))
    {
        m_daemon->m_coord->transfer_complete(tos->xfer.id);
    }
    else if (tos->window.empty())
    {
        m_daemon->m_coord->transfer_go_live(tos->xfer.id);
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
    if (!tis->handshake_complete)
    {
        return;
    }

    if (tis->wipe && !tis->wiped)
    {
        m_daemon->m_data.request_wipe(tis->xfer.id, tis->xfer.rid);
        return;
    }

    if (tis->queued.empty())
    {
        send_handshake_wiped(tis->xfer);
    }

    while (!tis->queued.empty() &&
           tis->queued.front()->seq_no == tis->upper_bound_acked)
    {
        e::intrusive_ptr<pending> op = tis->queued.front();

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
        tis->queued.pop_front();
    }
}

void
state_transfer_manager :: send_handshake_syn(const transfer& xfer)
{
    size_t sz = HYPERDEX_HEADER_SIZE_VV
              + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << xfer.id;
    m_daemon->m_comm.send_exact(xfer.vsrc, xfer.vdst, XFER_HS, msg);
}

void
state_transfer_manager :: send_handshake_synack(const transfer& xfer, uint64_t timestamp)
{
    size_t sz = HYPERDEX_HEADER_SIZE_VV
              + sizeof(uint64_t)
              + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << xfer.id << timestamp;
    m_daemon->m_comm.send_exact(xfer.vdst, xfer.vsrc, XFER_HSA, msg);
}

void
state_transfer_manager :: send_handshake_ack(const transfer& xfer, bool wipe)
{
    uint8_t flags = wipe ? 0x1 : 0;
    size_t sz = HYPERDEX_HEADER_SIZE_VV
              + sizeof(uint64_t)
              + sizeof(uint8_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << xfer.id << flags;
    m_daemon->m_comm.send_exact(xfer.vsrc, xfer.vdst, XFER_HA, msg);
}

void
state_transfer_manager :: send_handshake_wiped(const transfer& xfer)
{
    size_t sz = HYPERDEX_HEADER_SIZE_VV
              + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << xfer.id;
    m_daemon->m_comm.send_exact(xfer.vdst, xfer.vsrc, XFER_HW, msg);
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
    m_daemon->m_comm.send_exact(xfer.vsrc, xfer.vdst, XFER_OP, msg);
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
    m_daemon->m_comm.send_exact(xfer.vdst, xfer.vsrc, XFER_ACK, msg);
}

state_transfer_manager :: background_thread :: background_thread(state_transfer_manager* stm)
    : hyperdex::background_thread(stm->m_daemon)
    , m_stm(stm)
    , m_need_kickstart(false)
{
}

state_transfer_manager :: background_thread :: ~background_thread() throw ()
{
}

const char*
state_transfer_manager :: background_thread :: thread_name()
{
    return "state transfer";
}

bool
state_transfer_manager :: background_thread :: have_work()
{
    return m_need_kickstart;
}

void
state_transfer_manager :: background_thread :: copy_work()
{
    m_need_kickstart = false;
}

void
state_transfer_manager :: background_thread :: do_work()
{
    for (size_t idx = 0; idx < m_stm->m_transfers_out.size(); ++idx)
    {
        po6::threads::mutex::hold hold2(&m_stm->m_transfers_out[idx]->mtx);
        m_stm->retransmit(m_stm->m_transfers_out[idx].get());
        m_stm->transfer_more_state(m_stm->m_transfers_out[idx].get());
        ++idx;
    }

    m_stm->m_daemon->m_comm.wake_one();
}

void
state_transfer_manager :: background_thread :: kick()
{
    this->lock();
    m_need_kickstart = true;
    this->wakeup();
    this->unlock();
}
