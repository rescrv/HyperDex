// Copyright (c) 2015, Cornell University
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
#include <string.h>

// POSIX
#include <signal.h>

// STL
#include <algorithm>

// Google Log
#include <glog/logging.h>
#include <glog/raw_logging.h>

// e
#include <e/endian.h>

// HyperDex
#include "client/constants.h"
#include "common/coordinator_returncode.h"
#include "common/serialization.h"
#include <common/hyperspace.h>
#include "daemon/daemon.h"
#include "daemon/datalayer_iterator.h"
#include "daemon/wan_manager.h"
#include "daemon/wan_manager_pending.h"
#include "daemon/wan_manager_transfer_in_state.h"
#include "daemon/wan_manager_transfer_out_state.h"

using po6::threads::make_thread_wrapper;
using hyperdex::configuration;
using hyperdex::wan_manager;
using hyperdex::transfer_id;
using hyperdex::reconfigure_returncode;

class wan_manager::background_thread : public ::hyperdex::background_thread
{
    public:
        background_thread(wan_manager* wm);
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
        wan_manager* m_wm;
        bool m_need_kickstart;
};

wan_manager :: wan_manager(daemon* d)
    : m_daemon(d)
    , m_poller(make_thread_wrapper(&wan_manager::background_maintenance, this))
    , m_coord()
    , m_mtx()
    , m_cond(&m_mtx)
    , m_is_backup(false)
    , m_poller_started(false)
    , m_teardown(false)
    , m_manual_teardown(false)
    , m_deferred()
    , m_locked(false)
    , m_kill(false)
    , m_to_kill()
    , m_waiting(0)
    , m_sleep(1000ULL * 1000ULL)
    , m_online_id(-1)
    , m_shutdown_requested(false)
    , m_transfer_vids()
    , m_xid(66)
    , m_transfers_in()
    , m_transfers_out()
    , m_background_thread(new background_thread(this))
    , m_config()
    , m_busybee_mapper(&m_config)
    , m_busybee()
    , m_msg_thread(make_thread_wrapper(&wan_manager::loop, this))
    , m_link_thread(make_thread_wrapper(&wan_manager::run, this))
    , m_paused(false)
    , m_protect_pause()
    , m_can_pause(&m_protect_pause)
    , m_has_config(false)
    , m_busybee_running(false)
{
}

wan_manager :: ~wan_manager() throw ()
{
    if (m_manual_teardown) {
        m_teardown = true;
        m_busybee->shutdown();
        m_busybee_running = false;
        m_msg_thread.join();
        m_background_thread->shutdown();
        m_transfers_in.clear();
        m_transfers_out.clear();
        m_link_thread.join();
        if (m_poller_started)
        {
            m_poller.join();
        }
    }
}

bool
wan_manager :: setup(const char* host, int64_t port)
{
    // XXX please fix this, this is really hacky
    po6::net::location bind_to(host, port);
    m_busybee.reset(new busybee_mta(&m_daemon->m_gc, &m_busybee_mapper, bind_to,
                m_daemon->m_us.get(), 1));
    m_busybee_running = true;
    m_link_thread.start();
    m_background_thread->start();
    m_msg_thread.start();
    return true;
}

void
wan_manager :: teardown()
{
    enter_critical_section();
    m_teardown = true;
    exit_critical_section();
    m_busybee->shutdown();
    m_busybee_running = false;
    m_msg_thread.join();
    m_background_thread->shutdown();
    m_transfers_in.clear();
    m_transfers_out.clear();
    m_link_thread.join();
    if (m_poller_started)
    {
        m_poller.join();
    }
}

void
wan_manager :: wake_one()
{
    m_busybee->wake_one();
}

void
wan_manager :: set_coordinator_address(const char* host, uint16_t port)
{
    assert(!m_coord.get());
    m_coord.reset(new coordinator_link(host, port));
}

void
wan_manager :: set_is_backup(bool isbackup)
{
    m_is_backup = isbackup;
}

void
wan_manager :: pause()
{
    if (m_teardown) {
        LOG(INFO) << "should teardown, not pausing...";
        return;
    }

    po6::threads::mutex::hold hold(&m_protect_pause);

    while (m_paused) {
        if (m_teardown) {
            LOG(INFO) << "should teardown, not pausing...";
            return;
        }
        m_can_pause.wait();
    }

    if (!m_teardown) {
        m_paused = true;
        m_background_thread->initiate_pause();
        m_background_thread->wait_until_paused();
        if (m_busybee_running) {
            m_busybee->pause();
        }
    }
}

void
wan_manager :: unpause()
{
    po6::threads::mutex::hold hold(&m_protect_pause);

    m_background_thread->unpause();
    m_busybee->unpause();

    assert(m_paused);
    m_paused = false;
    m_can_pause.signal();
}

void
wan_manager :: set_teardown()
{
    enter_critical_section();
    m_teardown = true;
    m_manual_teardown = true;
    exit_critical_section();
}

void
wan_manager :: kick()
{
    m_background_thread->kick();
}

void
wan_manager :: reconfigure(configuration *config)
{
    // XXX busybee deliver early messages
    m_config = *config;
    std::vector<space> their_spaces = m_config.get_spaces();
    std::vector<space> our_spaces = m_daemon->m_config.get_spaces();
    std::vector<space>::iterator their;
    std::vector<space>::iterator our;

    for (their = their_spaces.begin(); their != their_spaces.end(); ++their) {
        for (our = our_spaces.begin(); our != our_spaces.end(); ++our) {
            if (*our == *their) {
                continue;
            }
        }
        m_daemon->m_coord.add_space(*their);
    }
}

void
wan_manager :: rm_all_spaces()
{
    std::vector<space> spaces = m_daemon->m_config.get_spaces();
    std::vector<space>::iterator it;

    for (it = spaces.begin(); it != spaces.end(); ++it) {
        m_daemon->m_coord.rm_space(*it);
    }
}

wan_manager::transfer_in_state*
wan_manager :: get_tis(const transfer_id& xid)
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

wan_manager::transfer_out_state*
wan_manager :: get_tos(const transfer_id& xid)
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
wan_manager :: send_handshake_syn(const transfer& xfer)
{
    uint64_t timestamp(0);
    size_t sz = HYPERDEX_HEADER_SIZE_SV
              + sizeof(uint64_t)
              + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_SV) << xfer.id.get() << timestamp;
    // LOG(INFO) << "xfer id = " << xfer.id.get() << " timestamp = " << timestamp;
    send(xfer.vdst, WAN_HS, msg);
}

void
wan_manager :: send_ask_for_more(const transfer& xfer)
{
    size_t sz = HYPERDEX_HEADER_SIZE_SV
              + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_SV) << xfer.id.get();
    // LOG(INFO) << "xfer id = " << xfer.id.get();
    send(xfer.vdst, WAN_MORE, msg);
}

void
wan_manager :: send_object(const transfer& xfer,
                                      pending* op)
{
    uint8_t flags = (op->has_value ? 1 : 0);
    // LOG(INFO) << "op has value = " << op->has_value;
    // LOG(INFO) << "flags= " << flags << " xfer id= " << xfer.id.get() << " seq_no= " << op->seq_no << " version= " << op->version;
    size_t sz = HYPERDEX_HEADER_SIZE_SV
              + sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint64_t)
              + sizeof(uint64_t)
              + sizeof(uint64_t)
              + sizeof(uint32_t) + op->key.size()
              + pack_size(op->value);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_SV) << flags << xfer.id.get() << xfer.rid.get() << op->seq_no
                                          << op->version << op->key << op->value;
    m_daemon->m_comm.send_wan(xfer.vsrc, xfer.dst, WAN_XFER, msg);
}

void
wan_manager :: send_ack(const transfer& xfer, uint64_t seq_no)
{
    uint8_t flags = 0;
    size_t sz = HYPERDEX_HEADER_SIZE_SV
              + sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_SV) << flags << xfer.id.get() << seq_no;
    // send_exact(xfer.vdst, xfer.vsrc, XFER_ACK, msg);
}

wan_manager :: background_thread :: background_thread(wan_manager* wm)
    : hyperdex::background_thread(wm->m_daemon)
    , m_wm(wm)
    , m_need_kickstart(false)
{
}

wan_manager :: background_thread :: ~background_thread() throw ()
{
}

const char*
wan_manager :: background_thread :: thread_name()
{
    return "wan manager";
}

bool
wan_manager :: background_thread :: have_work()
{
    return m_need_kickstart;
}

void
wan_manager :: background_thread :: copy_work()
{
    m_need_kickstart = false;
}

void
wan_manager :: background_thread :: do_work()
{
    if (m_wm->m_is_backup) {
        if (m_wm->m_has_config && m_wm->m_daemon->m_config.version() > 0) {
            std::vector<hyperdex::space> overlap = m_wm->config_space_overlap(m_wm->m_config,
                    m_wm->m_daemon->m_config);
            m_wm->setup_transfer_state(overlap);
            size_t i;
            for (i = 0; i < m_wm->m_transfers_in.size(); ++i) {
                po6::threads::mutex::hold hold2(&m_wm->m_transfers_in[i]->mtx);
                m_wm->give_me_more_state(m_wm->m_transfers_in[i].get());
            }
            m_wm->wake_one();
        }
    }
}

void
wan_manager :: background_thread :: kick()
{
    this->lock();
    m_need_kickstart = true;
    this->wakeup();
    this->unlock();
}

bool
wan_manager :: maintain_link()
{
    if (m_teardown) {
        return false;
    }

    enter_critical_section_killable();
    bool exit_status = false;

    if (!m_poller_started)
    {
        m_poller.start();
        m_poller_started = true;
    }

    while (!m_teardown) {
        int64_t id = -1;
        replicant_returncode status = REPLICANT_GARBAGE;

        if (!m_deferred.empty()) {
            id = m_deferred.front().first;
            status = m_deferred.front().second;
            m_deferred.pop();
        } else {
            id = m_coord->loop(1000, &status);
        }

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
            e::error err = m_coord->error();
            LOG(ERROR) << "coordinator disconnected: backing off before retrying";
            LOG(ERROR) << "details: " << err.msg() << " @ " << err.loc();
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

        if (id == INT64_MAX) {
            exit_status = m_coord->config()->exists(m_daemon->m_us);
            break;
        } else {
            e::error err = m_coord->error();
            LOG(ERROR) << "coordinator error: " << err.msg() << " @" << err.loc();
            do_sleep();
            exit_status = false;
            break;
        }
    }

    exit_critical_section_killable();
    return exit_status;
}

void
wan_manager :: copy_config(configuration* config)
{
    enter_critical_section();
    *config = *m_coord->config();
    exit_critical_section();
}

configuration
wan_manager :: get_config()
{
    return m_config;
}

void
wan_manager :: loop()
{
    sigset_t ss;

    LOG(INFO) << "network thread started on for wan manager.";

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
    m_daemon->m_gc.register_thread(&ts);

    server_id from;
    virtual_server_id vfrom;
    virtual_server_id vto;
    network_msgtype type;
    std::auto_ptr<e::buffer> msg;
    e::unpacker up;

    while (recv(&from, &vfrom, &vto, &type, &msg, &up))
    {
        assert(from != server_id());
        assert(vto != virtual_server_id());

        switch (type)
        {
            case WAN_HS:
                // handle_handshake(from, vfrom, vto, msg, up);
                break;
            case WAN_XFER:
                recv_data(from, vfrom, vto, msg, up);
                break;
            case WAN_MORE:
                // send_more_data(from, vfrom, vto, msg, up);
                break;
            case WAN_ACK:
                // handle_ack(from, vfrom, vto, msg, up);
                break;
            default:
                LOG(INFO) << "received " << type << " message which wan network thread does not process";
                break;
        }

        m_daemon->m_gc.quiescent_state(&ts);
    }

    m_daemon->m_gc.deregister_thread(&ts);
    LOG(INFO) << "network thread shutting down";
}

std::vector<hyperdex::space>
wan_manager :: config_space_overlap(configuration primary, configuration backup)
{
    std::vector<hyperdex::space> pspaces = primary.get_spaces();
    std::vector<hyperdex::space> bspaces = backup.get_spaces();
    std::vector<hyperdex::space> overlap;
    for (std::vector<hyperdex::space>::iterator pspace = pspaces.begin();
            pspace != pspaces.end(); ++pspace) {
        for (std::vector<hyperdex::space>::iterator bspace = bspaces.begin();
                bspace != bspaces.end(); ++bspace) {
            if (*pspace == *bspace) {
                overlap.push_back(*pspace);
            }
        }
    }
    return overlap; // primary spaces
}

void
wan_manager :: setup_transfer_state(std::vector<hyperdex::space> overlap)
{
    int i, j, k;
    for (i = 0; i < overlap.size(); i++) {
        hyperdex::space currspace = overlap[i];
        hyperdex::subspace currsub = currspace.subspaces[0];
        for (k = 0; k < currsub.regions.size(); k++) {
            hyperdex::region reg = currsub.regions[k];
            virtual_server_id vid = m_config.head_of_region(reg.id);
            const bool is_in = m_transfer_vids.find(vid) != m_transfer_vids.end();
            if (!is_in) {
                // XXX make wan_xfer, smaller # fields
                transfer xfer;
                xfer.vdst = vid;
                xfer.id = transfer_id(m_xid++);
                transfer_in_state *tis = new transfer_in_state(xfer);
                m_transfer_vids.insert(xfer.vdst);
                m_transfers_in.push_back(tis);
            }
        }
    }
}

void
wan_manager :: give_me_more_state(transfer_in_state* tis)
{
    if (!tis->handshake_complete) {
        // XXX robustness
        send_handshake_syn(tis->xfer);
        tis->handshake_complete = true;
    } else {
        send_ask_for_more(tis->xfer);
    }
}

void
wan_manager :: handle_handshake(const server_id& from,
                                const virtual_server_id&,
                                const virtual_server_id& vto,
                                std::auto_ptr<e::buffer> msg,
                                e::unpacker up)
{
    uint64_t xid;
    uint64_t timestamp;

    if((up >> xid >> timestamp).error()) {
        LOG(WARNING) << "unpack of WAN_HS failed; here's some hex:  " << msg->hex();
        return;
    }

    // LOG(INFO) << "xfer id = " << xid << " timestamp = " << timestamp;
    // XXX make wan_xfer, smaller # fields
    transfer xfer;
    xfer.id = transfer_id(xid);
    xfer.dst = from;
    xfer.vsrc = vto;

    transfer_out_state *tos = new transfer_out_state(xfer);
    po6::threads::mutex::hold hold(&tos->mtx);
    bool wipe = false;
    region_id curr_rid = m_daemon->m_config.get_region_id(vto);
    tos->xfer.rid = curr_rid;

    std::auto_ptr<datalayer::replay_iterator> iter;
    iter.reset(m_daemon->m_data.replay_region_from_checkpoint(curr_rid, timestamp, &wipe));
    tos->wipe = wipe;
    tos->iter = iter;
    tos->window_sz = 1024; // XXX: magic number
    m_transfers_out.push_back(tos);
    transfer_more_state(tos);
}

void
wan_manager :: transfer_more_state(transfer_out_state* tos)
{
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
            // LOG(INFO) << "iter has value in transfer more state";
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
        // LOG(INFO) << "transferring objects to backup";
        tos->iter->next();
    }
}

void
wan_manager :: recv_data(const server_id& from,
                              const virtual_server_id& vfrom,
                              const virtual_server_id& vto,
                              std::auto_ptr<e::buffer> msg,
                              e::unpacker up)
{
    uint8_t flags;
    uint64_t xid;
    uint64_t rid;
    uint64_t seq_no;
    uint64_t version;
    e::slice key;
    std::vector<e::slice> value;

    if ((up >> flags >> xid >> rid >> seq_no >> version >> key >> value).error())
    {
        LOG(WARNING) << "unpack of WAN_XFER failed; here's some hex:  " << msg->hex();
        return;
    }

    bool has_value = flags & 1;
    // LOG(INFO) << "flags = " << flags << " xid= " << xid << " rid= " << rid << " seq_no= " << seq_no << " version= " << version;
    wan_xfer(transfer_id(xid), seq_no, has_value, version, msg, key, value, region_id(rid));
}

void
wan_manager :: wan_xfer(const transfer_id& xid,
                        uint64_t seq_no,
                        bool has_value,
                        uint64_t version,
                        std::auto_ptr<e::buffer> msg,
                        const e::slice& key,
                        const std::vector<e::slice>& value,
                        const region_id rid)
{
    transfer_in_state* tis = get_tis(xid);

    if (!tis)
    {
        LOG(INFO) << "dropping WAN_XFER for " << xid << " which we don't know about";
        return;
    }

    po6::threads::mutex::hold hold(&tis->mtx);

    tis->xfer.rid = rid;

    // if (tis->xfer.vsrc != from || tis->xfer.id != xid)
    // {
    //     LOG(INFO) << "dropping XFER_OP that came from the wrong host";
    //     return;
    // }

    if (seq_no < tis->upper_bound_acked)
    {
        LOG(INFO) << "send ack";
        // return send_ack(tis->xfer, seq_no);
    }

    std::list<e::intrusive_ptr<pending> >::iterator where_to_put_it;

    for (where_to_put_it = tis->queued.begin();
            where_to_put_it != tis->queued.end(); ++where_to_put_it)
    {
        if ((*where_to_put_it)->seq_no == seq_no)
        {
            // silently drop it
            LOG(INFO) << "we have received op already";
            return;
        }

        if ((*where_to_put_it)->seq_no > seq_no)
        {
            LOG(INFO) << "we are past where we acked to";
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
    // LOG(INFO) << "iterators queued up for pending op";

    make_keychanges(tis);
    give_me_more_state(tis);
}

void
wan_manager :: make_keychanges(transfer_in_state* tis)
{
    for (std::list<e::intrusive_ptr<pending> >::iterator it = tis->queued.begin();
            it != tis->queued.end();
            ++it) {

        std::auto_ptr<e::buffer> msg;
        e::intrusive_ptr<pending> op = *it;
        size_t header_sz = HYPERDEX_CLIENT_HEADER_SIZE_REQ
                         + pack_size(op->key);

        std::vector<attribute_check> checks; // don't do anything with these
        std::vector<funcall> funcs;
        const schema& sc(*m_config.get_schema(tis->xfer.rid));

        for (size_t i = 0; i < op->value.size(); i++) {
            attribute attr = sc.attrs[i+1];  // recall that key is first attr in schema

            funcall o;
            o.attr = sc.lookup_attr(attr.name);
            o.name = FUNC_SET;
            o.arg1 = op->value[i];
            o.arg1_datatype = attr.type;
            funcs.push_back(o);
        }

        std::stable_sort(funcs.begin(), funcs.end());
        size_t sz = header_sz
            + sizeof(uint8_t)
            + pack_size(checks)
            + pack_size(funcs);
        msg.reset(e::buffer::create(sz));
        uint8_t flags = 0 | 0 | 128 | 0; // XXX magic
        msg->pack_at(header_sz) << flags << checks << funcs;
        msg->pack_at(HYPERDEX_CLIENT_HEADER_SIZE_REQ) << op->key;

        std::auto_ptr<key_change> kc(new key_change());
        kc->key = op->key;
        kc->erase = false;
        kc->fail_if_not_found = false;
        kc->fail_if_found = false;
        kc->checks = checks;
        kc->funcs = funcs;
        uint64_t nonce = tis->xfer.id.get(); // XXX real nonce

        std::vector<region_id> pt_leaders;
        m_daemon->m_config.point_leaders(m_daemon->m_us, &pt_leaders);

        for (int i = 0; i < pt_leaders.size(); ++i) {
            region_id ri = pt_leaders[i];
            virtual_server_id pt_lead = m_daemon->m_config.point_leader(ri, kc->key);
            if (pt_lead != virtual_server_id()) {
                m_daemon->m_repl.client_atomic(m_daemon->m_us, pt_lead, nonce, kc, msg);
                break;
            }
        }
    }
}

void
wan_manager :: send_more_data(const server_id& from,
                              const virtual_server_id&,
                              const virtual_server_id& vto,
                              std::auto_ptr<e::buffer> msg,
                              e::unpacker up)
{
    uint64_t xid;

    if((up >> xid).error()) {
        LOG(WARNING) << "unpack of WAN_MORE failed; here's some hex:  " << msg->hex();
        return;
    }
    // LOG(INFO) << "xfer id = " << xid;
    transfer_out_state *tos = get_tos(transfer_id(xid));

    if (!tos) {
        LOG(INFO) << "dropping WAN_MORE for " << xid << " which we don't know about";
        return;
    }
    po6::threads::mutex::hold hold(&tos->mtx);

    transfer_more_state(tos);
}

void
wan_manager :: handle_ack(const server_id& from,
                      const virtual_server_id& vfrom,
                      const virtual_server_id& vto,
                      std::auto_ptr<e::buffer> msg,
                      e::unpacker up)
{
}

void
wan_manager :: run()
{
    if (m_is_backup) {
        while (!m_daemon->m_coord.should_exit() && !m_teardown) {
            if (!maintain_link()) {
                LOG(INFO) << "WAN MANAGER COULD NOT MAINTAIN LINK";
                if (m_teardown) {
                    break;
                }
            }

            const configuration& old_pconfig(m_config);
            configuration new_pconfig;
            copy_config(&new_pconfig);
            if (old_pconfig.version() < new_pconfig.version()) {
                LOG(INFO) << "moving to cross configuration version=" << new_pconfig.version()
                    << " on this cluster.";
                pause();
                reconfigure(&new_pconfig);
                m_has_config = true;
                if (!m_manual_teardown) {
                    unpause();
                }
            }
        }
    }
}

void
wan_manager :: handle_disruption()
{
    m_busybee_running = false;
}

bool
wan_manager :: recv(server_id* from,
                   virtual_server_id* vfrom,
                   virtual_server_id* vto,
                   network_msgtype* msg_type,
                   std::auto_ptr<e::buffer>* msg,
                   e::unpacker* up)
{
    while (true)
    {
        uint64_t id;
        busybee_returncode rc = m_busybee->recv(&id, msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_SHUTDOWN:
                LOG(INFO) << "busybee shutdown, exiting...";
                handle_disruption();
                return false;
            case BUSYBEE_DISRUPTED:
                LOG(INFO) << "busybee distrupted, exiting...";
                handle_disruption();
                return false;
                break;
            case BUSYBEE_INTERRUPTED:
                LOG(INFO) << "busybee interrupted, exiting...";
                handle_disruption();
                return false;
                break;
            case BUSYBEE_POLLFAILED:
                LOG(INFO) << "receive pollfailed";
            case BUSYBEE_ADDFDFAIL:
                LOG(INFO) << "receive addfdfail";
            case BUSYBEE_TIMEOUT:
                LOG(INFO) << "receive timeout";
            case BUSYBEE_EXTERNAL:
                LOG(INFO) << "receive external";
            default:
                LOG(ERROR) << "busybee unexpectedly returned " << rc;
                continue;
        }

        uint8_t mt;
        uint8_t flags;
        uint64_t version;
        uint64_t vidf;
        uint64_t vidt;
        *up = (*msg)->unpack_from(BUSYBEE_HEADER_SIZE);
        *up = *up >> mt >> flags >> version >> vidt;
        *msg_type = static_cast<network_msgtype>(mt);
        *from = server_id(id);
        *vto = virtual_server_id(vidt);

        // if ((flags & 0x1))
        // {
        //     LOG(INFO) << "unpacked vidf";
        //     *up = *up >> vidf;
        //     *vfrom = virtual_server_id(vidf);
        // }
        // else
        // {
        //     *vfrom = virtual_server_id();
        // }

        if (up->error())
        {
            LOG(WARNING) << "dropping message that has a malformed header; here's some hex: " << (*msg)->hex();
            continue;
        }

        bool from_valid = true;
        bool to_valid = m_daemon->m_us == m_daemon->m_config.get_server_id(*vto) ||
                        *vto == virtual_server_id(UINT64_MAX);

        // If this is a virtual-virtual message, validate with other's config
        if ((flags & 0x1))
        {
            from_valid = *from == m_config.get_server_id(virtual_server_id(vidf));
        }

        // No matter what, wait for the config the sender saw
        if (version > m_config.version())
        {
            LOG(INFO) << "cross version  = " << version;
            LOG(INFO) << "our cross version = " << m_config.version();
            // early_message em(version, id, *msg);
            // m_early_messages.push(em);
            LOG(INFO) << "dropping early message in recv for now";
            continue;
        }

        if ((flags & 0x2) && version < m_config.version())
        {
            continue;
        }

        if (from_valid && to_valid)
        {
#ifdef HD_LOG_ALL_MESSAGES
            LOG(INFO) << "RECV " << *from << "/" << *vfrom << "->" << *vto << " " << *msg_type << " " << (*msg)->hex();
#endif
            return true;
        }

        // Shove the message back at the client so it fails with a reconfigure.
        if (!(flags & 0x1))
        {
            LOG(INFO) << "CONFIGMISMATCH";
            LOG(INFO) << "from_valid = " << from_valid << " to_valid = " << to_valid;
            // mt = static_cast<uint8_t>(CONFIGMISMATCH);
            // (*msg)->pack_at(BUSYBEE_HEADER_SIZE) << mt;
            // m_busybee->send(id, *msg);
        }
    }
}

bool
wan_manager :: send(const virtual_server_id& from,
                      const server_id& to,
                      network_msgtype msg_type,
                      std::auto_ptr<e::buffer> msg)
{
    assert(msg->size() >= HYPERDEX_HEADER_SIZE_VV);

    if (m_daemon->m_us != m_daemon->m_config.get_server_id(from))
    {
        return false;
    }

    uint8_t mt = static_cast<uint8_t>(msg_type);
    uint8_t flags = 1;
    virtual_server_id vto(UINT64_MAX);
    msg->pack_at(BUSYBEE_HEADER_SIZE) << mt << flags << m_daemon->m_config.version() << vto.get() << from.get();

    if (to == server_id())
    {
        return false;
    }

#ifdef HD_LOG_ALL_MESSAGES
    LOG(INFO) << "SEND " << from << "->" << to << " " << msg_type << " " << msg->hex();
#endif

    if (to == m_daemon->m_us)
    {
        m_busybee->deliver(to.get(), msg);
    }
    else
    {
        busybee_returncode rc = m_busybee->send(to.get(), msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_DISRUPTED:
                handle_disruption();
                return false;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_TIMEOUT:
            case BUSYBEE_EXTERNAL:
            case BUSYBEE_INTERRUPTED:
            default:
                LOG(ERROR) << "BusyBee unexpectedly returned " << rc;
                return false;
        }
    }

    return true;
}

bool
wan_manager :: send(const virtual_server_id& vto,
                    network_msgtype msg_type,
                    std::auto_ptr<e::buffer> msg)
{
    assert(msg->size() >= HYPERDEX_HEADER_SIZE_SV);

    uint8_t mt = static_cast<uint8_t>(msg_type);
    uint8_t flags = 0;
    msg->pack_at(BUSYBEE_HEADER_SIZE) << mt << flags << m_config.version() << vto.get();
    server_id to = m_config.get_server_id(vto);

    if (to == server_id())
    {
        return false;
    }

#ifdef HD_LOG_ALL_MESSAGES
    LOG(INFO) << "SEND ->" << vto << " " << msg_type << " " << msg->hex();
#endif

    if (to == m_daemon->m_us)
    {
        m_busybee->deliver(to.get(), msg);
    }
    else
    {
        busybee_returncode rc = m_busybee->send(to.get(), msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_DISRUPTED:
                handle_disruption();
                return false;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_TIMEOUT:
            case BUSYBEE_EXTERNAL:
            case BUSYBEE_INTERRUPTED:
            default:
                LOG(ERROR) << "BusyBee unexpectedly returned " << rc;
                return false;
        }
    }

    return true;
}

void
wan_manager :: background_maintenance()
{
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
        enter_critical_section_background();

        if (m_teardown)
        {
            break;
        }

        replicant_returncode status = REPLICANT_GARBAGE;
        int64_t id = m_coord->loop(1000, &status);

        if (status != REPLICANT_TIMEOUT && status != REPLICANT_INTERRUPTED)
        {
            m_deferred.push(std::make_pair(id, status));
        }

        exit_critical_section();
    }
}

void
wan_manager :: do_sleep()
{
    uint64_t sleep = m_sleep;
    timespec ts;

    while (sleep > 0)
    {
        ts.tv_sec = 0;
        ts.tv_nsec = std::min(static_cast<uint64_t>(10 * 1000ULL * 1000ULL), sleep);
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
wan_manager :: reset_sleep()
{
    uint64_t start_sleep = 1000ULL * 1000ULL;

    if (m_sleep != start_sleep)
    {
        m_sleep = start_sleep;
        LOG(INFO) << "connection to coordinator reestablished";
    }
}

void
wan_manager :: enter_critical_section()
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
    m_kill = false;
}

void
wan_manager :: exit_critical_section()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_locked = false;
    m_kill = false;

    if (m_waiting > 0)
    {
        m_cond.broadcast();
    }
}

void
wan_manager :: enter_critical_section_killable()
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
wan_manager :: exit_critical_section_killable()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_locked = false;
    m_kill = false;

    if (m_waiting > 0)
    {
        m_cond.broadcast();
    }
}

void
wan_manager :: enter_critical_section_background()
{
    po6::threads::mutex::hold hold(&m_mtx);

    while (m_locked || m_waiting > 0)
    {
        ++m_waiting;
        m_cond.wait();
        --m_waiting;
    }

    m_locked = true;
    m_kill = true;
    m_to_kill = pthread_self();
}
