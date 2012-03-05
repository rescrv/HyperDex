// Copyright (c) 2011-2012, Cornell University
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

// STL
#include <map>

// Google Log
#include <glog/logging.h>

// e
#include <e/timer.h>

// HyperDisk
#include "hyperdisk/hyperdisk/snapshot.h"

// HyperDex
#include "hyperdex/hyperdex/configuration.h"
#include "hyperdex/hyperdex/coordinatorlink.h"
#include "hyperdex/hyperdex/network_constants.h"
#include "hyperdex/hyperdex/packing.h"

// HyperDaemon
#include "hyperdaemon/datalayer.h"
#include "hyperdaemon/logical.h"
#include "hyperdaemon/ongoing_state_transfers.h"
#include "hyperdaemon/replication_manager.h"
#include "hyperdaemon/runtimeconfig.h"

using hyperdex::configuration;
using hyperdex::entityid;
using hyperdex::instance;
using hyperdex::network_msgtype;
using hyperdex::regionid;

///////////////////////////////// Transfers In /////////////////////////////////

class hyperdaemon::ongoing_state_transfers::transfer_in
{
    public:
        class op
        {
            public:
                op(bool hv, uint64_t ver,
                   std::tr1::shared_ptr<e::buffer> b,
                   const e::slice& k,
                   const std::vector<e::slice>& val)
                    : has_value(hv)
                    , version(ver)
                    , backing(b)
                    , key(k)
                    , value(val)
                    , m_ref(0)
                {
                }

            public:
                bool has_value;
                uint64_t version;
                std::tr1::shared_ptr<e::buffer> backing;
                const e::slice key;
                const std::vector<e::slice> value;

            private:
                friend class e::intrusive_ptr<op>;

            private:
                void inc() { __sync_add_and_fetch(&m_ref, 1); }
                void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

            private:
                size_t m_ref;
        };

    public:
        transfer_in(const hyperdex::entityid& from);

    public:
        po6::threads::mutex lock;
        std::map<uint64_t, e::intrusive_ptr<op> > ops;
        // "triggers" is protected by the replication lock.
        std::map<std::pair<e::slice, uint64_t>, std::tr1::shared_ptr<e::buffer> > triggers;
        const hyperdex::entityid replicate_from;
        uint64_t xfer_num;
        bool failed;
        bool started;
        bool go_live;
        bool triggered;

    private:
        friend class e::intrusive_ptr<transfer_in>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
};

hyperdaemon :: ongoing_state_transfers :: transfer_in :: transfer_in(const hyperdex::entityid& from)
    : lock()
    , ops()
    , triggers()
    , replicate_from(from)
    , xfer_num(0)
    , failed(false)
    , started(false)
    , go_live(false)
    , triggered(false)
    , m_ref(0)
{
}

///////////////////////////////// Transfers Out ////////////////////////////////

class hyperdaemon::ongoing_state_transfers::transfer_out
{
    public:
        transfer_out(e::intrusive_ptr<hyperdisk::rolling_snapshot> s);

    public:
        po6::threads::mutex lock;
        e::intrusive_ptr<hyperdisk::rolling_snapshot> snap;
        uint64_t xfer_num;
        bool failed;

    private:
        friend class e::intrusive_ptr<transfer_out>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
};

hyperdaemon :: ongoing_state_transfers :: transfer_out :: transfer_out(e::intrusive_ptr<hyperdisk::rolling_snapshot> s)
    : lock()
    , snap(s)
    , xfer_num(1)
    , failed(false)
    , m_ref(0)
{
}

///////////////////////////////// Public Class /////////////////////////////////

hyperdaemon :: ongoing_state_transfers :: ongoing_state_transfers(datalayer* data,
                                                                  logical* comm,
                                                                  hyperdex::coordinatorlink* cl)
    : m_data(data)
    , m_comm(comm)
    , m_cl(cl)
    , m_repl(NULL)
    , m_config()
    , m_transfers_in(STATE_TRANSFER_HASHTABLE_SIZE)
    , m_transfers_out(STATE_TRANSFER_HASHTABLE_SIZE)
    , m_shutdown(false)
    , m_periodic_lock()
    , m_periodic_thread(std::tr1::bind(&ongoing_state_transfers::periodic, this))
{
    m_periodic_thread.start();
}

hyperdaemon :: ongoing_state_transfers :: ~ongoing_state_transfers() throw ()
{
    if (!m_shutdown)
    {
        shutdown();
    }

    m_periodic_thread.join();
}

void
hyperdaemon :: ongoing_state_transfers :: prepare(const configuration& newconfig,
                                                  const instance& us)
{
    po6::threads::mutex::hold hold(&m_periodic_lock);
    std::map<uint16_t, regionid> in_transfers = newconfig.transfers_to(us);
    std::map<uint16_t, regionid> out_transfers = newconfig.transfers_from(us);
    std::map<uint16_t, regionid>::iterator t;

    // Make sure that inbound state exists for each in-progress transfer to us.
    for (t = in_transfers.begin(); t != in_transfers.end(); ++t)
    {
        if (!m_transfers_in.contains(t->first))
        {
            LOG(INFO) << "Initiating inbound transfer #" << t->first;
            e::intrusive_ptr<transfer_in> xfer;
            xfer = new transfer_in(newconfig.tailof(t->second));
            m_transfers_in.insert(t->first, xfer);
        }
    }

    // Make sure that outbound state exists for each in-progress transfer from us
    for (t = out_transfers.begin(); t != out_transfers.end(); ++t)
    {
        if (!m_transfers_out.contains(t->first))
        {
            LOG(INFO) << "Initiating outbound transfer #" << t->first;
            e::intrusive_ptr<hyperdisk::rolling_snapshot> snap;
            snap = m_data->make_rolling_snapshot(t->second);
            e::intrusive_ptr<transfer_out> xfer;
            xfer = new transfer_out(snap);
            m_transfers_out.insert(t->first, xfer);
        }
    }
}

void
hyperdaemon :: ongoing_state_transfers :: reconfigure(const configuration& config,
                                                      const instance&)
{
    po6::threads::mutex::hold hold(&m_periodic_lock);
    m_config = config;
    __sync_synchronize();
}

void
hyperdaemon :: ongoing_state_transfers :: cleanup(const configuration& newconfig,
                                                  const instance& us)
{
    po6::threads::mutex::hold hold(&m_periodic_lock);
    std::map<uint16_t, regionid> in_transfers = newconfig.transfers_to(us);
    std::map<uint16_t, regionid> out_transfers = newconfig.transfers_from(us);

    for (transfers_in_map_t::iterator ti = m_transfers_in.begin();
            ti != m_transfers_in.end(); ti.next())
    {
        std::map<uint16_t, regionid>::iterator it;
        it = in_transfers.find(ti.key());

        if (it == in_transfers.end())
        {
            LOG(INFO) << "Stopping incoming transfer #" << ti.key();
            m_transfers_in.remove(ti.key());
        }
        else
        {
            // Anything that we've tagged "go_live" that is not live needs to be
            // resent to the coordinator.
            if (ti.value()->go_live &&
                m_config.entityfor(us, it->second) == entityid())
            {
                m_cl->transfer_golive(it->first);
            }

            // If we've marked this complete, we should resend the message.
            if (ti.value()->triggered)
            {
                m_cl->transfer_complete(it->first);
            }
        }
    }

    for (transfers_out_map_t::iterator to = m_transfers_out.begin();
            to != m_transfers_out.end(); to.next())
    {
        if (out_transfers.find(to.key()) == out_transfers.end())
        {
            LOG(INFO) << "Stopping outgoing transfer #" << to.key();
            m_transfers_out.remove(to.key());
        }
    }
}

void
hyperdaemon :: ongoing_state_transfers :: shutdown()
{
    m_shutdown = true;
}

void
hyperdaemon :: ongoing_state_transfers :: region_transfer_send(const entityid& from,
                                                               const entityid& to)
{
    // Find the outgoing transfer state
    e::intrusive_ptr<transfer_out> t;

    if (!m_transfers_out.lookup(from.subspace, &t))
    {
        LOG(WARNING) << "dropping request for unknown network transfer " << from.subspace;
        return;
    }

    if (t->failed)
    {
        m_cl->transfer_fail(from.subspace);
        return;
    }

    network_msgtype type;
    std::auto_ptr<e::buffer> msg;

    po6::threads::mutex::hold hold(&t->lock);

    if (t->snap->valid())
    {
        size_t size = m_comm->header_size()
                    + sizeof(uint64_t)
                    + sizeof(uint64_t)
                    + sizeof(uint32_t)
                    + t->snap->key().size()
                    + sizeof(uint8_t);
        type = hyperdex::XFER_DATA;

        if (t->snap->has_value())
        {
            size += hyperdex::packspace(t->snap->value());
        }

        msg.reset(e::buffer::create(size));
        e::buffer::packer pa = msg->pack_at(m_comm->header_size());
        pa = pa << t->xfer_num
                << t->snap->version()
                << t->snap->key();

        if (t->snap->has_value())
        {
            pa = pa << static_cast<uint8_t>(1) << t->snap->value();
        }
        else
        {
            pa = pa << static_cast<uint8_t>(0);
        }

        assert(!pa.error());
        ++t->xfer_num;
        t->snap->next();
    }
    else
    {
        type = hyperdex::XFER_DONE;
        msg.reset(e::buffer::create(m_comm->header_size()));
    }

    if (!m_comm->send(to, from, type, msg))
    {
        t->failed = true;
        m_cl->transfer_fail(from.subspace);
    }
}

void
hyperdaemon :: ongoing_state_transfers :: region_transfer_recv(const hyperdex::entityid& from,
                                                               uint16_t xfer_id,
                                                               uint64_t xfer_num,
                                                               bool has_value,
                                                               uint64_t version,
                                                               std::auto_ptr<e::buffer> backing,
                                                               const e::slice& key,
                                                               const std::vector<e::slice>& value)
{
    // Find the incoming transfer state
    e::intrusive_ptr<transfer_in> t;

    if (!m_transfers_in.lookup(xfer_id, &t))
    {
        LOG(INFO) << "received XFER_DATA for transfer #" << xfer_id << ", but we know nothing about it";
        return;
    }

    // Grab a lock to ensure we can safely update the transfer object.
    po6::threads::mutex::hold hold_t(&t->lock);

    if (t->failed)
    {
        LOG(INFO) << "received XFER_DATA for transfer #" << xfer_id << ", but we have failed it";
        m_cl->transfer_fail(xfer_id);
        return;
    }

    // If we've triggered, then we can safely drop the message.
    if (t->triggered)
    {
        return;
    }

    // This is a probabilistic test of the remote end's failure.  If we have
    // queued more than TRANSFERS_IN_FLIGHT messages, then somewhere one got
    // dropped as the other end inserts them into the queue in FIFO order, and
    // they are delivered to threads in FIFO order.  There is always a chance
    // that what really happened was one thread pulled off the missing message,
    // and then was blocked arbitrarily long while other threads worked through
    // several messages.  The large constant ensures that this is exceedingly
    // unlikely (but admittedly still possible).  Best chances are when the
    // constant is many times the number of network threads
    //  XXX Autotune this.
    if (t->ops.size() > TRANSFERS_IN_FLIGHT * 64)
    {
        t->failed = true;
        m_cl->transfer_fail(xfer_id);
        return;
    }

    // Insert the new operation.
    std::tr1::shared_ptr<e::buffer> back(backing);
    e::intrusive_ptr<transfer_in::op> o = new transfer_in::op(has_value, version, back, key, value);
    t->ops.insert(std::make_pair(xfer_num, o));

    while (!t->ops.empty() && t->ops.begin()->first == t->xfer_num + 1)
    {
        transfer_in::op& oneop(*t->ops.begin()->second);

        // XXX We should do better than being friends with m_repl.
        // Grab a lock to ensure that we order the puts to disk correctly.
        e::striped_lock<po6::threads::mutex>::hold hold(&m_repl->m_locks, m_repl->get_lock_num(from.get_region(), oneop.key));

        // If this op acts as a trigger
        if (t->triggers.find(std::make_pair(oneop.key, oneop.version)) !=
                t->triggers.end())
        {
            t->triggered = true;
            m_cl->transfer_complete(xfer_id);
            return;
        }

        // This op can only be put to disk when there is not another version of
        // the key in the triggers.  If there is another version in the
        // triggers, then this version or another has already been written to
        // disk and we cannot overwrite it.
        if (t->triggers.lower_bound(std::make_pair(oneop.key, 0)) ==
                t->triggers.upper_bound(std::make_pair(oneop.key, UINT64_MAX)))
        {
            hyperdisk::returncode res;

            if (oneop.has_value)
            {
                res = m_data->put(t->replicate_from.get_region(), oneop.backing, oneop.key, oneop.value, oneop.version);
            }
            else
            {
                res = m_data->del(t->replicate_from.get_region(), oneop.backing, oneop.key);
            }

            if (res != hyperdisk::SUCCESS)
            {
                LOG(ERROR) << "transfer " << xfer_id << " failed because HyperDisk returned " << res;
                t->failed = true;
                m_cl->transfer_fail(xfer_id);
                return;
            }

            m_repl->check_for_deferred_operations(from.get_region(),
                    oneop.version, oneop.backing, oneop.key, oneop.has_value,
                    oneop.value);
        }

        t->ops.erase(t->ops.begin());
        ++t->xfer_num;
    }

    t->started = true;
    std::auto_ptr<e::buffer> msg(e::buffer::create(m_comm->header_size()));

    if (!m_comm->send(entityid(configuration::TRANSFERSPACE, xfer_id, 0, 0, 0),
                      t->replicate_from, hyperdex::XFER_MORE, msg))
    {
        t->failed = true;
        m_cl->transfer_fail(xfer_id);
        return;
    }
}

void
hyperdaemon :: ongoing_state_transfers :: region_transfer_done(const entityid& from,
                                                               const entityid& to)
{
    // Find the incoming transfer state
    e::intrusive_ptr<transfer_in> t;

    if (!m_transfers_in.lookup(to.subspace, &t))
    {
        LOG(INFO) << "received XFER_DONE for transfer #" << to.subspace << ", but we know nothing about it";
        return;
    }

    po6::threads::mutex::hold hold(&t->lock);

    if (t->failed)
    {
        LOG(INFO) << "received XFER_DONE for transfer #" << to.subspace << ", but we have failed it";
        m_cl->transfer_fail(to.subspace);
        return;
    }

    if (from != t->replicate_from)
    {
        LOG(WARNING) << "another host is stepping on transfer #" << to.subspace;
        return;
    }

    t->started = true;

    if (!t->go_live)
    {
        LOG(WARNING) << "transfer #" << to.subspace << " is asking to go live";
        t->go_live = true;
        m_cl->transfer_golive(to.subspace);
    }
}

void
hyperdaemon :: ongoing_state_transfers :: add_trigger(const hyperdex::regionid& reg,
                                                      std::tr1::shared_ptr<e::buffer> backing,
                                                      const e::slice& key,
                                                      uint64_t rev)
{
    uint16_t xfer_id = m_config.transfer_id(reg);
    e::intrusive_ptr<transfer_in> t;

    if (!m_transfers_in.lookup(xfer_id, &t) || !t)
    {
        return;
    }

    t->triggers[std::make_pair(key, rev)] = backing;
}

void
hyperdaemon :: ongoing_state_transfers :: set_replication_manager(replication_manager* repl)
{
    m_repl = repl;
}

void
hyperdaemon :: ongoing_state_transfers :: periodic()
{
    LOG(WARNING) << "State transfer \"cron\" thread started.";

    for (uint64_t i = 0; !m_shutdown; ++i)
    {
        try
        {
            // Every half second
            if (i % 2 == 0)
            {
                start_transfers();
            }
        }
        catch (std::exception& e)
        {
            LOG(INFO) << "Uncaught exception when starting transfers: " << e.what();
        }

        try
        {
            // Every second
            if (i % 4 == 0)
            {
                finish_transfers();
            }
        }
        catch (std::exception& e)
        {
            LOG(INFO) << "Uncaught exception when finishing transfers: " << e.what();
        }

        e::sleep_ms(250);
    }
}

void
hyperdaemon :: ongoing_state_transfers :: start_transfers()
{
    for (transfers_in_map_t::iterator t = m_transfers_in.begin();
            t != m_transfers_in.end(); t.next())
    {
        po6::threads::mutex::hold hold(&m_periodic_lock);

        if (!t.value()->started)
        {
            for (size_t i = 0; i < TRANSFERS_IN_FLIGHT; ++i)
            {
                std::auto_ptr<e::buffer> msg(e::buffer::create(m_comm->header_size()));
                m_comm->send(entityid(configuration::TRANSFERSPACE, t.key(), 0, 0, 0),
                             t.value()->replicate_from, hyperdex::XFER_MORE, msg);
            }
        }
    }
}

void
hyperdaemon :: ongoing_state_transfers :: finish_transfers()
{
    for (transfers_in_map_t::iterator t = m_transfers_in.begin();
            t != m_transfers_in.end(); t.next())
    {
        po6::threads::mutex::hold hold(&m_periodic_lock);

        if (t.value()->go_live)
        {
            std::auto_ptr<e::buffer> msg(e::buffer::create(m_comm->header_size()));
            if (!m_comm->send(entityid(configuration::TRANSFERSPACE, t.key(), 0, 0, 0),
                              t.value()->replicate_from, hyperdex::XFER_MORE, msg))
            {
                t.value()->failed = true;
                m_cl->transfer_fail(t.key());
            }
        }
    }
}
