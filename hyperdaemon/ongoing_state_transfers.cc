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

// Google Log
#include <glog/logging.h>

// HyperDisk
#include <hyperdisk/snapshot.h>

// HyperDex
#include <hyperdex/configuration.h>
#include <hyperdex/ids.h>

// HyperDaemon
#include "datalayer.h"
#include "ongoing_state_transfers.h"
#include "runtimeconfig.h"

using hyperdex::configuration;
using hyperdex::entityid;
using hyperdex::instance;
using hyperdex::regionid;

// XXX The correctness of this code relies upon each endpoint sending XFER_MORE
// messages to not only the correct host, but the correct entityid.  It will
// take a little more to enforce that.

class hyperdaemon::ongoing_state_transfers::transfer_in
{
    public:
        transfer_in(const hyperdex::entityid& from) : m_ref(0) {}
#if 0
    public:
        po6::threads::mutex lock;
        std::map<uint64_t, e::intrusive_ptr<op> > ops;
        uint64_t xferred_so_far;
        hyperdex::regionid reg;
        const hyperdex::entityid replicate_from;
        const hyperdex::entityid transfer_entity;
        bool started;
        bool go_live;
        bool triggered;
        std::set<std::pair<e::buffer, uint64_t> > triggers;
        bool failed;

#endif
    private:
        friend class e::intrusive_ptr<transfer_in>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
};

class hyperdaemon::ongoing_state_transfers::transfer_out
{
    public:
        transfer_out(e::intrusive_ptr<hyperdisk::rolling_snapshot> s) : m_ref(0) {}
#if 0
    public:
        po6::threads::mutex lock;
        e::intrusive_ptr<hyperdisk::rolling_snapshot> snap;
        uint64_t xfer_num;
        const hyperdex::entityid replicate_from;
        const hyperdex::entityid transfer_entity;
        bool failed;

#endif
    private:
        friend class e::intrusive_ptr<transfer_out>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
};

hyperdaemon :: ongoing_state_transfers :: ongoing_state_transfers(datalayer* data)
    : m_data(data)
    , m_repl(NULL)
    , m_transfers_in(STATE_TRANSFER_HASHTABLE_SIZE)
    , m_transfers_out(STATE_TRANSFER_HASHTABLE_SIZE)
    , m_shutdown(false)
{
}

hyperdaemon :: ongoing_state_transfers :: ~ongoing_state_transfers() throw ()
{
}

void
hyperdaemon :: ongoing_state_transfers :: prepare(const configuration& newconfig,
                                                  const instance& us)
{
#if 0
    // Make sure that inbound transfer state exists for each in-progress
    // transfer to "us".
    std::map<uint16_t, regionid> in_transfers = newconfig.transfers_to(us);

    for (std::map<uint16_t, regionid>::iterator t = in_transfers.begin();
            t != in_transfers.end(); ++t)
    {
        if (!m_transfers_in.contains(t->first))
        {
            LOG(INFO) << "Initiating inbound transfer #" << t->first << ".";
            e::intrusive_ptr<transfer_in> xfer;
            xfer = new transfer_in(newconfig.tailof(t->second));
            m_transfers_in.insert(t->first, xfer);
        }
    }

    // Make sure that outbound transfer state exists for each in-progress
    // transfer from "us".
    std::map<uint16_t, regionid> out_transfers = newconfig.transfers_from(us);

    for (std::map<uint16_t, regionid>::iterator t = out_transfers.begin();
            t != out_transfers.end(); ++t)
    {
        if (!m_transfers_out.contains(t->first))
        {
            LOG(INFO) << "Initiating outbound transfer #" << t->first << ".";
            e::intrusive_ptr<hyperdisk::rolling_snapshot> snap;
            snap = m_data->make_rolling_snapshot(t->second);
            e::intrusive_ptr<transfer_out> xfer;
            xfer = new transfer_out(snap/*, XXX */);
            m_transfers_out.insert(t->first, xfer);
        }
    }
#endif
}

void
hyperdaemon :: ongoing_state_transfers :: reconfigure(const configuration&,
                                                      const instance&)
{
    // Do nothing.
}

void
hyperdaemon :: ongoing_state_transfers :: cleanup(const configuration& newconfig,
                                                  const instance& us)
{
#if 0
    std::map<uint16_t, regionid> in_transfers = newconfig.transfers_to(us);

    for (transfers_in_map_t::iterator t = m_transfers_in.begin();
            t != m_transfers_in.end(); t.next())
    {
        if (in_transfers.find(t.key()) == in_transfers.end())
        {
            LOG(INFO) << "Stopping transfer #" << t.key() << ".";
            m_transfers_in.remove(t.key());
        }
    }

    std::map<uint16_t, regionid> out_transfers = newconfig.transfers_from(us);

    for (transfers_out_map_t::iterator t = m_transfers_out.begin();
            t != m_transfers_out.end(); t.next())
    {
        if (out_transfers.find(t.key()) == out_transfers.end())
        {
            LOG(INFO) << "Stopping transfer #" << t.key() << ".";
            m_transfers_out.remove(t.key());
        }
    }
#endif
}

void
hyperdaemon :: ongoing_state_transfers :: shutdown()
{
    m_shutdown = true;
}

#if 0
void
hyperdaemon :: ongoing_state_transfers :: region_transfer(const entityid& from,
                                                          const entityid& to)
{
    // Find the outgoing transfer state
    e::intrusive_ptr<transfer_out> t;

    if (!m_transfers_out.lookup(from.subspace, &t))
    {
        return;
    }

    if (t->is_failed())
    {
        m_cl->fail_transfer(from.subspace);
        return;
    }

    network_msgtype type;
    std::auto_ptr<e::buffer> msg;

    po6::threads::mutex::hold hold(t->lock());

    if (t->snap()->valid())
    {
        size_t size = m_comm->header_size()
                    + sizeof(uint64_t)
                    + sizeof(uint64_t)
                    + sizeof(uint32_t) + t->snap->key().size();

        if (t->snap()->has_value())
        {
            type = hyperdex::XFER_PUT;
            size += packspace(t->snap->value());
        }
        else
        {
            type = hyperdex::XFER_DEL;
        }

        e::buffer::unpacker up = *msg << t->xfer_num()
                                      << t->snap()->version()
                                      << t->snap()->key();

        if (t->snap()->has_value())
        {
            up = up << t->snap()->value();
        }

        assert(!up.error());
        t->inc_xfer_num();
        t->snap()->next();
    }
    else
    {
        type = hyperdex::XFER_DONE;
        msg.reset(e::buffer::create(m_comm->header_size()));
    }

    if (!m_comm->send(to, from, type, msg))
    {
        t->fail();
        m_cl->fail_transfer(from.subspace);
        return;
    }
}

void
hyperdaemon :: ongoing_state_transfers :: region_transfer(const entityid& from,
                                                          uint16_t xfer_id,
                                                          uint64_t xfer_num,
                                                          bool has_value,
                                                          uint64_t version,
                                                          const e::buffer& key,
                                                          const std::vector<e::buffer>& value)
{
    // Find the incoming transfer state
    e::intrusive_ptr<transfer_in> t;

    if (!m_transfers_in.lookup(from.subspace, &t))
    {
        return;
    }

    if (t->is_failed())
    {
        m_cl->fail_transfer(from.subspace);
        return;
    }

#if 0
    // Grab a lock to ensure that we order the puts to disk correctly.
    keypair kp(t->replicate_from.get_region(), key);
    e::striped_lock<po6::threads::mutex>::hold hold_k(&m_locks, get_lock_num(kp));

    // Grab a lock to ensure we can safely update the transfer object.
    po6::threads::mutex::hold hold_t(&t->lock);

    // If we've triggered, then we can safely drop the message.
    if (t->triggered)
    {
        return;
    }

    // This is a probabilistic test of the remote end's failure.  If we have
    // queued more than 1000 messages, then somewhere one got dropped as the
    // other end inserts them into the queue in FIFO order, and they are
    // delivered to threads in FIFO order.  There is always a chance that what
    // really happened was one thread pulled off the missing message, and then
    // was blocked arbitrarily long while other threads worked through several
    // messages.  The large constant ensures that this is exceedingly unlikely
    // (but admittedly still possible).
    if (t->ops.size() > TRANSFERS_IN_FLIGHT)
    {
        t->failed = true;
        m_cl->fail_transfer(xfer_id);
        return;
    }

    // Insert the new operation.
    e::intrusive_ptr<transfer_in::op> o = new transfer_in::op(has_value, version, key, value);
    t->ops.insert(std::make_pair(xfer_num, o));

    while (!t->ops.empty() && t->ops.begin()->first == t->xferred_so_far + 1)
    {
        transfer_in::op& one(*t->ops.begin()->second);

        if (t->triggers.find(std::make_pair(one.key, one.version)) != t->triggers.end())
        {
            t->triggered = true;
            LOG(INFO) << "COMPLETE TRANSFER " << xfer_id;
            return;
        }

        if (t->triggers.lower_bound(std::make_pair(one.key, 0)) ==
                t->triggers.upper_bound(std::make_pair(one.key, std::numeric_limits<uint64_t>::max() - 1)))
        {
            hyperdisk::returncode res;

            if (one.has_value)
            {
                switch ((res = m_data->put(t->replicate_from.get_region(), one.key, one.value, one.version)))
                {
                    case hyperdisk::SUCCESS:
                        break;
                    case hyperdisk::WRONGARITY:
                        LOG(ERROR) << "Transfer " << xfer_id << " caused WRONGARITY.";
                        break;
                    case hyperdisk::MISSINGDISK:
                        LOG(ERROR) << "Transfer " << xfer_id << " caused MISSINGDISK.";
                        break;
                    case hyperdisk::NOTFOUND:
                    case hyperdisk::DATAFULL:
                    case hyperdisk::SEARCHFULL:
                    case hyperdisk::SYNCFAILED:
                    case hyperdisk::DROPFAILED:
                    case hyperdisk::SPLITFAILED:
                    case hyperdisk::DIDNOTHING:
                    default:
                        LOG(ERROR) << "Transfer " << xfer_id << " caused unexpected error code.";
                        break;
                }
            }
            else
            {
                switch ((res = m_data->del(t->replicate_from.get_region(), one.key)))
                {
                    case hyperdisk::SUCCESS:
                        break;
                    case hyperdisk::MISSINGDISK:
                        LOG(ERROR) << "Transfer " << xfer_id << " caused MISSINGDISK.";
                        break;
                    case hyperdisk::WRONGARITY:
                    case hyperdisk::NOTFOUND:
                    case hyperdisk::DATAFULL:
                    case hyperdisk::SEARCHFULL:
                    case hyperdisk::SYNCFAILED:
                    case hyperdisk::DROPFAILED:
                    case hyperdisk::SPLITFAILED:
                    case hyperdisk::DIDNOTHING:
                    default:
                        LOG(ERROR) << "Transfer " << xfer_id << " caused unexpected error code.";
                        break;
                }
            }

            if (res != hyperdisk::SUCCESS)
            {
                t->failed = true;
                m_cl->fail_transfer(xfer_id);
                return;
            }
        }

        ++t->xferred_so_far;
        t->ops.erase(t->ops.begin());
    }

    t->started = true;
    e::buffer msg;

    if (!m_comm->send(t->transfer_entity, t->replicate_from, hyperdex::XFER_MORE, msg))
    {
        t->failed = true;
        m_cl->fail_transfer(xfer_id);
        return;
    }
#endif
}

void
hyperdaemon :: ongoing_state_transfers :: region_transfer_done(const entityid& from, const entityid& to)
{
    // Find the incoming transfer state
    e::intrusive_ptr<transfer_in> t;

    if (!m_transfers_in.lookup(from.subspace, &t))
    {
        return;
    }

    if (t->is_failed())
    {
        m_cl->fail_transfer(from.subspace);
        return;
    }

    po6::threads::mutex::hold hold(t->lock());

#if 0
    if (from != t->replicate_from)
    {
        return;
    }

    t->started = true;

    if (!t->go_live)
    {
        t->go_live = true;
        LOG(INFO) << "REGION GO LIVE " << to.subspace;
    }
#endif
}


#if 0
void
hyperdaemon :: ongoing_state_transfers :: start_transfers()
{
    for (std::map<uint16_t, e::intrusive_ptr<transfer_in> >::iterator t = m_transfers_in.begin();
            t != m_transfers_in.end(); ++t)
    {
        if (!t->second->started)
        {
            e::buffer msg;

            for (size_t i = 0; i < TRANSFERS_IN_FLIGHT; ++i)
            {
                m_comm->send(t->second->transfer_entity, t->second->replicate_from, hyperdex::XFER_MORE, msg);
            }
        }
    }
}

void
hyperdaemon :: ongoing_state_transfers :: finish_transfers()
{
    for (std::map<uint16_t, e::intrusive_ptr<transfer_in> >::iterator t = m_transfers_in.begin();
            t != m_transfers_in.end(); ++t)
    {
        if (t->second->go_live)
        {
            e::buffer msg;
            m_comm->send(t->second->transfer_entity, t->second->replicate_from, hyperdex::XFER_MORE, msg);
        }
    }
}
#endif
#endif

void
hyperdaemon :: ongoing_state_transfers :: add_trigger(const hyperdex::regionid& reg,
                                                      const e::slice& key,
                                                      uint64_t rev)
{
    // XXX Must actually trigger something
}

void
hyperdaemon :: ongoing_state_transfers :: set_replication_manager(replication_manager* repl)
{
    m_repl = repl;
}

#if 0

        try
        {
            // Every second.
            if (i % 4 == 0)
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
            // Every ten seconds.
            if (i % 40 == 0)
            {
                finish_transfers();
            }
        }
        catch (std::exception& e)
        {
            LOG(INFO) << "Uncaught exception when finishing transfers: " << e.what();
        }
#endif
