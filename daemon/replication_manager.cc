// Copyright (c) 2011-2014, Cornell University
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
#include "common/datatype_info.h"
#include "common/hash.h"
#include "common/serialization.h"
#include "cityhash/city.h"
#include "daemon/background_thread.h"
#include "daemon/daemon.h"
#include "daemon/replication_manager.h"

using po6::threads::make_thread_wrapper;
using hyperdex::key_state;
using hyperdex::reconfigure_returncode;
using hyperdex::replication_manager;

class replication_manager::retransmitter_thread : public hyperdex::background_thread
{
    public:
        retransmitter_thread(daemon* d);
        ~retransmitter_thread() throw ();

    public:
        virtual const char* thread_name() { return "replication"; }
        virtual bool have_work();
        virtual void copy_work();
        virtual void do_work();

    public:
        void trigger();

    public:
        replication_manager* m_rm;
        uint64_t m_trigger;

    private:
        retransmitter_thread(const retransmitter_thread&);
        retransmitter_thread& operator = (const retransmitter_thread&);
};

replication_manager :: replication_manager(daemon* d)
    : m_daemon(d)
    , m_key_states(&d->m_gc)
    , m_idgen()
    , m_idcol(&d->m_gc)
    , m_stable()
    , m_retransmitter(new retransmitter_thread(d))
    , m_protect_stable_stuff()
    , m_checkpoint(0)
    , m_need_check(0)
    , m_timestamps()
    , m_unstable()
{
    po6::threads::mutex::hold hold(&m_protect_stable_stuff);
    check_is_needed();
}

replication_manager :: ~replication_manager() throw ()
{
    m_retransmitter->shutdown();
}

bool
replication_manager :: setup()
{
    m_retransmitter->start();
    return true;
}

void
replication_manager :: teardown()
{
    m_retransmitter->shutdown();
}

void
replication_manager :: pause()
{
    m_retransmitter->initiate_pause();
}

void
replication_manager :: unpause()
{
    m_retransmitter->unpause();
    m_retransmitter->trigger();
}

void
replication_manager :: reconfigure(const configuration&,
                                   const configuration& new_config,
                                   const server_id&)
{
    m_retransmitter->wait_until_paused();
    m_retransmitter->trigger();

    std::vector<region_id> key_regions;
    new_config.key_regions(m_daemon->m_us, &key_regions);
    m_idgen.adopt(&key_regions[0], key_regions.size());
    m_idcol.adopt(&key_regions[0], key_regions.size());

    std::vector<region_id> transfers_in_regions;
    new_config.transfers_in_regions(m_daemon->m_us, &transfers_in_regions);

    // iterate over all key states; cleanup dead ones, and bump idgen
    for (key_map_t::iterator it(&m_key_states); it.valid(); ++it)
    {
        key_state* ks = *it;
        ks->reconfigure(&m_daemon->m_gc);
        region_id ri = ks->state_key().region;

        if (std::binary_search(transfers_in_regions.begin(),
                               transfers_in_regions.end(), ri))
        {
            ks->reset(&m_daemon->m_gc);
        }

        if (std::binary_search(key_regions.begin(),
                               key_regions.end(), ri))
        {
            m_idgen.bump(ri, ks->max_version());
        }
    }

    // iterate over all regions on disk/lb, and bump idgen
    for (size_t i = 0; i < key_regions.size(); ++i)
    {
        uint64_t disk_ver = m_daemon->m_data.max_version(key_regions[i]);
        uint64_t mem_ver = m_idcol.lower_bound(key_regions[i]);
        uint64_t max_ver = std::max(disk_ver, mem_ver);

        if (max_ver > 0)
        {
            m_idgen.bump(key_regions[i], max_ver - 1);
        }
    }

    // figure out when we're stable
    m_stable.copy_from(m_idgen);

    // clear timestamps for regions we no longer manage
    std::vector<region_id> mapped_regions;
    m_daemon->m_config.mapped_regions(m_daemon->m_us, &mapped_regions);
    po6::threads::mutex::hold hold(&m_protect_stable_stuff);
    reset_to_unstable();

    for (size_t i = 0; i < m_timestamps.size(); )
    {
        if (!std::binary_search(mapped_regions.begin(),
                                mapped_regions.end(),
                                m_timestamps[i].rid))
        {
            m_timestamps[i] = m_timestamps.back();
            m_timestamps.pop_back();
        }
        else
        {
            ++i;
        }
    }
}

void
replication_manager :: debug_dump()
{
    m_retransmitter->initiate_pause();
    m_retransmitter->wait_until_paused();
    std::vector<region_id> regions;
    m_daemon->m_config.key_regions(m_daemon->m_us, &regions);

    // print counters
    LOG(INFO) << "region counters ===============================================================";

    for (size_t i = 0; i < regions.size(); ++i)
    {
        uint64_t idgen = m_idgen.peek(regions[i]);
        uint64_t lb = m_idcol.lower_bound(regions[i]);
        uint64_t stable = m_stable.peek(regions[i]);
        LOG(INFO) << regions[i] << " idgen=" << idgen << " lb=" << lb << " stable=" << stable;
    }

    // print key state
    LOG(INFO) << "key states ====================================================================";

    for (key_map_t::iterator it(&m_key_states); it.valid(); ++it)
    {
        key_state* ks = *it;
        LOG(INFO) << "state for " << ks->state_key().region << " " << e::slice(ks->state_key().key).hex();
        ks->debug_dump();
    }

    m_retransmitter->unpause();
    m_retransmitter->trigger();
}

void
replication_manager :: client_atomic(const server_id& from,
                                     const virtual_server_id& to,
                                     uint64_t nonce,
                                     std::auto_ptr<key_change> kc,
                                     std::auto_ptr<e::buffer> backing)
{
    const region_id ri(m_daemon->m_config.get_region_id(to));
    const schema& sc(*m_daemon->m_config.get_schema(ri));

    if (m_daemon->m_config.read_only())
    {
        respond_to_client(to, from, nonce, NET_READONLY);
        return;
    }

    if (!kc->validate(sc))
    {
        LOG(ERROR) << "dropping nonce=" << nonce << " from client=" << from
                   << " because the key, checks, or funcs don't validate";
        respond_to_client(to, from, nonce, NET_BADDIMSPEC);
        return;
    }

    if (m_daemon->m_config.point_leader(ri, kc->key) != to)
    {
        LOG(ERROR) << "dropping nonce=" << nonce << " from client=" << from
                   << " because it doesn't map to " << ri;
        respond_to_client(to, from, nonce, NET_NOTUS);
        return;
    }

    key_map_t::state_reference ksr;
    key_state* ks = get_or_create_key_state(ri, kc->key, &ksr);
    ks->enqueue_client_atomic(this, to, sc, from, nonce, kc, backing);
}

void
replication_manager :: chain_op(const virtual_server_id& from,
                                const virtual_server_id& to,
                                uint64_t old_version,
                                uint64_t new_version,
                                bool fresh,
                                bool has_value,
                                const e::slice& key,
                                const std::vector<e::slice>& value,
                                std::auto_ptr<e::buffer> backing)
{
    const region_id ri(m_daemon->m_config.get_region_id(to));
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    bool valid = sc.attrs_sz == value.size() + 1 &&
                 datatype_info::lookup(sc.attrs[0].type)->validate(key);

    if (has_value)
    {
        valid = valid && sc.attrs_sz == value.size() + 1;

        for (size_t i = 0; valid && i + 1 < sc.attrs_sz; ++i)
        {
            valid = datatype_info::lookup(sc.attrs[i + 1].type)->validate(value[i]);
        }
    }

    if (!valid)
    {
        LOG(ERROR) << "dropping CHAIN_OP because the dimensions are incorrect";
        return;
    }

    key_map_t::state_reference ksr;
    key_state* ks = get_or_create_key_state(ri, key, &ksr);
    ks->enqueue_chain_op(this, to, sc, from, old_version, new_version, fresh, has_value, value, backing);
}

void
replication_manager :: chain_subspace(const virtual_server_id& from,
                                      const virtual_server_id& to,
                                      uint64_t old_version,
                                      uint64_t new_version,
                                      const e::slice& key,
                                      const std::vector<e::slice>& value,
                                      std::auto_ptr<e::buffer> backing,
                                      const region_id& prev_region,
                                      const region_id& this_old_region,
                                      const region_id& this_new_region,
                                      const region_id& next_region)
{
    const region_id ri(m_daemon->m_config.get_region_id(to));
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    bool valid = sc.attrs_sz == value.size() + 1 &&
                 datatype_info::lookup(sc.attrs[0].type)->validate(key);

    for (size_t i = 0; valid && i + 1 < sc.attrs_sz; ++i)
    {
        valid = datatype_info::lookup(sc.attrs[i + 1].type)->validate(value[i]);
    }

    if (!valid)
    {
        LOG(ERROR) << "dropping CHAIN_SUBSPACE because the dimensions are incorrect";
        return;
    }

    key_map_t::state_reference ksr;
    key_state* ks = get_or_create_key_state(ri, key, &ksr);
    ks->enqueue_chain_subspace(this, to, sc, from, old_version, new_version, value, backing,
                               prev_region, this_old_region, this_new_region, next_region);
}

void
replication_manager :: chain_ack(const virtual_server_id& from,
                                 const virtual_server_id& to,
                                 uint64_t version,
                                 const e::slice& key)
{
    const region_id ri(m_daemon->m_config.get_region_id(to));
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    key_map_t::state_reference ksr;
    key_state* ks = get_key_state(ri, key, &ksr);

    if (!ks)
    {
        LOG(ERROR) << "dropping CHAIN_ACK for update we haven't seen";
        LOG(ERROR) << "troubleshoot info: from=" << from << " to=" << to
                   << " version=" << version << " key=" << key.hex();
        return;
    }

    ks->enqueue_chain_ack(this, to, sc, from, version);
}

void
replication_manager :: begin_checkpoint(uint64_t checkpoint_num)
{
    m_retransmitter->initiate_pause();
    m_retransmitter->wait_until_paused();
    std::string timestamp = m_daemon->m_data.get_timestamp();

    {
        std::vector<region_id> mapped_regions;
        m_daemon->m_config.mapped_regions(m_daemon->m_us, &mapped_regions);
        po6::threads::mutex::hold hold(&m_protect_stable_stuff);
        m_checkpoint = std::max(m_checkpoint, checkpoint_num);
        reset_to_unstable();

        for (size_t i = 0; i < mapped_regions.size(); ++i)
        {
            m_timestamps.push_back(region_timestamp(mapped_regions[i], checkpoint_num, timestamp));
        }
    }

    std::vector<region_id> key_regions;
    m_daemon->m_config.key_regions(m_daemon->m_us, &key_regions);

    for (size_t i = 0; i < key_regions.size(); ++i)
    {
        uint64_t id = m_idgen.peek(key_regions[i]);

        if (id > 0)
        {
            bool x = m_stable.bump(key_regions[i], id - 1);
            assert(x);
        }

        check_stable(key_regions[i]);
    }

    check_stable();
    m_retransmitter->unpause();
    m_retransmitter->trigger();
}

void
replication_manager :: end_checkpoint(uint64_t checkpoint_num)
{
    m_retransmitter->initiate_pause();
    m_retransmitter->wait_until_paused();
    po6::threads::mutex::hold hold(&m_protect_stable_stuff);

    for (size_t i = 0; i < m_timestamps.size(); )
    {
        if (m_timestamps[i].checkpoint <= checkpoint_num)
        {
            m_daemon->m_data.create_checkpoint(m_timestamps[i]);
            std::swap(m_timestamps[i], m_timestamps.back());
            m_timestamps.pop_back();
        }
        else
        {
            ++i;
        }
    }

    m_retransmitter->unpause();
    m_retransmitter->trigger();
}

key_state*
replication_manager :: get_key_state(const region_id& ri,
                                     const e::slice& key,
                                     key_map_t::state_reference* ksr)
{
    key_region kr(ri, key);
    key_state* ks = m_key_states.get_state(kr, ksr);
    return post_get_key_state_init(ri, ks);
}

key_state*
replication_manager :: get_or_create_key_state(const region_id& ri,
                                               const e::slice& key,
                                               key_map_t::state_reference* ksr)
{
    key_region kr(ri, key);
    key_state* ks = m_key_states.get_or_create_state(kr, ksr);
    return post_get_key_state_init(ri, ks);
}

key_state*
replication_manager :: post_get_key_state_init(region_id ri, key_state* ks)
{
    if (!ks || ks->initialized())
    {
        return ks;
    }

    const schema& sc(*m_daemon->m_config.get_schema(ri));

    switch (ks->initialize(&m_daemon->m_data, sc, ri))
    {
        case datalayer::SUCCESS:
        case datalayer::NOT_FOUND:
            return ks;
        case datalayer::BAD_ENCODING:
        case datalayer::CORRUPTION:
        case datalayer::IO_ERROR:
        case datalayer::LEVELDB_ERROR:
        default:
            LOG(ERROR) << "Data layer returned unexpected result when reading old value.";
            return NULL;
    }
}

void
replication_manager :: respond_to_client(const virtual_server_id& us,
                                         const server_id& client,
                                         uint64_t nonce,
                                         network_returncode ret)
{
    size_t sz = HYPERDEX_HEADER_SIZE_VC
              + sizeof(uint64_t)
              + sizeof(uint16_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    uint16_t result = static_cast<uint16_t>(ret);
    msg->pack_at(HYPERDEX_HEADER_SIZE_VC) << nonce << result;
    m_daemon->m_comm.send_client(us, client, RESP_ATOMIC, msg);
}

bool
replication_manager :: send_message(const virtual_server_id& us,
                                    const e::slice& key,
                                    e::intrusive_ptr<key_operation> op)
{
    // If we've sent it somewhere, we shouldn't resend.  If the sender intends a
    // resend, they should clear "sent" first.
    assert(op->sent_to() == virtual_server_id());
    region_id ri(m_daemon->m_config.get_region_id(us));

    // If there's an ongoing transfer, don't actually send
    if (m_daemon->m_config.is_server_blocked_by_live_transfer(m_daemon->m_us, ri))
    {
        return false;
    }

    // facts we use to decide what to do
    assert(ri == op->this_old_region() || ri == op->this_new_region());
    bool last_in_chain = m_daemon->m_config.tail_of_region(ri) == us;
    bool has_next_subspace = op->next_region() != region_id();

    // variables we fill in to determine the message type/destination
    virtual_server_id dest;
    network_msgtype type = CHAIN_OP;

    if (op->this_old_region() == op->this_new_region())
    {
        if (last_in_chain)
        {
            if (has_next_subspace)
            {
                dest = m_daemon->m_config.head_of_region(op->next_region());
                type = CHAIN_OP;
            }
            else
            {
                if (!op->ackable())
                {
                    op->mark_acked();
                    collect(ri, op);
                }

                return send_ack(us, key, op);
            }
        }
        else
        {
            dest = m_daemon->m_config.next_in_region(us);
            type = CHAIN_OP;
        }
    }
    else if (op->this_old_region() == ri)
    {
        if (last_in_chain)
        {
            assert(op->has_value());
            dest = m_daemon->m_config.head_of_region(op->this_new_region());
            type = CHAIN_SUBSPACE;
        }
        else
        {
            dest = m_daemon->m_config.next_in_region(us);
            type = CHAIN_OP;
        }
    }
    else if (op->this_new_region() == ri)
    {
        if (last_in_chain)
        {
            if (has_next_subspace)
            {
                dest = m_daemon->m_config.head_of_region(op->next_region());
                type = CHAIN_OP;
            }
            else
            {
                if (!op->ackable())
                {
                    op->mark_acked();
                    collect(ri, op);
                }

                return send_ack(us, key, op);
            }
        }
        else
        {
            assert(op->has_value());
            dest = m_daemon->m_config.next_in_region(us);
            type = CHAIN_SUBSPACE;
        }
    }
    else
    {
        abort();
    }

    std::auto_ptr<e::buffer> msg;

    if (type == CHAIN_OP)
    {
        uint8_t flags = (op->is_fresh() ? 1 : 0)
                      | (op->has_value() ? 2 : 0);
        size_t sz = HYPERDEX_HEADER_SIZE_VV
                  + sizeof(uint8_t)
                  + sizeof(uint64_t)
                  + sizeof(uint64_t)
                  + pack_size(key)
                  + pack_size(op->value());
        msg.reset(e::buffer::create(sz));
        msg->pack_at(HYPERDEX_HEADER_SIZE_VV)
            << flags << op->prev_version() << op->this_version()
            << key << op->value();
    }
    else if (type == CHAIN_SUBSPACE)
    {
        size_t sz = HYPERDEX_HEADER_SIZE_VV
                  + sizeof(uint64_t)
                  + sizeof(uint64_t)
                  + pack_size(key)
                  + pack_size(op->value())
                  + pack_size(op->prev_region())
                  + pack_size(op->this_old_region())
                  + pack_size(op->this_new_region())
                  + pack_size(op->next_region());
        msg.reset(e::buffer::create(sz));
        msg->pack_at(HYPERDEX_HEADER_SIZE_VV)
            << op->prev_version() << op->this_version()
            << key << op->value()
            << op->prev_region()
            << op->this_old_region()
            << op->this_new_region()
            << op->next_region();
    }
    else
    {
        abort();
    }

    op->set_sent(m_daemon->m_config.version(), dest);
    return m_daemon->m_comm.send_exact(us, dest, type, msg);
}

bool
replication_manager :: send_ack(const virtual_server_id& us,
                                const e::slice& key,
                                e::intrusive_ptr<key_operation> op)
{
    if (!op->ackable() || !op->recv_from(m_daemon->m_config.version()))
    {
        return false;
    }

    size_t sz = HYPERDEX_HEADER_SIZE_VV + sizeof(uint64_t) + pack_size(key);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << op->this_version() << key;
    return m_daemon->m_comm.send_exact(us, op->recv_from(), CHAIN_ACK, msg);
}

void
replication_manager :: retransmit(const std::vector<region_id>& point_leaders,
                                  std::vector<std::pair<region_id, uint64_t> >* versions)
{
    for (key_map_t::iterator it(&m_key_states); it.valid(); ++it)
    {
        key_state* ks = *it;
        region_id ri = ks->state_key().region;

        if (std::binary_search(point_leaders.begin(),
                               point_leaders.end(), ri))
        {
            ks->append_all_versions(versions);
        }

        if (m_daemon->m_config.is_server_blocked_by_live_transfer(m_daemon->m_us, ri))
        {
            continue;
        }

        virtual_server_id us = m_daemon->m_config.get_virtual(ri, m_daemon->m_us);

        if (us == virtual_server_id() || ks->finished())
        {
            ks->reset(&m_daemon->m_gc);
            continue;
        }

        const schema& sc(*m_daemon->m_config.get_schema(ri));
        ks->resend_committable(this, us);
        ks->work_state_machine(this, us, sc);
    }

    m_daemon->m_comm.wake_one();
}

void
replication_manager :: collect(const region_id& ri, e::intrusive_ptr<key_operation> op)
{
    if (op->prev_region() == region_id())
    {
        collect(ri, op->this_version());
    }

    check_stable();
}

void
replication_manager :: collect(const region_id& ri, uint64_t version)
{
    m_idcol.collect(ri, version);
    check_stable(ri);
}

void
replication_manager :: close_gaps(const std::vector<region_id>& point_leaders,
                                  const identifier_generator& peek_ids,
                                  std::vector<std::pair<region_id, uint64_t> >* versions)
{
    std::sort(versions->begin(), versions->end());

    for (size_t i = 0; i < point_leaders.size(); ++i)
    {
        // figure out the start and end ptrs within versions
        // s.t. [start, limit) contains all point_leaders[i]
        std::pair<region_id, uint64_t>* start = &(*versions)[0];
        std::pair<region_id, uint64_t>* const end = start + versions->size();

        while (start < end && start->first == point_leaders[i])
        {
            ++start;
        }

        std::pair<region_id, uint64_t>* limit = start;

        while (limit < end && limit->first == start->first)
        {
            ++limit;
        }

        // find the next ID to generate for point_leaders[i] (cant_touch_this)
        uint64_t cant_touch_this = peek_ids.peek(point_leaders[i]);

        // if start == limit, then there are no versions and everything less than
        // cant_touch_this is collectable and we can short-circuit the
        // complexity below
        if (start == limit)
        {
            m_idcol.bump(point_leaders[i], cant_touch_this);
            continue;
        }

        // find the lower bound for point_leaders[i] (lb)
        uint64_t lb = m_idcol.lower_bound(point_leaders[i]);

        // for every value [lb, cant_touch_this), call collect if not found in
        // versions [start, limit)
        for (uint64_t maybe_next_to_collect = lb;
                maybe_next_to_collect < cant_touch_this; ++maybe_next_to_collect)
        {
            if (!std::binary_search(start, limit,
                        std::make_pair(point_leaders[i], maybe_next_to_collect)))
            {
                m_idcol.collect(point_leaders[i], maybe_next_to_collect);
            }
        }

        // compute the lower bound on point_leaders[i] to squash gaps
        uint64_t tmp = m_idcol.lower_bound(point_leaders[i]);
        (void) tmp;
    }
}

void
replication_manager :: reset_to_unstable()
{
    m_unstable.clear();
    m_daemon->m_config.point_leaders(m_daemon->m_us, &m_unstable);
    check_is_needed();
    m_retransmitter->trigger();
}

void
replication_manager :: check_stable()
{
    bool tell_coord_stable = false;
    uint64_t checkpoint = 0;

    if (is_check_needed())
    {
        po6::threads::mutex::hold hold(&m_protect_stable_stuff);
        tell_coord_stable = m_unstable.empty();
        checkpoint = m_checkpoint;

        if (tell_coord_stable)
        {
            check_is_not_needed();
        }
    }

    if (tell_coord_stable)
    {
        m_daemon->m_coord->config_stable(m_daemon->m_config.version());
        m_daemon->m_coord->checkpoint_report_stable(checkpoint);
    }
}

void
replication_manager :: check_stable(const region_id& ri)
{
    if (!is_check_needed())
    {
        return;
    }

    bool tell_coord_stable = false;
    uint64_t checkpoint = 0;
    uint64_t lb = m_idcol.lower_bound(ri);
    uint64_t stable = m_stable.peek(ri);

    if (stable <= lb)
    {
        po6::threads::mutex::hold hold(&m_protect_stable_stuff);
        bool popped = false;

        for (size_t i = 0; i < m_unstable.size(); )
        {
            if (m_unstable[i] == ri)
            {
                std::swap(m_unstable[i], m_unstable.back());
                m_unstable.pop_back();
                popped = true;
            }
            else
            {
                ++i;
            }
        }

        tell_coord_stable = popped && m_unstable.empty();
        checkpoint = m_checkpoint;

        if (tell_coord_stable)
        {
            check_is_not_needed();
        }
    }

    if (tell_coord_stable)
    {
        m_daemon->m_coord->config_stable(m_daemon->m_config.version());
        m_daemon->m_coord->checkpoint_report_stable(checkpoint);
    }
}

replication_manager :: retransmitter_thread :: retransmitter_thread(daemon* d)
    : background_thread(d)
    , m_rm(&d->m_repl)
    , m_trigger(0)
{
}

replication_manager :: retransmitter_thread :: ~retransmitter_thread() throw ()
{
}

bool
replication_manager :: retransmitter_thread :: have_work()
{
    return m_trigger > 0;
}

void
replication_manager :: retransmitter_thread :: copy_work()
{
    assert(m_trigger > 0);
    --m_trigger;
}

void
replication_manager :: retransmitter_thread :: do_work()
{
    // get the list of point leaders
    std::vector<region_id> point_leaders;
    m_rm->m_daemon->m_config.point_leaders(m_rm->m_daemon->m_us, &point_leaders);
    std::sort(point_leaders.begin(), point_leaders.end());

    // peek at the next-to-generate values of m_idgen
    identifier_generator peeked_values;
    peeked_values.copy_from(m_rm->m_idgen);
    std::vector<std::pair<region_id, uint64_t> > versions;

    // retransmit everything
    m_rm->retransmit(point_leaders, &versions);

    // now close all gaps
    m_rm->close_gaps(point_leaders, peeked_values, &versions);

    for (size_t i = 0; i < point_leaders.size(); ++i)
    {
        m_rm->check_stable(point_leaders[i]);
    }

    m_rm->check_stable();
    m_rm->m_daemon->m_comm.wake_one();
}

void
replication_manager :: retransmitter_thread :: trigger()
{
    this->lock();
    ++m_trigger;
    this->wakeup();
    this->unlock();
}
