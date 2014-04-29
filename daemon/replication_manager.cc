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

// POSIX
#include <signal.h>

// STL
#include <algorithm>

// Google Log
#include <glog/logging.h>

// e
#include <e/time.h>

// HyperDex
#include "common/datatype_info.h"
#include "common/hash.h"
#include "common/serialization.h"
#include "cityhash/city.h"
#include "daemon/daemon.h"
#include "daemon/replication_manager.h"
#include "daemon/replication_manager_key_state.h"
#include "daemon/replication_manager_pending.h"

using po6::threads::make_thread_wrapper;
using hyperdex::reconfigure_returncode;
using hyperdex::replication_manager;

replication_manager :: replication_manager(daemon* d)
    : m_daemon(d)
    , m_key_states(&d->m_gc)
    , m_idgen()
    , m_idcol()
    , m_stable_counters()
    , m_unstable_regions()
    , m_checkpoint(0)
    , m_need_check(1)
    , m_timestamps()
    , m_background_thread(make_thread_wrapper(&replication_manager::background_thread, this))
    , m_block_background_thread()
    , m_wakeup_background_thread(&m_block_background_thread)
    , m_wakeup_reconfigurer(&m_block_background_thread)
    , m_shutdown(true)
    , m_need_pause(false)
    , m_paused(false)
    , m_need_post_reconfigure(false)
    , m_need_periodic(false)
    , m_lower_bounds()
{
    check_is_needed();
}

replication_manager :: ~replication_manager() throw ()
{
    shutdown();
}

bool
replication_manager :: setup()
{
    po6::threads::mutex::hold holdr(&m_block_background_thread);
    m_background_thread.start();
    m_shutdown = false;
    return true;
}

void
replication_manager :: teardown()
{
    shutdown();
}

void
replication_manager :: pause()
{
    po6::threads::mutex::hold hold(&m_block_background_thread);
    assert(!m_need_pause);
    m_need_pause = true;
}

void
replication_manager :: unpause()
{
    po6::threads::mutex::hold hold(&m_block_background_thread);
    assert(m_need_pause);
    m_wakeup_background_thread.broadcast();
    m_need_pause = false;
    m_need_post_reconfigure = true;
}

void
replication_manager :: reconfigure(const configuration&,
                                   const configuration& new_config,
                                   const server_id&)
{
    wait_until_paused();

    std::vector<region_id> key_regions;
    new_config.key_regions(m_daemon->m_us, &key_regions);
    std::sort(key_regions.begin(), key_regions.end());
    m_idgen.adopt(&key_regions[0], key_regions.size());
    m_idcol.adopt(&key_regions[0], key_regions.size());

    // iterate over all key states; cleanup dead ones, and bump idgen
    std::vector<region_id> transfers_in_regions;
    new_config.transfers_in_regions(m_daemon->m_us, &transfers_in_regions);
    std::sort(transfers_in_regions.begin(), transfers_in_regions.end());

    for (key_map_t::iterator it(&m_key_states); it.valid(); ++it)
    {
        key_state* ks = *it;
        ks->clear_deferred();

        if (std::binary_search(transfers_in_regions.begin(),
                               transfers_in_regions.end(),
                               ks->state_key().region))
        {
            ks->clear();
        }

        if (std::binary_search(key_regions.begin(),
                               key_regions.end(),
                               ks->state_key().region))
        {
            bool x;
            x = m_idgen.bump(ks->state_key().region, ks->max_seq_id());
            assert(x);
        }
    }

    // iterate over all regions on disk/lb, and bump idgen
    for (size_t i = 0; i < key_regions.size(); ++i)
    {
        // from disk
        uint64_t seq_id = 0;
        m_daemon->m_data.max_seq_id(key_regions[i], &seq_id);
        bool x;
        x = m_idgen.bump(key_regions[i], seq_id);
        assert(x);
        // from lower bound
        x = m_idcol.lower_bound(key_regions[i], &seq_id);
        assert(x);

        if (seq_id > 0)
        {
            x = m_idgen.bump(key_regions[i], seq_id - 1);
            assert(x);
        }
    }

    // figure out when we're stable
    m_stable_counters.copy_from(m_idgen);
    m_unstable_regions.clear();
    new_config.point_leaders(m_daemon->m_us, &m_unstable_regions);
    check_is_needed();
}

void
replication_manager :: debug_dump()
{
    pause();
    wait_until_paused();
    std::vector<region_id> regions;
    m_daemon->m_config.key_regions(m_daemon->m_us, &regions);

    // print counters
    LOG(INFO) << "region counters ===============================================================";

    for (size_t i = 0; i < regions.size(); ++i)
    {
        bool x;
        uint64_t idgen;
        x = m_idgen.peek(regions[i], &idgen);
        assert(x);
        uint64_t lb;
        x = m_idcol.lower_bound(regions[i], &lb);
        assert(x);
        uint64_t stable;
        x = m_stable_counters.peek(regions[i], &stable);
        assert(x);
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

    unpause();
}

void
replication_manager :: client_atomic(const server_id& from,
                                     const virtual_server_id& to,
                                     uint64_t nonce,
                                     bool erase,
                                     bool fail_if_not_found,
                                     bool fail_if_found,
                                     const e::slice& key,
                                     const std::vector<attribute_check>& checks,
                                     const std::vector<funcall>& funcs)
{
    if (m_daemon->m_config.read_only())
    {
        respond_to_client(to, from, nonce, NET_READONLY);
        return;
    }

    const region_id ri(m_daemon->m_config.get_region_id(to));
    const schema& sc(*m_daemon->m_config.get_schema(ri));

    if (!datatype_info::lookup(sc.attrs[0].type)->validate(key) ||
        validate_attribute_checks(sc, checks) != checks.size() ||
        validate_funcs(sc, funcs) != funcs.size() ||
        (erase && !funcs.empty()))
    {
        LOG(ERROR) << "dropping nonce=" << nonce << " from client=" << from
                   << " because the key, checks, or funcs don't validate";
        respond_to_client(to, from, nonce, NET_BADDIMSPEC);
        return;
    }

    if (m_daemon->m_config.point_leader(ri, key) != to)
    {
        LOG(ERROR) << "dropping nonce=" << nonce << " from client=" << from
                   << " because it doesn't map to " << ri;
        respond_to_client(to, from, nonce, NET_NOTUS);
        return;
    }

    key_map_t::state_reference ksr;
    key_state* ks = get_or_create_key_state(ri, key, &ksr);
    network_returncode nrc;

    if (!ks->check_against_latest_version(sc, erase, fail_if_not_found, fail_if_found, checks, &nrc))
    {
        respond_to_client(to, from, nonce, nrc);
        return;
    }

    uint64_t seq_id;
    bool found = m_idgen.generate_id(ri, &seq_id);
    assert(found);

    if (erase)
    {
        ks->delete_latest(sc, ri, seq_id, from, nonce);
    }
    else
    {
        if (!ks->put_from_funcs(sc, ri, seq_id, funcs, from, nonce))
        {
            respond_to_client(to, from, nonce, NET_OVERFLOW);
            return;
        }
    }

    ks->move_operations_between_queues(this, to, ri, sc);
}

void
replication_manager :: chain_op(const virtual_server_id& from,
                                const virtual_server_id& to,
                                bool retransmission,
                                const region_id& reg_id,
                                uint64_t seq_id,
                                uint64_t version,
                                bool fresh,
                                bool has_value,
                                std::auto_ptr<e::buffer> backing,
                                const e::slice& key,
                                const std::vector<e::slice>& value)
{
    const region_id ri(m_daemon->m_config.get_region_id(to));
    const schema& sc(*m_daemon->m_config.get_schema(ri));

    if (retransmission && m_daemon->m_data.check_acked(ri, reg_id, seq_id))
    {
        send_ack(to, from, true, reg_id, seq_id, version, key);
        return;
    }

    bool valid = sc.attrs_sz > 0 &&
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
    e::intrusive_ptr<pending> op = ks->get_version(version);

    if (op)
    {
        op->recv_config_version = m_daemon->m_config.version();
        op->recv = from;

        if (op->acked)
        {
            send_ack(to, from, false, reg_id, seq_id, version, key);
        }

        return;
    }

    op = new pending(backing,
                     reg_id, seq_id, fresh,
                     has_value, value,
                     server_id(), 0,
                     m_daemon->m_config.version(), from);
    ks->insert_deferred(version, op);
    ks->move_operations_between_queues(this, to, ri, sc);
}

void
replication_manager :: chain_subspace(const virtual_server_id& from,
                                      const virtual_server_id& to,
                                      bool retransmission,
                                      const region_id& reg_id,
                                      uint64_t seq_id,
                                      uint64_t version,
                                      std::auto_ptr<e::buffer> backing,
                                      const e::slice& key,
                                      const std::vector<e::slice>& value,
                                      const std::vector<uint64_t>& hashes)
{
    const region_id ri(m_daemon->m_config.get_region_id(to));
    const schema& sc(*m_daemon->m_config.get_schema(ri));

    if (retransmission && m_daemon->m_data.check_acked(ri, reg_id, seq_id))
    {
        send_ack(to, from, true, reg_id, seq_id, version, key);
        return;
    }

    bool valid = sc.attrs_sz == value.size() + 1 &&
                 sc.attrs_sz == hashes.size() &&
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

    // Create a new pending object to set as pending.
    e::intrusive_ptr<pending> op = ks->get_version(version);

    if (op)
    {
        op->recv_config_version = m_daemon->m_config.version();
        op->recv = from;

        if (op->acked)
        {
            send_ack(to, from, false, reg_id, seq_id, version, key);
        }

        return;
    }

    op = new pending(backing,
                     reg_id, seq_id, false,
                     true, value,
                     server_id(), 0,
                     m_daemon->m_config.version(), from);
    op->old_hashes.resize(sc.attrs_sz);
    op->new_hashes.resize(sc.attrs_sz);
    op->this_old_region = region_id();
    op->this_new_region = region_id();
    op->prev_region = region_id();
    op->next_region = region_id();
    subspace_id subspace_this = m_daemon->m_config.subspace_of(ri);
    subspace_id subspace_prev = m_daemon->m_config.subspace_prev(subspace_this);
    subspace_id subspace_next = m_daemon->m_config.subspace_next(subspace_this);
    op->old_hashes = hashes;
    hyperdex::hash(sc, key, value, &op->new_hashes.front());

    if (subspace_prev != subspace_id())
    {
        m_daemon->m_config.lookup_region(subspace_prev, op->new_hashes, &op->prev_region);
    }

    m_daemon->m_config.lookup_region(subspace_this, op->old_hashes, &op->this_old_region);
    m_daemon->m_config.lookup_region(subspace_this, op->new_hashes, &op->this_new_region);

    if (subspace_next != subspace_id())
    {
        m_daemon->m_config.lookup_region(subspace_next, op->old_hashes, &op->next_region);
    }

    if (!(op->this_old_region == m_daemon->m_config.get_region_id(from) &&
          m_daemon->m_config.tail_of_region(op->this_old_region) == from) &&
        !(op->this_new_region == m_daemon->m_config.get_region_id(from) &&
          m_daemon->m_config.next_in_region(from) == to))
    {
        LOG(ERROR) << "dropping CHAIN_SUBSPACE which didn't obey chaining rules";
        return;
    }

    ks->insert_deferred(version, op);
    ks->move_operations_between_queues(this, to, ri, sc);
}

void
replication_manager :: chain_ack(const virtual_server_id& from,
                                 const virtual_server_id& to,
                                 bool retransmission,
                                 const region_id& reg_id,
                                 uint64_t seq_id,
                                 uint64_t version,
                                 const e::slice& key)
{
    const region_id ri(m_daemon->m_config.get_region_id(to));
    const schema& sc(*m_daemon->m_config.get_schema(ri));

    if (retransmission && m_daemon->m_data.check_acked(ri, reg_id, seq_id))
    {
        return;
    }

    key_map_t::state_reference ksr;
    key_state* ks = get_key_state(ri, key, &ksr);

    if (!ks)
    {
        LOG(ERROR) << "dropping CHAIN_ACK for update we haven't seen";
        return;
    }

    e::intrusive_ptr<pending> op = ks->get_version(version);

    if (!op)
    {
        LOG(INFO) << "dropping CHAIN_ACK for update we haven't seen";
        return;
    }

    if (op->sent == virtual_server_id() ||
        from != op->sent ||
        m_daemon->m_config.version() != op->sent_config_version)
    {
        LOG(ERROR) << "dropping CHAIN_ACK that came from " << from
                   << " in version " << m_daemon->m_config.version()
                   << " but should have come from " << op->sent
                   << " in version " << op->sent_config_version;
        return;
    }

    if (op->reg_id != reg_id || op->seq_id != seq_id)
    {
        LOG(ERROR) << "dropping CHAIN_ACK that was sent with mismatching reg/seq ids";
        return;
    }

    op->acked = true;
    bool is_head = m_daemon->m_config.head_of_region(ri) == to;

    if (!is_head && m_daemon->m_config.version() == op->recv_config_version)
    {
        send_ack(to, op->recv, false, reg_id, seq_id, version, key);
    }

    if (!ks->persist_to_datalayer(this, ri, reg_id, seq_id, version))
    {
        LOG(ERROR) << "commit encountered unrecoverable error";
        return;
    }

    ks->clear_acked_prefix();
    ks->move_operations_between_queues(this, to, ri, sc);

    if (op->client != server_id())
    {
        respond_to_client(to, op->client, op->nonce, NET_SUCCESS);
    }

    if (is_head && m_daemon->m_config.version() == op->recv_config_version)
    {
        send_ack(to, op->recv, false, reg_id, seq_id, version, key);
    }

    if (op->reg_id == ri)
    {
        bool x;
        x = m_idcol.collect(ri, op->seq_id);
        assert(x);
        check_stable(ri);
    }
}

void
replication_manager :: chain_gc(const region_id& reg_id, uint64_t seq_id)
{
    po6::threads::mutex::hold hold(&m_block_background_thread);
    m_wakeup_background_thread.broadcast();
    m_lower_bounds.push_back(std::make_pair(reg_id, seq_id));
}

void
replication_manager :: trip_periodic()
{
    po6::threads::mutex::hold hold(&m_block_background_thread);
    m_wakeup_background_thread.broadcast();
    m_need_periodic = true;
}

void
replication_manager :: begin_checkpoint(uint64_t seq)
{
    std::vector<region_id> key_regions;
    std::vector<region_id> mapped_regions;
    m_daemon->m_coord.config().key_regions(m_daemon->m_us, &key_regions);
    m_daemon->m_coord.config().mapped_regions(m_daemon->m_us, &mapped_regions);
    std::string timestamp = m_daemon->m_data.get_timestamp();

    {
        po6::threads::mutex::hold hold(&m_block_background_thread);

        if (m_checkpoint >= seq)
        {
            return;
        }

        m_checkpoint = seq;
        m_unstable_regions.clear();
        m_daemon->m_config.point_leaders(m_daemon->m_us, &m_unstable_regions);
        check_is_needed();

        for (size_t i = 0; i < mapped_regions.size(); ++i)
        {
            m_timestamps.push_back(region_timestamp(mapped_regions[i], m_checkpoint, timestamp));
        }
    }

    for (size_t i = 0; i < key_regions.size(); ++i)
    {
        bool x;
        uint64_t id = 0;
        x = m_idgen.peek(key_regions[i], &id);
        assert(x);

        if (id > 0)
        {
            x = m_stable_counters.bump(key_regions[i], id - 1);
            assert(x);
        }

        check_stable(key_regions[i]);
    }

    check_stable();
}

void
replication_manager :: end_checkpoint(uint64_t seq)
{
    po6::threads::mutex::hold hold(&m_block_background_thread);

    for (size_t i = 0; i < m_timestamps.size(); )
    {
        if (m_timestamps[i].checkpoint <= seq)
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
}

replication_manager::key_state*
replication_manager :: get_key_state(const region_id& ri,
                                     const e::slice& key,
                                     key_map_t::state_reference* ksr)
{
    key_region kr(ri, key);
    return m_key_states.get_state(kr, ksr);
}

replication_manager::key_state*
replication_manager :: get_or_create_key_state(const region_id& ri,
                                               const e::slice& key,
                                               key_map_t::state_reference* ksr)
{
    key_region kr(ri, key);
    key_state* ks = m_key_states.get_or_create_state(kr, ksr);

    if (!ks || ks->initialized())
    {
        return ks;
    }

    switch (ks->initialize(&m_daemon->m_data, ri))
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
replication_manager :: send_message(const virtual_server_id& us,
                                    bool retransmission,
                                    uint64_t version,
                                    const e::slice& key,
                                    e::intrusive_ptr<pending> op)
{
    // If we've sent it somewhere, we shouldn't resend.  If the sender intends a
    // resend, they should clear "sent" first.
    assert(op->sent == virtual_server_id());
    region_id ri(m_daemon->m_config.get_region_id(us));

    // If there's an ongoing transfer, don't actually send
    if (m_daemon->m_config.is_server_blocked_by_live_transfer(m_daemon->m_us, ri))
    {
        return;
    }

    // facts we use to decide what to do
    assert(ri == op->this_old_region || ri == op->this_new_region);
    bool last_in_chain = m_daemon->m_config.tail_of_region(ri) == us;
    bool has_next_subspace = op->next_region != region_id();

    // variables we fill in to determine the message type/destination
    virtual_server_id dest;
    network_msgtype type = CHAIN_OP;

    if (op->this_old_region == op->this_new_region)
    {
        if (last_in_chain)
        {
            if (has_next_subspace)
            {
                dest = m_daemon->m_config.head_of_region(op->next_region);
                type = type; // it stays the same
            }
            else
            {
                dest = us;
                type = CHAIN_ACK;
            }
        }
        else
        {
            dest = m_daemon->m_config.next_in_region(us);
            type = type; // it stays the same
        }
    }
    else if (op->this_old_region == ri)
    {
        if (last_in_chain)
        {
            assert(op->has_value);
            dest = m_daemon->m_config.head_of_region(op->this_new_region);
            type = CHAIN_SUBSPACE;
        }
        else
        {
            dest = m_daemon->m_config.next_in_region(us);
            type = type; // it stays the same
        }
    }
    else if (op->this_new_region == ri)
    {
        if (last_in_chain)
        {
            if (has_next_subspace)
            {
                dest = m_daemon->m_config.head_of_region(op->next_region);
                type = type; // it stays the same
            }
            else
            {
                dest = us;
                type = CHAIN_ACK;
            }
        }
        else
        {
            assert(op->has_value);
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
        uint8_t flags = (op->fresh ? 1 : 0)
                      | (op->has_value ? 2 : 0)
                      | (retransmission ? 128 : 0);
        size_t sz = HYPERDEX_HEADER_SIZE_VV
                  + sizeof(uint8_t)
                  + sizeof(uint64_t)
                  + sizeof(uint64_t)
                  + sizeof(uint64_t)
                  + sizeof(uint32_t)
                  + key.size()
                  + pack_size(op->value);
        msg.reset(e::buffer::create(sz));
        msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << flags << op->reg_id.get() << op->seq_id << version << key << op->value;
    }
    else if (type == CHAIN_ACK)
    {
        uint8_t flags = (retransmission ? 128 : 0);
        size_t sz = HYPERDEX_HEADER_SIZE_VV
                  + sizeof(uint8_t)
                  + sizeof(uint64_t)
                  + sizeof(uint64_t)
                  + sizeof(uint64_t)
                  + sizeof(uint32_t)
                  + key.size();
        msg.reset(e::buffer::create(sz));
        msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << flags << op->reg_id.get() << op->seq_id << version << key;
    }
    else if (type == CHAIN_SUBSPACE)
    {
        uint8_t flags = (retransmission ? 128 : 0);
        size_t sz = HYPERDEX_HEADER_SIZE_VV
                  + sizeof(uint8_t)
                  + sizeof(uint64_t)
                  + sizeof(uint64_t)
                  + sizeof(uint64_t)
                  + sizeof(uint32_t)
                  + key.size()
                  + pack_size(op->value)
                  + pack_size(op->old_hashes);
        msg.reset(e::buffer::create(sz));
        msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << flags << op->reg_id.get() << op->seq_id << version << key << op->value << op->old_hashes;
    }
    else
    {
        abort();
    }

    op->sent_config_version = m_daemon->m_config.version();
    op->sent = dest;
    m_daemon->m_comm.send_exact(us, dest, type, msg);
}

bool
replication_manager :: send_ack(const virtual_server_id& us,
                                const virtual_server_id& to,
                                bool retransmission,
                                const region_id& reg_id,
                                uint64_t seq_id,
                                uint64_t version,
                                const e::slice& key)
{
    uint8_t flags = (retransmission ? 128 : 0);
    size_t sz = HYPERDEX_HEADER_SIZE_VV
              + sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint64_t)
              + sizeof(uint64_t)
              + sizeof(uint32_t)
              + key.size();
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << flags << reg_id.get() << seq_id << version << key;
    return m_daemon->m_comm.send_exact(us, to, CHAIN_ACK, msg);
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
replication_manager :: is_check_needed()
{
    return e::atomic::compare_and_swap_32_nobarrier(&m_need_check, 0, 0) == 1;
}

void
replication_manager :: check_is_needed()
{
    e::atomic::compare_and_swap_32_nobarrier(&m_need_check, 0, 1);
}

void
replication_manager :: check_is_not_needed()
{
    e::atomic::compare_and_swap_32_nobarrier(&m_need_check, 1, 0);
}

void
replication_manager :: check_stable()
{
    bool tell_coord_stable = false;
    uint64_t checkpoint = 0;

    if (is_check_needed())
    {
        po6::threads::mutex::hold hold(&m_block_background_thread);
        tell_coord_stable = m_unstable_regions.empty();
        checkpoint = m_checkpoint;

        if (tell_coord_stable)
        {
            check_is_not_needed();
        }
    }

    if (tell_coord_stable)
    {
        m_daemon->m_coord.config_stable(m_daemon->m_coord.config().version());
        m_daemon->m_coord.checkpoint_report_stable(checkpoint);
    }
}

void
replication_manager :: check_stable(const region_id& ri)
{
    bool x;
    uint64_t lb = 0;
    x = m_idcol.lower_bound(ri, &lb);
    assert(x);
    uint64_t stable = 0;
    x = m_stable_counters.peek(ri, &stable);
    assert(x);
    bool tell_coord_stable = false;
    uint64_t checkpoint = 0;

    if (stable <= lb && is_check_needed())
    {
        po6::threads::mutex::hold hold(&m_block_background_thread);

        for (size_t i = 0; i < m_unstable_regions.size(); )
        {
            if (m_unstable_regions[i] == ri)
            {
                std::swap(m_unstable_regions[i], m_unstable_regions.back());
                m_unstable_regions.pop_back();
            }
            else
            {
                ++i;
            }
        }

        tell_coord_stable = m_unstable_regions.empty();
        checkpoint = m_checkpoint;

        if (tell_coord_stable)
        {
            check_is_not_needed();
        }
    }

    if (tell_coord_stable)
    {
        m_daemon->m_coord.config_stable(m_daemon->m_coord.config().version());
        m_daemon->m_coord.checkpoint_report_stable(checkpoint);
    }
}

void
replication_manager :: wait_until_paused()
{
    po6::threads::mutex::hold hold(&m_block_background_thread);
    assert(m_need_pause);

    while (!m_paused)
    {
        m_wakeup_reconfigurer.wait();
    }
}

void
replication_manager :: background_thread()
{
    LOG(INFO) << "replication background thread started";
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

    e::garbage_collector::thread_state gc_ts;
    m_daemon->m_gc.register_thread(&gc_ts);

    while (true)
    {
        bool need_post_reconfigure = false;
        bool need_periodic = false;
        region_id reg_id;
        uint64_t seq_id = 0;

        {
            m_daemon->m_gc.quiescent_state(&gc_ts);
            po6::threads::mutex::hold hold(&m_block_background_thread);

            while ((!(m_need_post_reconfigure ||
                      m_need_periodic ||
                      !m_lower_bounds.empty()) && !m_shutdown) ||
                   m_need_pause)
            {
                m_paused = true;

                if (m_need_pause)
                {
                    m_wakeup_reconfigurer.signal();
                }

                m_daemon->m_gc.offline(&gc_ts);
                m_wakeup_background_thread.wait();
                m_daemon->m_gc.online(&gc_ts);
                m_paused = false;
            }

            if (m_shutdown)
            {
                break;
            }

            need_post_reconfigure = m_need_post_reconfigure;
            m_need_post_reconfigure = false;
            need_periodic = m_need_periodic;
            m_need_periodic = false;

            if (!m_lower_bounds.empty())
            {
                reg_id = m_lower_bounds.front().first;
                seq_id = m_lower_bounds.front().second;
                m_lower_bounds.pop_front();
            }
        }

        if (need_post_reconfigure)
        {
            // get the list of point leaders
            std::vector<region_id> point_leaders;
            m_daemon->m_coord.config().point_leaders(m_daemon->m_us, &point_leaders);
            std::sort(point_leaders.begin(), point_leaders.end());

            // peek at the next-to-generate values of m_idgen
            identifier_generator peeked_values;
            peeked_values.copy_from(m_idgen);
            std::vector<std::pair<region_id, uint64_t> > seq_ids;

            // retransmit everything
            retransmit(point_leaders, &seq_ids);

            // now close all gaps
            close_gaps(point_leaders, peeked_values, &seq_ids);

            for (size_t i = 0; i < point_leaders.size(); ++i)
            {
                check_stable(point_leaders[i]);
            }

            check_stable();
        }

        if (need_periodic)
        {
            send_chain_gc();
        }

        if (seq_id > 1)
        {
            bool x;
            x = m_idcol.bump(reg_id, seq_id - 1);
            m_daemon->m_data.clear_acked(reg_id, seq_id - 2);

            if (x)
            {
                check_stable(reg_id);
            }
        }
    }

    m_daemon->m_gc.deregister_thread(&gc_ts);
    LOG(INFO) << "replication background thread shutting down";
}

void
replication_manager :: send_chain_gc()
{
    std::vector<std::pair<server_id, po6::net::location> > cluster_members;
    m_daemon->m_config.get_all_addresses(&cluster_members);
    std::vector<region_id> regions;
    m_daemon->m_config.point_leaders(m_daemon->m_us, &regions);

    for (size_t i = 0; i < regions.size(); ++i)
    {
        for (size_t j = 0; j < cluster_members.size(); ++j)
        {
            virtual_server_id us = m_daemon->m_config.get_virtual(regions[i], m_daemon->m_us);
            uint64_t lb = 0;
            bool x = m_idcol.lower_bound(regions[i], &lb);

            if (!x || lb == 0 || us == virtual_server_id())
            {
                continue;
            }

            size_t sz = HYPERDEX_HEADER_SIZE_VS + sizeof(uint64_t);
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(HYPERDEX_HEADER_SIZE_VS) << lb;
            m_daemon->m_comm.send(us, cluster_members[j].first, CHAIN_GC, msg);
        }
    }

    m_daemon->m_comm.wake_one();
}

void
replication_manager :: retransmit(const std::vector<region_id>& point_leaders,
                                  std::vector<std::pair<region_id, uint64_t> >* seq_ids)
{
    for (key_map_t::iterator it(&m_key_states); it.valid(); ++it)
    {
        key_state* ks = *it;
        region_id ri = ks->state_key().region;

        if (std::binary_search(point_leaders.begin(),
                               point_leaders.end(), ri))
        {
            ks->append_seq_ids(seq_ids);
        }

        if (m_daemon->m_config.is_server_blocked_by_live_transfer(m_daemon->m_us, ri))
        {
            continue;
        }

        virtual_server_id us = m_daemon->m_config.get_virtual(ri, m_daemon->m_us);

        if (us == virtual_server_id() || ks->empty())
        {
            ks->clear();
            continue;
        }

        const schema& sc(*m_daemon->m_config.get_schema(ri));
        ks->resend_committable(this, us);
        ks->move_operations_between_queues(this, us, ri, sc);
    }

    m_daemon->m_comm.wake_one();
}

void
replication_manager :: close_gaps(const std::vector<region_id>& point_leaders,
                                  const identifier_generator& peek_ids,
                                  std::vector<std::pair<region_id, uint64_t> >* seq_ids)
{
    std::sort(seq_ids->begin(), seq_ids->end());

    for (size_t i = 0; i < point_leaders.size(); ++i)
    {
        // figure out the start and end ptrs within seq_ids
        // s.t. [start, limit) contains all point_leaders[i]
        std::pair<region_id, uint64_t>* start = &(*seq_ids)[0];
        std::pair<region_id, uint64_t>* const end = start + seq_ids->size();

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
        bool x;
        uint64_t cant_touch_this;
        x = peek_ids.peek(point_leaders[i], &cant_touch_this);
        assert(x);

        // if start == limit, then there are no seq_ids and everything less than
        // cant_touch_this is collectable and we can short-circuit the
        // complexity below
        if (start == limit)
        {
            m_idcol.bump(point_leaders[i], cant_touch_this);
            continue;
        }

        // find the lower bound for point_leaders[i] (lb)
        uint64_t lb;
        x = m_idcol.lower_bound(point_leaders[i], &lb);
        assert(x);

        // for every value [lb, cant_touch_this), call collect if not found in
        // seq_ids [start, limit)
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
        uint64_t tmp;
        m_idcol.lower_bound(point_leaders[i], &tmp);
    }
}

void
replication_manager :: shutdown()
{
    bool is_shutdown;

    {
        po6::threads::mutex::hold holdr(&m_block_background_thread);
        m_wakeup_background_thread.broadcast();
        is_shutdown = m_shutdown;
        m_shutdown = true;
    }

    if (!is_shutdown)
    {
        m_background_thread.join();
    }
}
