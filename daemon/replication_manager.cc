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

// POSIX
#include <signal.h>

// STL
#include <algorithm>

// Google CityHash
#include <city.h>

// Google Log
#include <glog/logging.h>

// e
#include <e/time.h>

// HyperDex
#include "common/datatypes.h"
#include "common/hash.h"
#include "common/serialization.h"
#include "daemon/daemon.h"
#include "daemon/replication_manager.h"
#include "daemon/replication_manager_key_region.h"
#include "daemon/replication_manager_key_state.h"
#include "daemon/replication_manager_key_state_reference.h"
#include "daemon/replication_manager_pending.h"

using hyperdex::reconfigure_returncode;
using hyperdex::replication_manager;

replication_manager :: replication_manager(daemon* d)
    : m_daemon(d)
    , m_key_states_locks(256)
    , m_key_states(16)
    , m_counters()
    , m_shutdown(true)
    , m_retransmitter(std::tr1::bind(&replication_manager::retransmitter, this))
    , m_garbage_collector(std::tr1::bind(&replication_manager::garbage_collector, this))
    , m_block_both()
    , m_wakeup_retransmitter(&m_block_both)
    , m_wakeup_garbage_collector(&m_block_both)
    , m_wakeup_reconfigurer(&m_block_both)
    , m_need_retransmit(false)
    , m_lower_bounds()
    , m_need_pause(false)
    , m_paused_retransmitter(false)
    , m_paused_garbage_collector(false)
{
}

replication_manager :: ~replication_manager() throw ()
{
    shutdown();
}

bool
replication_manager :: setup()
{
    po6::threads::mutex::hold holdr(&m_block_both);
    m_retransmitter.start();
    m_garbage_collector.start();
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
    po6::threads::mutex::hold hold(&m_block_both);
    assert(!m_need_pause);
    m_need_pause = true;
}

void
replication_manager :: unpause()
{
    po6::threads::mutex::hold hold(&m_block_both);
    assert(m_need_pause);
    m_wakeup_retransmitter.broadcast();
    m_wakeup_garbage_collector.broadcast();
    m_need_pause = false;
    m_need_retransmit = true;
}

void
replication_manager :: reconfigure(const configuration&,
                                   const configuration& new_config,
                                   const server_id&)
{
    {
        po6::threads::mutex::hold hold(&m_block_both);
        assert(m_need_pause);

        while (!m_paused_retransmitter || !m_paused_garbage_collector)
        {
            m_wakeup_reconfigurer.wait();
        }
    }

    std::map<uint64_t, uint64_t> seq_ids;
    std::vector<transfer> transfers_in;
    new_config.transfer_in_regions(m_daemon->m_us, &transfers_in);
    std::vector<region_id> transfer_in_regions;
    transfer_in_regions.reserve(transfers_in.size());

    for (size_t i = 0; i < transfers_in.size(); ++i)
    {
        transfer_in_regions.push_back(transfers_in[i].rid);
    }

    std::sort(transfer_in_regions.begin(), transfer_in_regions.end());

    for (key_state_map_t::iterator it = m_key_states.begin();
            it != m_key_states.end(); it.next())
    {
        e::intrusive_ptr<key_state> ks = it.value();
        key_state_reference ksr(this, ks);
        ks->clear_deferred();
        uint64_t max_seq_id = ks->max_seq_id();
        seq_ids[it.key().region.get()] = std::max(seq_ids[it.key().region.get()], max_seq_id);

        if (std::binary_search(transfer_in_regions.begin(), transfer_in_regions.end(), it.key().region))
        {
            m_key_states.remove(it.key());
        }
    }

    std::vector<region_id> regions;
    new_config.point_leaders(m_daemon->m_us, &regions);
    std::sort(regions.begin(), regions.end());
    m_counters.adopt(regions);

    for (size_t i = 0; i < regions.size(); ++i)
    {
        std::map<uint64_t, uint64_t>::iterator it = seq_ids.find(regions[i].get());
        uint64_t non_counter_max = 0;

        if (it == seq_ids.end())
        {
            m_daemon->m_data.max_seq_id(regions[i], &non_counter_max);
            ++non_counter_max;
        }
        else
        {
            non_counter_max = it->second + 1;
        }

        bool found = m_counters.take_max(regions[i], non_counter_max);
        assert(found);
    }
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

    key_state_reference ksr;
    e::intrusive_ptr<key_state> ks = get_or_create_key_state(ri, key, &ksr);
    network_returncode nrc;

    if (!ks->check_against_latest_version(sc, erase, fail_if_not_found, fail_if_found, checks, &nrc))
    {
        respond_to_client(to, from, nonce, nrc);
        return;
    }

    uint64_t seq_id;
    bool found = m_counters.lookup(ri, &seq_id);
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

    key_state_reference ksr;
    e::intrusive_ptr<key_state> ks = get_or_create_key_state(ri, key, &ksr);
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

    key_state_reference ksr;
    e::intrusive_ptr<key_state> ks = get_or_create_key_state(ri, key, &ksr);

    // Create a new pending object to set as pending.
    e::intrusive_ptr<pending> op;
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

    key_state_reference ksr;
    e::intrusive_ptr<key_state> ks = get_key_state(ri, key, &ksr);

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
}

void
replication_manager :: chain_gc(const region_id& reg_id, uint64_t seq_id)
{
    po6::threads::mutex::hold hold(&m_block_both);
    m_wakeup_garbage_collector.broadcast();
    m_lower_bounds.push_back(std::make_pair(reg_id, seq_id));
}

void
replication_manager :: trip_periodic()
{
    po6::threads::mutex::hold hold(&m_block_both);
    m_wakeup_retransmitter.broadcast();
    m_need_retransmit = true;
}

uint64_t
replication_manager :: hash(const key_region& kr)
{
    return CityHash64WithSeed(reinterpret_cast<const char*>(kr.key.data()),
                              kr.key.size(),
                              kr.region.get());
}

uint64_t
replication_manager :: get_lock_num(const region_id& ri,
                                    const e::slice& key)
{
    return CityHash64WithSeed(reinterpret_cast<const char*>(key.data()),
                              key.size(), ri.get());
}

e::intrusive_ptr<replication_manager::key_state>
replication_manager :: get_key_state(const region_id& ri,
                                     const e::slice& key,
                                     key_state_reference* ksr)
{
    key_region kr(ri, key);
    e::intrusive_ptr<key_state> ks;

    while (true)
    {
        e::striped_lock<po6::threads::mutex>::hold hold(&m_key_states_locks,
                get_lock_num(ri, key));

        if (!m_key_states.lookup(kr, &ks))
        {
            return NULL;
        }

        ksr->lock(this, ks);

        if (ks->marked_garbage())
        {
            ksr->unlock();
            continue;
        }

        return ks;
    }
}

e::intrusive_ptr<replication_manager::key_state>
replication_manager :: get_or_create_key_state(const region_id& ri,
                                               const e::slice& key,
                                               key_state_reference* ksr)
{
    key_region kr(ri, key);
    e::intrusive_ptr<key_state> ks;

    while (true)
    {
        {
            e::striped_lock<po6::threads::mutex>::hold hold(&m_key_states_locks,
                    get_lock_num(ri, key));

            if (m_key_states.lookup(kr, &ks))
            {
                ksr->lock(this, ks);

                if (ks->marked_garbage())
                {
                    ksr->unlock();
                    continue;
                }

                assert(ks);
                return ks;
            }

            ks = new key_state(ri, key);
            ksr->lock(this, ks);

            if (!m_key_states.insert(ks->kr(), ks))
            {
                abort();
            }
        }

        switch (ks->initialize(&m_daemon->m_data, ri))
        {
            case datalayer::SUCCESS:
            case datalayer::NOT_FOUND:
                break;
            case datalayer::BAD_ENCODING:
            case datalayer::CORRUPTION:
            case datalayer::IO_ERROR:
            case datalayer::LEVELDB_ERROR:
            default:
                LOG(ERROR) << "Data layer returned unexpected result when reading old value.";
                return NULL;
        }

        return ks;
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

void
replication_manager :: retransmitter()
{
    LOG(INFO) << "retransmitter thread started";
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

    uint64_t then = e::time();

    while (true)
    {
        {
            po6::threads::mutex::hold hold(&m_block_both);

            while ((!m_need_retransmit && !m_shutdown) || m_need_pause)
            {
                m_paused_retransmitter = true;

                if (m_need_pause)
                {
                    m_wakeup_reconfigurer.signal();
                }

                m_wakeup_retransmitter.wait();
                m_paused_retransmitter = false;
            }

            if (m_shutdown)
            {
                break;
            }

            m_need_retransmit = false;
        }

        std::set<region_id> region_cache;
        std::map<region_id, uint64_t> seq_id_lower_bounds;

        {
            m_counters.peek(&seq_id_lower_bounds);
        }

        for (key_state_map_t::iterator it = m_key_states.begin();
                it != m_key_states.end(); it.next())
        {
            region_id ri(it.key().region);

            if (region_cache.find(ri) != region_cache.end() ||
                m_daemon->m_config.is_server_blocked_by_live_transfer(m_daemon->m_us, ri))
            {
                region_cache.insert(ri);
                continue;
            }

            e::slice key(it.key().key.data(), it.key().key.size());
            key_state_reference ksr;
            e::intrusive_ptr<key_state> ks = get_key_state(ri, key, &ksr);

            if (!ks)
            {
                continue;
            }

            virtual_server_id us = m_daemon->m_config.get_virtual(ri, m_daemon->m_us);

            if (us == virtual_server_id())
            {
                m_key_states.remove(it.key());
                continue;
            }

            const schema& sc(*m_daemon->m_config.get_schema(ri));

            if (ks->empty())
            {
                continue;
            }

            ks->resend_committable(this, us);
            ks->move_operations_between_queues(this, us, ri, sc);

            if (m_daemon->m_config.is_point_leader(us))
            {
                uint64_t min_id = ks->min_seq_id();
                std::map<region_id, uint64_t>::iterator lb = seq_id_lower_bounds.find(ri);

                if (lb == seq_id_lower_bounds.end())
                {
                    seq_id_lower_bounds.insert(std::make_pair(ri, min_id));
                }
                else
                {
                    lb->second = std::min(lb->second, min_id);
                }
            }
        }

        m_daemon->m_comm.wake_one();
        uint64_t now = e::time();

        // Rate limit CHAIN_GC sending
        if (((now - then) / 1000 / 1000 / 1000) < 30)
        {
            continue;
        }

        then = now;
        std::vector<std::pair<server_id, po6::net::location> > cluster_members;
        m_daemon->m_config.get_all_addresses(&cluster_members);

        for (std::map<region_id, uint64_t>::iterator it = seq_id_lower_bounds.begin();
                it != seq_id_lower_bounds.end(); ++it)
        {
            // lookup and check again since we lost/acquired the lock
            virtual_server_id us = m_daemon->m_config.get_virtual(it->first, m_daemon->m_us);

            if (us == virtual_server_id() || !m_daemon->m_config.is_point_leader(us))
            {
                continue;
            }

            size_t sz = HYPERDEX_HEADER_SIZE_VV
                      + sizeof(uint64_t);

            for (size_t i = 0; i < cluster_members.size(); ++i)
            {
                std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
                msg->pack_at(HYPERDEX_HEADER_SIZE_VS) << it->second;
                m_daemon->m_comm.send(us, cluster_members[i].first, CHAIN_GC, msg);
            }
        }
    }

    LOG(INFO) << "retransmitter thread shutting down";
}

void
replication_manager :: garbage_collector()
{
    LOG(INFO) << "garbage collector thread started";
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
        std::list<std::pair<region_id, uint64_t> > lower_bounds;

        {
            po6::threads::mutex::hold hold(&m_block_both);

            while ((m_lower_bounds.empty() && !m_shutdown) || m_need_pause)
            {
                m_paused_garbage_collector = true;

                if (m_need_pause)
                {
                    m_wakeup_reconfigurer.signal();
                }

                m_wakeup_garbage_collector.wait();
                m_paused_garbage_collector = false;
            }

            if (m_shutdown)
            {
                break;
            }

            lower_bounds.swap(m_lower_bounds);
        }

        // sort so that we scan disk sequentially
        lower_bounds.sort();

        while (!lower_bounds.empty())
        {
            region_id reg_id = lower_bounds.front().first;
            uint64_t seq_id = lower_bounds.front().second;

            // I chose to use seq_id - 1 for clearing because i'm too tired to check for
            // an off by one.  At worst it'll leave a little extra state laying around,
            // and is guaranteed to be as correct as garbage collecting seq_id.
            if (seq_id > 0)
            {
                m_daemon->m_data.clear_acked(reg_id, seq_id - 1);
            }

            lower_bounds.pop_front();
        }
    }

    LOG(INFO) << "garbage collector thread shutting down";
}

void
replication_manager :: shutdown()
{
    bool is_shutdown;

    {
        po6::threads::mutex::hold holdr(&m_block_both);
        m_wakeup_retransmitter.broadcast();
        m_wakeup_garbage_collector.broadcast();
        is_shutdown = m_shutdown;
        m_shutdown = true;
    }

    if (!is_shutdown)
    {
        m_retransmitter.join();
        m_garbage_collector.join();
    }
}
