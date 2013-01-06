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

// Google CityHash
#include <city.h>

// Google Log
#include <glog/logging.h>

// HyperDex
#include "common/hash.h"
#include "common/serialization.h"
#include "datatypes/apply.h"
#include "datatypes/validate.h"
#include "daemon/daemon.h"
#include "daemon/replication_manager.h"
#include "daemon/replication_manager_keyholder.h"
#include "daemon/replication_manager_keypair.h"
#include "daemon/replication_manager_pending.h"

using hyperdex::reconfigure_returncode;
using hyperdex::replication_manager;

#define _CONCAT(x, y) x ## y
#define CONCAT(x, y) _CONCAT(x, y)

// This macro should be used in the body of non-static members to hold the
// appropriate lock for the request.  E should be an entity whose region the key
// resides in.  K is the key for the object being protected.
#define HOLD_LOCK_FOR_KEY(R, K) \
    e::striped_lock<po6::threads::mutex>::hold CONCAT(_anon, __LINE__)(&m_keyholder_locks, get_lock_num(R, K))
#define CLEANUP_KEYHOLDER(R, K, KH) \
    if (kh->empty()) \
    { \
        erase_keyholder(ri, key); \
    }

replication_manager :: replication_manager(daemon* d)
    : m_daemon(d)
    , m_keyholder_locks(1024)
    , m_keyholders(16)
    , m_retransmitter(std::tr1::bind(&replication_manager::retransmitter, this))
    , m_block_retransmitter()
    , m_wakeup_retransmitter(&m_block_retransmitter)
    , m_need_retransmit(false)
    , m_shutdown(true)
    , m_counters()
{
}

replication_manager :: ~replication_manager() throw ()
{
    shutdown();
}

bool
replication_manager :: setup()
{
    m_retransmitter.start();
    m_shutdown = false;
    return true;
}

void
replication_manager :: teardown()
{
    shutdown();
}

reconfigure_returncode
replication_manager :: prepare(const configuration&,
                               const configuration&,
                               const server_id&)
{
    m_block_retransmitter.lock();
    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
replication_manager :: reconfigure(const configuration&,
                                   const configuration& new_config,
                                   const server_id&)
{
    std::map<uint64_t, uint64_t> seq_ids;

    for (keyholder_map_t::iterator it = m_keyholders.begin();
            it != m_keyholders.end(); it.next())
    {
        region_id ri(it.key().region);
        e::slice key(it.key().key.data(), it.key().key.size());
        HOLD_LOCK_FOR_KEY(ri, key);
        e::intrusive_ptr<keyholder> kh = it.value();
        kh->clear_deferred();
        uint64_t max_seq_id = kh->max_seq_id();
        seq_ids[ri.get()] = std::max(seq_ids[ri.get()], max_seq_id);
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

    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
replication_manager :: cleanup(const configuration&,
                               const configuration&,
                               const server_id&)
{
    m_wakeup_retransmitter.broadcast();
    m_need_retransmit = true;
    m_block_retransmitter.unlock();
    return RECONFIGURE_SUCCESS;
}

void
replication_manager :: client_atomic(const server_id& from,
                                     const virtual_server_id& to,
                                     uint64_t nonce,
                                     bool fail_if_not_found,
                                     bool fail_if_found,
                                     bool erase,
                                     const e::slice& key,
                                     std::vector<attribute_check>* checks,
                                     std::vector<funcall>* funcs)
{
    region_id ri(m_daemon->m_config.get_region_id(to));
    const schema* sc = m_daemon->m_config.get_schema(ri);

    if (!validate_as_type(key, sc->attrs[0].type))
    {
        respond_to_client(to, from, nonce, NET_BADDIMSPEC);
        return;
    }

    // Make sure this message is to the point-leader.
    if (!m_daemon->m_config.is_point_leader(to))
    {
        respond_to_client(to, from, nonce, NET_NOTUS);
        return;
    }

    HOLD_LOCK_FOR_KEY(ri, key);
    e::intrusive_ptr<keyholder> kh = get_or_create_keyholder(ri, key);
    bool has_old_value = false;
    uint64_t old_version = 0;
    std::vector<e::slice>* old_value = NULL; // don't use pointer?
    kh->get_latest_version(&has_old_value, &old_version, &old_value);

    if (erase && !funcs->empty())
    {
        respond_to_client(to, from, nonce, NET_BADDIMSPEC);
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    if (!has_old_value && erase)
    {
        respond_to_client(to, from, nonce, NET_NOTFOUND);
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    if ((has_old_value && fail_if_found) ||
        (!has_old_value && fail_if_not_found))
    {
        respond_to_client(to, from, nonce, NET_CMPFAIL);
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    if (has_old_value && old_value->size() + 1 != sc->attrs_sz)
    {
        LOG(ERROR) << "received a corrupt object"; // XXX don't leave the client hanging
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    std::vector<e::slice> tmp_value;

    if (!old_value)
    {
        old_value = &tmp_value;
    }

    old_value->resize(sc->attrs_sz - 1);

    // Create a new version of the object in a contiguous buffer using the old
    // version and the funcalls.
    std::tr1::shared_ptr<e::buffer> backing;
    std::vector<e::slice> new_value;
    microerror error;
    size_t passed = perform_checks_and_apply_funcs(sc, *checks, *funcs, key, *old_value,
                                                   &backing, &new_value, &error);

    if (passed != checks->size() + funcs->size())
    {
        /* XXX say why */
        respond_to_client(to, from, nonce, error == MICROERR_OVERFLOW ? NET_OVERFLOW : NET_CMPFAIL);
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    bool has_new_value = !erase;
    uint64_t seq_id;
    bool found = m_counters.lookup(ri, &seq_id);
    assert(found);

    e::intrusive_ptr<pending> new_pend(new pending(backing, ri, seq_id, !has_old_value && has_new_value, has_new_value, new_value, from, nonce));
    hash_objects(ri, *sc, key, has_new_value, new_value, has_old_value, *old_value, new_pend);

    if (new_pend->this_old_region != ri && new_pend->this_new_region != ri)
    {
        respond_to_client(to, from, nonce, NET_NOTUS);
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    assert(!kh->has_deferred_ops());
    kh->insert_deferred(old_version + 1, new_pend);
    move_operations_between_queues(to, ri, *sc, key, kh);
    assert(!kh->has_deferred_ops());
    CLEANUP_KEYHOLDER(ri, key, kh);
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
    region_id ri(m_daemon->m_config.get_region_id(to));
    const schema* sc = m_daemon->m_config.get_schema(ri);
    HOLD_LOCK_FOR_KEY(ri, key);
    e::intrusive_ptr<keyholder> kh = get_or_create_keyholder(ri, key);

    if (reg_id != ri)
    {
        LOG(ERROR) << "dropping CHAIN_OP send to the wrong region";
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    if (retransmission && m_daemon->m_data.check_acked(reg_id, seq_id))
    {
        LOG(INFO) << "acking duplicate CHAIN_*";
        send_ack(to, from, true, reg_id, seq_id, version, key);
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    // Check that a chain's put matches the dimensions of the space.
    if (has_value && sc->attrs_sz != value.size() + 1)
    {
        LOG(INFO) << "dropping CHAIN_* because the dimensions are incorrect";
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    e::intrusive_ptr<pending> new_op = kh->get_by_version(version);

    if (new_op)
    {
        new_op->recv_config_version = m_daemon->m_config.version();
        new_op->recv = from;

        if (new_op->acked)
        {
            send_ack(to, from, false, reg_id, seq_id, version, key);
        }

        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    if (version <= kh->version_on_disk())
    {
        send_ack(to, from, false, reg_id, seq_id, version, key);
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    std::tr1::shared_ptr<e::buffer> new_backing(backing.release());
    e::intrusive_ptr<pending> new_defer(new pending(new_backing, reg_id, seq_id, fresh, has_value, value, m_daemon->m_config.version(), from));
    kh->insert_deferred(version, new_defer);
    move_operations_between_queues(to, ri, *sc, key, kh);
    CLEANUP_KEYHOLDER(ri, key, kh);
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
    region_id ri(m_daemon->m_config.get_region_id(to));
    const schema* sc = m_daemon->m_config.get_schema(ri);
    HOLD_LOCK_FOR_KEY(ri, key);
    e::intrusive_ptr<keyholder> kh = get_or_create_keyholder(ri, key);

    if (reg_id != ri)
    {
        LOG(ERROR) << "dropping CHAIN_OP send to the wrong region";
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    if (retransmission && m_daemon->m_data.check_acked(reg_id, seq_id))
    {
        LOG(INFO) << "acking duplicate CHAIN_SUBSPACE";
        send_ack(to, from, true, reg_id, seq_id, version, key);
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    // Check that a chain's put matches the dimensions of the space.
    if (sc->attrs_sz != value.size() + 1 || sc->attrs_sz != hashes.size())
    {
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    // Create a new pending object to set as pending.
    std::tr1::shared_ptr<e::buffer> new_backing(backing.release());
    e::intrusive_ptr<pending> new_pend(new pending(new_backing, reg_id, seq_id, false, true, value, m_daemon->m_config.version(), from));
    new_pend->old_hashes.resize(sc->attrs_sz);
    new_pend->new_hashes.resize(sc->attrs_sz);
    new_pend->this_old_region = region_id();
    new_pend->this_new_region = region_id();
    new_pend->prev_region = region_id();
    new_pend->next_region = region_id();
    subspace_id subspace_this = m_daemon->m_config.subspace_of(ri);
    subspace_id subspace_prev = m_daemon->m_config.subspace_prev(subspace_this);
    subspace_id subspace_next = m_daemon->m_config.subspace_next(subspace_this);
    hyperdex::hash(*sc, key, value, &new_pend->new_hashes.front());
    new_pend->old_hashes = hashes;

    if (subspace_prev != subspace_id())
    {
        m_daemon->m_config.lookup_region(subspace_prev, new_pend->new_hashes, &new_pend->prev_region);
    }

    m_daemon->m_config.lookup_region(subspace_this, new_pend->old_hashes, &new_pend->this_old_region);
    m_daemon->m_config.lookup_region(subspace_this, new_pend->new_hashes, &new_pend->this_new_region);

    if (subspace_next != subspace_id())
    {
        m_daemon->m_config.lookup_region(subspace_next, new_pend->old_hashes, &new_pend->next_region);
    }

    if (!(new_pend->this_old_region == m_daemon->m_config.get_region_id(from) && 
          m_daemon->m_config.tail_of_region(ri) == from) &&
        !(new_pend->this_new_region == m_daemon->m_config.get_region_id(from) &&
          m_daemon->m_config.next_in_region(from) != to))
    {
        LOG(INFO) << "dropping CHAIN_SUBSPACE which didn't obey chaining rules";
        CLEANUP_KEYHOLDER(ri, key, kh);
        return;
    }

    kh->insert_deferred(version, new_pend);
    move_operations_between_queues(to, ri, *sc, key, kh);
    CLEANUP_KEYHOLDER(ri, key, kh);
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
    region_id ri(m_daemon->m_config.get_region_id(to));
    const schema* sc = m_daemon->m_config.get_schema(ri);
    HOLD_LOCK_FOR_KEY(ri, key);
    e::intrusive_ptr<keyholder> kh = get_keyholder(ri, key);

    if (reg_id != ri)
    {
        LOG(ERROR) << "dropping CHAIN_OP send to the wrong region";
        return;
    }

    if (retransmission && m_daemon->m_data.check_acked(reg_id, seq_id))
    {
        LOG(INFO) << "dropping duplicate CHAIN_ACK";
        return;
    }

    if (!kh)
    {
        LOG(INFO) << "dropping CHAIN_ACK for update we haven't seen";
        return;
    }

    e::intrusive_ptr<pending> pend = kh->get_by_version(version);

    if (!pend)
    {
        LOG(INFO) << "dropping CHAIN_ACK for update we haven't seen";
        return;
    }

    if (pend->sent == virtual_server_id())
    {
        LOG(INFO) << "dropping CHAIN_ACK for update we haven't sent";
        return;
    }

    if (from != pend->sent)
    {
        LOG(INFO) << "dropping CHAIN_ACK that came from the wrong host";
        return;
    }

    if (m_daemon->m_config.version() != pend->sent_config_version)
    {
        LOG(INFO) << "dropping CHAIN_ACK that was sent in a previous version and hasn't been retransmitted";
        return;
    }

    if (pend->reg_id != reg_id || pend->seq_id != seq_id)
    {
        LOG(INFO) << "dropping CHAIN_ACK that was sent with mismatching reg/seq ids";
        return;
    }

    pend->acked = true;

    if (kh->version_on_disk() < version)
    {
        assert(reg_id == ri);
        e::intrusive_ptr<pending> op = kh->get_by_version(version);
        assert(op);

        datalayer::returncode rc;

        if (!op->has_value || (op->this_old_region != op->this_new_region && ri == op->this_old_region))
        {
            rc = m_daemon->m_data.del(ri, seq_id, key);
        }
        else
        {
            rc = m_daemon->m_data.put(ri, seq_id, key, op->value, version);
        }

        switch (rc)
        {
            case datalayer::SUCCESS:
                break;
            case datalayer::NOT_FOUND:
            case datalayer::BAD_ENCODING:
            case datalayer::CORRUPTION:
            case datalayer::IO_ERROR:
            case datalayer::LEVELDB_ERROR:
                LOG(ERROR) << "commit caused error " << rc;
                break;
            default:
                LOG(ERROR) << "commit caused unknown error";
                break;
        }

        kh->set_version_on_disk(version);
    }
    else
    {
        m_daemon->m_data.mark_acked(reg_id, seq_id);
    }

    kh->clear_committable_acked();
    move_operations_between_queues(to, ri, *sc, key, kh);

    if (m_daemon->m_config.is_point_leader(to))
    {
        respond_to_client(to, pend->client, pend->nonce, NET_SUCCESS);
    }
    else
    {
        send_ack(to, pend->recv, false, reg_id, seq_id, version, key);
    }

    CLEANUP_KEYHOLDER(ri, key, kh);
}

uint64_t
replication_manager :: hash(const keypair& kp)
{
    return CityHash64WithSeed(reinterpret_cast<const char*>(kp.key.data()),
                              kp.key.size(),
                              kp.region.get());
}

uint64_t
replication_manager :: get_lock_num(const region_id& reg,
                                    const e::slice& key)
{
    return CityHash64WithSeed(reinterpret_cast<const char*>(key.data()),
                              key.size(),
                              reg.get());
}

e::intrusive_ptr<replication_manager::keyholder>
replication_manager :: get_keyholder(const region_id& reg, const e::slice& key)
{
    keypair kp(reg, std::string(reinterpret_cast<const char*>(key.data()), key.size()));
    e::intrusive_ptr<keyholder> kh;

    if (!m_keyholders.lookup(kp, &kh))
    {
        return NULL;
    }

    return kh;
}

e::intrusive_ptr<replication_manager::keyholder>
replication_manager :: get_or_create_keyholder(const region_id& reg, const e::slice& key)
{
    keypair kp(reg, std::string(reinterpret_cast<const char*>(key.data()), key.size()));
    e::intrusive_ptr<keyholder> kh;

    if (!m_keyholders.lookup(kp, &kh))
    {
        kh = new keyholder();

        if (!m_keyholders.insert(kp, kh))
        {
            abort();
        }

        switch (m_daemon->m_data.get(reg, key,
                                     &kh->get_old_value(),
                                     &kh->get_old_version(),
                                     &kh->get_old_disk_ref()))
        {
            case datalayer::SUCCESS:
                kh->get_has_old_value() = true;
                break;
            case datalayer::NOT_FOUND:
                kh->get_has_old_value() = false;
                kh->get_old_version() = 0;
                break;
            case datalayer::BAD_ENCODING:
            case datalayer::CORRUPTION:
            case datalayer::IO_ERROR:
            case datalayer::LEVELDB_ERROR:
            default:
                LOG(ERROR) << "Data layer returned unexpected result when reading old value.";
                return NULL;
        }
    }

    return kh;
}

void
replication_manager :: erase_keyholder(const region_id& reg,
                                       const e::slice& key)
{
    keypair kp(reg, std::string(reinterpret_cast<const char*>(key.data()), key.size()));
    m_keyholders.remove(kp);
}

void
replication_manager :: hash_objects(const region_id& reg,
                                    const schema& sc,
                                    const e::slice& key,
                                    bool has_new_value,
                                    const std::vector<e::slice>& new_value,
                                    bool has_old_value,
                                    const std::vector<e::slice>& old_value,
                                    e::intrusive_ptr<pending> pend)
{
    pend->old_hashes.resize(sc.attrs_sz);
    pend->new_hashes.resize(sc.attrs_sz);
    pend->this_old_region = region_id();
    pend->this_new_region = region_id();
    pend->prev_region = region_id();
    pend->next_region = region_id();
    subspace_id subspace_this = m_daemon->m_config.subspace_of(reg);
    subspace_id subspace_prev = m_daemon->m_config.subspace_prev(subspace_this);
    subspace_id subspace_next = m_daemon->m_config.subspace_next(subspace_this);

    if (has_old_value && has_new_value)
    {
        hyperdex::hash(sc, key, new_value, &pend->new_hashes.front());
        hyperdex::hash(sc, key, old_value, &pend->old_hashes.front());

        if (subspace_prev != subspace_id())
        {
            m_daemon->m_config.lookup_region(subspace_prev, pend->new_hashes, &pend->prev_region);
        }

        m_daemon->m_config.lookup_region(subspace_this, pend->old_hashes, &pend->this_old_region);
        m_daemon->m_config.lookup_region(subspace_this, pend->new_hashes, &pend->this_new_region);

        if (subspace_next != subspace_id())
        {
            m_daemon->m_config.lookup_region(subspace_next, pend->old_hashes, &pend->next_region);
        }
    }
    else if (has_old_value)
    {
        hyperdex::hash(sc, key, old_value, &pend->old_hashes.front());

        for (size_t i = 0; i < sc.attrs_sz; ++i)
        {
            pend->new_hashes[i] = pend->old_hashes[i];
        }

        if (subspace_prev != subspace_id())
        {
            m_daemon->m_config.lookup_region(subspace_prev, pend->old_hashes, &pend->prev_region);
        }

        m_daemon->m_config.lookup_region(subspace_this, pend->old_hashes, &pend->this_old_region);
        pend->this_new_region = pend->this_old_region;

        if (subspace_next != subspace_id())
        {
            m_daemon->m_config.lookup_region(subspace_next, pend->old_hashes, &pend->next_region);
        }
    }
    else if (has_new_value)
    {
        hyperdex::hash(sc, key, new_value, &pend->new_hashes.front());

        for (size_t i = 0; i < sc.attrs_sz; ++i)
        {
            pend->old_hashes[i] = pend->new_hashes[i];
        }

        if (subspace_prev != subspace_id())
        {
            m_daemon->m_config.lookup_region(subspace_prev, pend->old_hashes, &pend->prev_region);
        }

        m_daemon->m_config.lookup_region(subspace_this, pend->old_hashes, &pend->this_new_region);
        pend->this_old_region = pend->this_new_region;

        if (subspace_next != subspace_id())
        {
            m_daemon->m_config.lookup_region(subspace_next, pend->old_hashes, &pend->next_region);
        }
    }
    else
    {
        abort();
    }
}

void
replication_manager :: move_operations_between_queues(const virtual_server_id& us,
                                                      const region_id& ri,
                                                      const schema& sc,
                                                      const e::slice& key,
                                                      e::intrusive_ptr<keyholder> kh)
{
    // See if we can re-use some of the deferred operations
    while (kh->has_deferred_ops())
    {
        bool has_old_value = false;
        uint64_t old_version = 0;
        std::vector<e::slice>* old_value = NULL;
        kh->get_latest_version(&has_old_value, &old_version, &old_value);
        assert(old_version < kh->oldest_deferred_version());

        if (old_version + 1 != kh->oldest_deferred_version())
        {
            break;
        }

        e::intrusive_ptr<pending> new_pend = kh->oldest_deferred_op();
        hash_objects(ri, sc, key, new_pend->has_value, new_pend->value, has_old_value, old_value ? *old_value : new_pend->value, new_pend);

        if (new_pend->this_old_region != ri && new_pend->this_new_region != ri)
        {
            LOG(INFO) << "dropping deferred CHAIN_* which didn't get sent to the right host";
            kh->pop_oldest_deferred();
            continue;
        }

        if (new_pend->recv != virtual_server_id() &&
            m_daemon->m_config.next_in_region(new_pend->recv) != us &&
            !m_daemon->m_config.subspace_adjacent(new_pend->recv, us))
        {
            LOG(INFO) << "dropping deferred CHAIN_* which didn't come from the right host";
            kh->pop_oldest_deferred();
            continue;
        }

        kh->shift_one_deferred_to_blocked();
    }

    // Issue blocked operations
    while (kh->has_blocked_ops())
    {
        uint64_t version = kh->oldest_blocked_version();
        e::intrusive_ptr<pending> op = kh->oldest_blocked_op();

        // If the op is on either side of a delete, wait until there are no more
        // committable ops
        if ((op->fresh || !op->has_value) && kh->has_committable_ops())
        {
            break;
        }

        kh->shift_one_blocked_to_committable();
        send_message(us, false, version, key, op);
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

    while (true)
    {
        {
            po6::threads::mutex::hold hold(&m_block_retransmitter);

            while (!m_need_retransmit && !m_shutdown)
            {
                m_wakeup_retransmitter.wait();
            }

            if (m_shutdown)
            {
                break;
            }

            m_need_retransmit = false;
        }

        std::set<region_id> region_cache;

        for (keyholder_map_t::iterator it = m_keyholders.begin();
                it != m_keyholders.end(); it.next())
        {
            po6::threads::mutex::hold hold(&m_block_retransmitter);
            region_id ri(it.key().region);

            if (region_cache.find(ri) != region_cache.end() ||
                m_daemon->m_config.is_server_blocked_by_live_transfer(m_daemon->m_us, ri))
            {
                region_cache.insert(ri);
                continue;
            }

            e::slice key(it.key().key.data(), it.key().key.size());
            HOLD_LOCK_FOR_KEY(ri, key);
            e::intrusive_ptr<keyholder> kh = get_keyholder(ri, key);

            if (!kh)
            {
                continue;
            }

            virtual_server_id us = m_daemon->m_config.get_virtual(ri, m_daemon->m_us);

            if (us == virtual_server_id())
            {
                m_keyholders.remove(it.key());
                continue;
            }

            const schema* sc = m_daemon->m_config.get_schema(ri);
            assert(sc);

            if (kh->empty())
            {
                LOG(WARNING) << "leaking keyholders (this is harmless if you reconfigure enough)";
                m_keyholders.remove(it.key());
                continue;
            }

            kh->resend_committable(this, us, key);
            move_operations_between_queues(us, ri, *sc, key, kh);
        }

        m_daemon->m_comm.wake_one();
    }

    LOG(INFO) << "retransmitter thread shutting down";
}

void
replication_manager :: shutdown()
{
    bool is_shutdown;

    {
        po6::threads::mutex::hold hold(&m_block_retransmitter);
        m_wakeup_retransmitter.broadcast();
        is_shutdown = m_shutdown;
        m_shutdown = true;
    }

    if (!is_shutdown)
    {
        m_retransmitter.join();
    }
}
