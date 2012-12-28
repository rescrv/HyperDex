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
    e::striped_lock<po6::threads::mutex>::hold CONCAT(_anon, __LINE__)(&m_locks, get_lock_num(R, K))

replication_manager :: replication_manager(daemon* d)
    : m_daemon(d)
    , m_locks(1024)
    , m_keyholders(10)
{
}

replication_manager :: ~replication_manager() throw ()
{
}

bool
replication_manager :: setup()
{
    return true;
}

void
replication_manager :: teardown()
{
}

reconfigure_returncode
replication_manager :: prepare(const configuration&,
                               const configuration&,
                               const server_id&)
{
    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
replication_manager :: reconfigure(const configuration&,
                                   const configuration&,
                                   const server_id&)
{
    // XXX killall deferred for is_point_leader(vsi)
    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
replication_manager :: cleanup(const configuration&,
                               const configuration&,
                               const server_id&)
{
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
        return;
    }

    if (!has_old_value && erase)
    {
        respond_to_client(to, from, nonce, NET_NOTFOUND);
        return;
    }

    if ((has_old_value && fail_if_found) ||
        (!has_old_value && fail_if_not_found))
    {
        respond_to_client(to, from, nonce, NET_CMPFAIL);
        return;
    }

    if (has_old_value && old_value->size() + 1 != sc->attrs_sz)
    {
        LOG(ERROR) << "received a corrupt object"; // XXX don't leave the client hanging
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
        return;
    }

    bool has_new_value = !erase;

    e::intrusive_ptr<pending> new_pend(new pending(backing, !has_old_value && has_new_value, has_new_value, new_value, from, nonce));
    hash_objects(ri, *sc, key, has_new_value, new_value, has_old_value, *old_value, new_pend);

    if (new_pend->this_old_region != ri && new_pend->this_new_region != ri)
    {
        respond_to_client(to, from, nonce, NET_NOTUS);
        return;
    }

    assert(!kh->has_deferred_ops());
    kh->append_blocked(old_version + 1, new_pend);
    move_operations_between_queues(to, ri, *sc, key, kh);
}

void
replication_manager :: chain_put(const virtual_server_id& from,
                                 const virtual_server_id& to,
                                 uint64_t newversion,
                                 bool fresh,
                                 std::auto_ptr<e::buffer> backing,
                                 const e::slice& key,
                                 const std::vector<e::slice>& newvalue)
{
    chain_common(true, from, to, newversion, fresh, backing, key, newvalue);
}

void
replication_manager :: chain_del(const virtual_server_id& from,
                                 const virtual_server_id& to,
                                 uint64_t newversion,
                                 std::auto_ptr<e::buffer> backing,
                                 const e::slice& key)
{
    chain_common(false, from, to, newversion, false, backing, key, std::vector<e::slice>());
}

void
replication_manager :: chain_subspace(const virtual_server_id& from,
                                      const virtual_server_id& to,
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

    // Check that a chain's put matches the dimensions of the space.
    if (sc->attrs_sz != value.size() + 1 || sc->attrs_sz != hashes.size())
    {
        return;
    }

    // XXX check previous versions to see if we can ack right away

    // Create a new pending object to set as pending.
    std::tr1::shared_ptr<e::buffer> new_backing(backing.release());
    e::intrusive_ptr<pending> new_pend(new pending(new_backing, false, true, value, m_daemon->m_config.version(), from));
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
        return;
    }

    kh->append_blocked(version, new_pend);
    move_operations_between_queues(to, ri, *sc, key, kh);
}

void
replication_manager :: chain_ack(const virtual_server_id& from,
                                 const virtual_server_id& to,
                                 uint64_t version,
                                 const e::slice& key)
{
    region_id ri(m_daemon->m_config.get_region_id(to));
    const schema* sc = m_daemon->m_config.get_schema(ri);
    HOLD_LOCK_FOR_KEY(ri, key);
    e::intrusive_ptr<keyholder> kh = get_keyholder(ri, key);

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

    pend->acked = true;

    if (kh->version_on_disk() < version)
    {
        e::intrusive_ptr<pending> op = kh->get_by_version(version);
        assert(op);

        datalayer::returncode rc;

        if (!op->has_value || (op->this_old_region != op->this_new_region && ri == op->this_old_region))
        {
            switch ((rc = m_daemon->m_data.del(ri, key)))
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
        }
        else
        {
            switch ((rc = m_daemon->m_data.put(ri, key, op->value, version)))
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
        }

        kh->set_version_on_disk(version);
    }

    kh->clear_committable_acked();
    move_operations_between_queues(to, ri, *sc, key, kh);

    if (m_daemon->m_config.is_point_leader(to))
    {
        respond_to_client(to, pend->client, pend->nonce, NET_SUCCESS);
    }
    else
    {
        send_ack(to, pend->recv, version, key);
    }

    if (kh->empty())
    {
        erase_keyholder(ri, key);
    }
}

uint64_t
replication_manager :: hash(const keypair& kp)
{
    return CityHash64WithSeed(reinterpret_cast<const char*>(kp.key.data()),
                              kp.key.size(),
                              kp.region.hash());
}

void
replication_manager :: chain_common(bool has_value,
                                    const virtual_server_id& from,
                                    const virtual_server_id& to,
                                    uint64_t version,
                                    bool fresh,
                                    std::auto_ptr<e::buffer> backing,
                                    const e::slice& key,
                                    const std::vector<e::slice>& value)
{
    region_id ri(m_daemon->m_config.get_region_id(to));
    const schema* sc = m_daemon->m_config.get_schema(ri);
    HOLD_LOCK_FOR_KEY(ri, key);
    e::intrusive_ptr<keyholder> kh = get_or_create_keyholder(ri, key);

    // Check that a chain's put matches the dimensions of the space.
    if (has_value && sc->attrs_sz != value.size() + 1)
    {
        LOG(INFO) << "dropping CHAIN_* because the dimensions are incorrect";
        return;
    }

    e::intrusive_ptr<pending> new_op = kh->get_by_version(version);

    if (new_op)
    {
        new_op->recv_config_version = m_daemon->m_config.version();
        new_op->recv = from;

        if (new_op->acked)
        {
            send_ack(to, from, version, key);
        }

        return;
    }

    if (version <= kh->version_on_disk())
    {
        send_ack(to, from, version, key);
        return;
    }

    e::intrusive_ptr<pending> old_op = kh->get_by_version(version - 1);
    std::tr1::shared_ptr<e::buffer> new_backing(backing.release());

    if (!old_op && !fresh)
    {
        e::intrusive_ptr<pending> new_defer(new pending(new_backing, fresh, has_value, value, m_daemon->m_config.version(), from));
        kh->insert_deferred(version, new_defer);
        return;
    }

    e::intrusive_ptr<pending> new_pend(new pending(new_backing, fresh, has_value, value, m_daemon->m_config.version(), from));
    hash_objects(ri, *sc, key, has_value, value, bool(old_op), old_op ? old_op->value : value, new_pend);

    if (new_pend->this_old_region != ri)
        /* if ^ is false and new_pend->this_new_region == ri then it should be a chain_subspace */
    {
        LOG(INFO) << "dropping CHAIN_* which didn't get sent to the right host";
        return;
    }

    if (m_daemon->m_config.next_in_region(from) != to &&
        !m_daemon->m_config.subspace_adjacent(from, to))
    {
        LOG(INFO) << "dropping CHAIN_* which didn't come from the right host";
        return;
    }

    kh->append_blocked(version, new_pend);
    move_operations_between_queues(to, ri, *sc, key, kh);
}

uint64_t
replication_manager :: get_lock_num(const region_id& reg,
                                    const e::slice& key)
{
    return CityHash64WithSeed(reinterpret_cast<const char*>(key.data()),
                              key.size(),
                              reg.hash());
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

        if (!has_old_value)
        {
            break;
        }

        if (old_version >= kh->oldest_deferred_version())
        {
            LOG(INFO) << "We are dropping a deferred message because we've already seen the version";
            kh->remove_oldest_deferred_op();
            continue;
        }

        if (old_version + 1 != kh->oldest_deferred_version())
        {
            break;
        }

        e::intrusive_ptr<pending> new_pend = kh->oldest_deferred_op();
        hash_objects(ri, sc, key, new_pend->has_value, new_pend->value, has_old_value, old_value ? *old_value : new_pend->value, new_pend);

        if (new_pend->this_old_region != ri && new_pend->this_new_region != ri)
        {
            LOG(INFO) << "dropping deferred CHAIN_* which didn't get sent to the right host";
            return;
        }

        if (m_daemon->m_config.next_in_region(new_pend->recv) != us &&
            !m_daemon->m_config.subspace_adjacent(new_pend->recv, us))
        {
            LOG(INFO) << "dropping deferred CHAIN_* which didn't come from the right host";
            return;
        }

        kh->append_blocked(kh->oldest_deferred_version(), new_pend);
        kh->remove_oldest_deferred_op();
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
        send_message(us, version, key, op);
    }
}

void
replication_manager :: send_message(const virtual_server_id& us,
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
    network_msgtype type = op->has_value ? CHAIN_PUT : CHAIN_DEL;

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

    if (type == CHAIN_PUT)
    {
        size_t sz = HYPERDEX_HEADER_SIZE_VV
                  + sizeof(uint64_t)
                  + sizeof(uint8_t)
                  + sizeof(uint32_t)
                  + key.size()
                  + pack_size(op->value);
        msg.reset(e::buffer::create(sz));
        uint8_t flags = op->fresh ? 1 : 0;
        msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << version << flags << key << op->value;
    }
    else if (type == CHAIN_DEL)
    {
        size_t sz = HYPERDEX_HEADER_SIZE_VV
                  + sizeof(uint64_t)
                  + sizeof(uint32_t)
                  + key.size();
        msg.reset(e::buffer::create(sz));
        msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << version << key;
    }
    else if (type == CHAIN_ACK)
    {
        size_t sz = HYPERDEX_HEADER_SIZE_VV
                  + sizeof(uint64_t)
                  + sizeof(uint32_t)
                  + key.size();
        msg.reset(e::buffer::create(sz));
        msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << version << key;
    }
    else if (type == CHAIN_SUBSPACE)
    {
        size_t sz = HYPERDEX_HEADER_SIZE_VV
                  + sizeof(uint64_t)
                  + sizeof(uint8_t)
                  + sizeof(uint32_t)
                  + key.size()
                  + pack_size(op->value)
                  + pack_size(op->old_hashes);
        msg.reset(e::buffer::create(sz));
        msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << version << key << op->value << op->old_hashes;
    }
    else
    {
        abort();
    }

    op->sent_config_version = m_daemon->m_config.version();
    op->sent = dest;
    m_daemon->m_comm.send(us, dest, type, msg);
}

bool
replication_manager :: send_ack(const virtual_server_id& from,
                                const virtual_server_id& to,
                                uint64_t version,
                                const e::slice& key)
{
    size_t sz = HYPERDEX_HEADER_SIZE_VV
              + sizeof(uint64_t)
              + sizeof(uint32_t)
              + key.size();
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(HYPERDEX_HEADER_SIZE_VV) << version << key;
    return m_daemon->m_comm.send(from, to, CHAIN_ACK, msg);
}

void
replication_manager :: respond_to_client(const virtual_server_id& us,
                                         const server_id& client,
                                         uint64_t nonce,
                                         network_returncode ret)
{
    size_t sz = HYPERDEX_HEADER_SIZE_VS
              + sizeof(uint64_t)
              + sizeof(uint16_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    uint16_t result = static_cast<uint16_t>(ret);
    msg->pack_at(HYPERDEX_HEADER_SIZE_VS) << nonce << result;
    m_daemon->m_comm.send(us, client, RESP_ATOMIC, msg);
}
