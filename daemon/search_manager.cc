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

// HyperDex
#include "common/attribute_check.h"
#include "daemon/search_manager.h"

using hyperdex::search_manager;
using hyperdex::reconfigure_returncode;

/////////////////////////////// Search Manager ID //////////////////////////////

class search_manager::id
{
    public:
        id(const regionid re,
           const entityid cl,
           uint64_t sn)
        : region(re), client(cl), search_number(sn) {}
        ~id() throw () {}

    public:
        int compare(const id& other) const
        { return e::tuple_compare(region, client, search_number,
                                  other.region, other.client, other.search_number); }

    public:
        bool operator < (const id& rhs) const { return compare(rhs) < 0; }
        bool operator <= (const id& rhs) const { return compare(rhs) <= 0; }
        bool operator == (const id& rhs) const { return compare(rhs) == 0; }
        bool operator != (const id& rhs) const { return compare(rhs) != 0; }
        bool operator >= (const id& rhs) const { return compare(rhs) >= 0; }
        bool operator > (const id& rhs) const { return compare(rhs) > 0; }

    public:
        regionid region;
        entityid client;
        uint64_t search_number;
};

///////////////////////////// Search Manager State /////////////////////////////

class search_manager::state
{
    public:
        state(const regionid& region,
              std::auto_ptr<e::buffer> msg,
              std::vector<attribute_check>* checks);
        ~state() throw ();

    public:
        po6::threads::mutex lock;
        const regionid region;
        const std::auto_ptr<e::buffer> backing;
        std::vector<attribute_check> checks;
        datalayer::snapshot snap;

    private:
        friend class e::intrusive_ptr<state>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
};

search_manager :: state :: ~state() throw ()
{
}

//////////////////////////////// Search Manager ////////////////////////////////

search_manager :: search_manager(daemon* d)
    : m_daemon(d)
    , m_searches(1024)
{
}

search_manager :: ~search_manager() throw ()
{
}

reconfigure_returncode
search_manager :: prepare(const configuration&,
                          const configuration&,
                          const instance&)
{
    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
search_manager :: reconfigure(const configuration&,
                              const configuration&,
                              const instance&)
{
    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
search_manager :: cleanup(const configuration&,
                          const configuration&,
                          const instance&)
{
    return RECONFIGURE_SUCCESS;
}

#if 0
void
search_manager :: start(const entityid& us,
                        const entityid& client,
                        uint64_t search_num,
                        uint64_t nonce,
                        std::auto_ptr<e::buffer> msg,
                        const hyperspacehashing::search& wc)
{
    id key(us.get_region(), client, search_num);

    if (m_searches.contains(key))
    {
        next(us, client, search_num, nonce);
        return;
    }

    schema* sc = m_config.get_schema(us.get_space());
    assert(sc);

    if (sc->attrs_sz != terms.size())
    {
        LOG(INFO) << "DROPPED"; // XXX
        return;
    }
}

void
hyperdaemon :: searches :: start(const hyperdex::entityid& us,
                                 const hyperdex::entityid& client,
                                 uint64_t search_num,
                                 uint64_t nonce,
                                 std::auto_ptr<e::buffer> msg,
                                 const hyperspacehashing::search& terms)
{

    e::intrusive_ptr<search_state> state = new search_state(us.get_region(), coord, msg, terms);

    if (!m_data->make_search_snapshot(us.get_region(), terms, &state->snap))
    {
        LOG(INFO) << "DROPPED"; // XXX
        return;
    }

    m_searches.insert(key, state);
    next(us, client, search_num, nonce);
}

void
hyperdaemon :: searches :: next(const hyperdex::entityid& us,
                                const hyperdex::entityid& client,
                                uint64_t search_num,
                                uint64_t nonce)
{
    search_id key(us.get_region(), client, search_num);
    e::intrusive_ptr<search_state> state;

    if (!m_searches.lookup(key, &state))
    {
        LOG(INFO) << "DROPPED"; // XXX
        return;
    }

    po6::threads::mutex::hold hold(&state->lock);

    while (state->snap.has_next())
    {
        if (state->terms.matches(state->snap.key(), state->snap.value()))
        {
            size_t sz = m_comm->header_size() + sizeof(uint64_t)
                      + sizeof(uint32_t) + state->snap.key().size()
                      + hyperdex::packspace(state->snap.value());
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(m_comm->header_size())
                << nonce
                << state->snap.key()
                << state->snap.value();
            m_comm->send(us, client, hyperdex::RESP_SEARCH_ITEM, msg);
            state->snap.next();
            return;
        }

        state->snap.next();
    }

    std::auto_ptr<e::buffer> msg(e::buffer::create(m_comm->header_size() + sizeof(uint64_t)));
    msg->pack_at(m_comm->header_size()) << nonce;
    m_comm->send(us, client, hyperdex::RESP_SEARCH_DONE, msg);
    stop(us, client, search_num);
}

void
hyperdaemon :: searches :: stop(const hyperdex::entityid& us,
                                const hyperdex::entityid& client,
                                uint64_t search_num)
{
    m_searches.remove(search_id(us.get_region(), client, search_num));
}

void
hyperdaemon :: searches :: group_keyop(const hyperdex::entityid& us,
                                       const hyperdex::entityid& client,
                                       uint64_t nonce,
                                       const hyperspacehashing::search& terms,
                                       enum hyperdex::network_msgtype reqtype,
                                       const e::slice& remain)
{
    schema* sc = m_config.get_schema(us.get_space());
    assert(sc);

    if (sc->attrs_sz != terms.size())
    {
        size_t sz = m_comm->header_size() + sizeof(uint64_t) + sizeof(uint16_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        e::buffer::packer pa = msg->pack_at(m_comm->header_size());
        pa = pa << nonce << static_cast<uint16_t>(hyperdex::NET_BADDIMSPEC);
        m_comm->send(us, client, hyperdex::RESP_GROUP_DEL, msg);
        return;
    }

    hyperspacehashing::mask::hasher hasher(m_config.disk_hasher(us.get_subspace()));
    hyperspacehashing::mask::coordinate coord(hasher.hash(terms));
    search_snapshot snap;

    if (!m_data->make_search_snapshot(us.get_region(), terms, &snap))
    {
        LOG(INFO) << "DROPPED"; // XXX
        return;
    }

    while (snap.has_next())
    {
        if (terms.matches(snap.key(), snap.value()))
        {
            size_t sz = m_comm->header_size()
                      + sizeof(uint64_t)
                      + sizeof(uint32_t)
                      + snap.key().size()
                      + remain.size();
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            e::buffer::packer pa = msg->pack_at(m_comm->header_size());
            pa = pa << static_cast<uint64_t>(0) << snap.key();
            pa = pa.copy(remain);

            // Figure out who to talk with.
            hyperdex::entityid dst_ent;
            hyperdex::_instance dst_inst;

            if (m_config.point_leader_entity(us.get_space(), snap.key(), &dst_ent, &dst_inst))
            {
                m_comm->send(us, dst_ent, reqtype, msg);
            }
            else
            {
                // If it fails to find the point leader entity, something is
                // horribly wrong.  We make no guarantees, so we just log and move
                // on.
                LOG(ERROR) << "Could not find point leader for GROUP_DEL (serious bug; please report)";
            }
        }

        snap.next();
    }

    size_t sz = m_comm->header_size() + sizeof(uint64_t) + sizeof(uint16_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(m_comm->header_size());
    pa = pa << nonce << static_cast<uint16_t>(hyperdex::NET_SUCCESS);
    m_comm->send(us, client, hyperdex::RESP_GROUP_DEL, msg);
}

void
hyperdaemon :: searches :: count(const hyperdex::entityid& us,
                                 const hyperdex::entityid& client,
                                 uint64_t nonce,
                                 const hyperspacehashing::search& terms)
{
    schema* sc = m_config.get_schema(us.get_space());
    assert(sc);

    if (sc->attrs_sz != terms.size())
    {
        size_t sz = m_comm->header_size() + sizeof(uint64_t) + sizeof(uint16_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        e::buffer::packer pa = msg->pack_at(m_comm->header_size());
        pa = pa << nonce << static_cast<uint16_t>(hyperdex::NET_BADDIMSPEC);
        m_comm->send(us, client, hyperdex::RESP_COUNT, msg);
        return;
    }

    hyperspacehashing::mask::hasher hasher(m_config.disk_hasher(us.get_subspace()));
    hyperspacehashing::mask::coordinate coord(hasher.hash(terms));
    search_snapshot snap;

    if (!m_data->make_search_snapshot(us.get_region(), terms, &snap))
    {
        LOG(INFO) << "DROPPED"; // XXX
        return;
    }

    uint64_t result = 0;

    while (snap.has_next())
    {
        if (terms.matches(snap.key(), snap.value()))
        {
            ++result;
        }

        snap.next();
    }

    size_t sz = m_comm->header_size() + sizeof(uint64_t) + sizeof(uint16_t) + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(m_comm->header_size());
    pa = pa << nonce << static_cast<uint16_t>(hyperdex::NET_SUCCESS) << result;
    m_comm->send(us, client, hyperdex::RESP_COUNT, msg);
}

struct sorted_search_item
{
    sorted_search_item(const disk_reference& ref,
                       const e::slice& key,
                       const std::vector<e::slice>& value,
                       uint16_t sort_by);
    disk_reference ref;
    e::slice key;
    std::vector<e::slice> value;
    uint16_t sort_by;
};

sorted_search_item :: sorted_search_item(const disk_reference& r,
                                         const e::slice& k,
                                         const std::vector<e::slice>& v,
                                         uint16_t s)
    : ref(r)
    , key(k)
    , value(v)
    , sort_by(s)
{
}

// XXX use the other code for this
static bool
sorted_search_lt_string(const sorted_search_item& ssilhs,
                        const sorted_search_item& ssirhs)
{
    e::slice lhs(ssilhs.sort_by > 0 ? ssilhs.value[ssilhs.sort_by - 1] : ssilhs.key);
    e::slice rhs(ssirhs.sort_by > 0 ? ssirhs.value[ssirhs.sort_by - 1] : ssirhs.key);
    int cmp = memcmp(lhs.data(), rhs.data(), std::min(lhs.size(), rhs.size()));

    if (cmp == 0)
    {
        return lhs.size() < rhs.size();
    }

    return cmp < 0;
}

static bool
sorted_search_gt_string(const sorted_search_item& lhs,
                        const sorted_search_item& rhs)
{
    return sorted_search_lt_string(rhs, lhs);
}

static bool
sorted_search_lt_int64(const sorted_search_item& ssilhs,
                       const sorted_search_item& ssirhs)
{
    e::slice lhs(ssilhs.sort_by > 0 ? ssilhs.value[ssilhs.sort_by - 1] : ssilhs.key);
    e::slice rhs(ssirhs.sort_by > 0 ? ssirhs.value[ssirhs.sort_by - 1] : ssirhs.key);
    uint8_t buflhs[sizeof(int64_t)];
    uint8_t bufrhs[sizeof(int64_t)];
    memset(buflhs, 0, sizeof(int64_t));
    memset(bufrhs, 0, sizeof(int64_t));
    memmove(buflhs, lhs.data(), lhs.size());
    memmove(bufrhs, rhs.data(), rhs.size());
    int64_t ilhs;
    int64_t irhs;
    e::unpack64le(buflhs, &ilhs);
    e::unpack64le(bufrhs, &irhs);
    return ilhs < irhs;
}

static bool
sorted_search_gt_int64(const sorted_search_item& lhs,
                       const sorted_search_item& rhs)
{
    return sorted_search_lt_int64(rhs, lhs);
}

void
hyperdaemon :: searches :: sorted_search(const hyperdex::entityid& us,
                                         const hyperdex::entityid& client,
                                         uint64_t nonce,
                                         const hyperspacehashing::search& terms,
                                         uint64_t num,
                                         uint16_t sort_by,
                                         bool maximize)
{
    schema* sc = m_config.get_schema(us.get_space());
    assert(sc);

    if (sc->attrs_sz != terms.size() || sort_by >= sc->attrs_sz ||
        (sc->attrs[sort_by].type != HYPERDATATYPE_STRING && sc->attrs[sort_by].type != HYPERDATATYPE_INT64))
    {
        size_t sz = m_comm->header_size() + sizeof(uint64_t) + sizeof(uint16_t);
        std::auto_ptr<e::buffer> errmsg(e::buffer::create(sz));
        e::buffer::packer pa = errmsg->pack_at(m_comm->header_size());
        pa = pa << nonce << static_cast<uint16_t>(hyperdex::NET_BADDIMSPEC);
        m_comm->send(us, client, hyperdex::RESP_SORTED_SEARCH, errmsg);
        return;
    }

    bool (*cmp)(const sorted_search_item& lhs, const sorted_search_item& rhs);

    if (sc->attrs[sort_by].type == HYPERDATATYPE_STRING)
    {
        if (maximize)
        {
            cmp = sorted_search_gt_string;
        }
        else
        {
            cmp = sorted_search_lt_string;
        }
    }
    else if (sc->attrs[sort_by].type == HYPERDATATYPE_INT64)
    {
        if (maximize)
        {
            cmp = sorted_search_gt_int64;
        }
        else
        {
            cmp = sorted_search_lt_int64;
        }
    }
    else
    {
        abort();
    }

    hyperspacehashing::mask::hasher hasher(m_config.disk_hasher(us.get_subspace()));
    hyperspacehashing::mask::coordinate coord(hasher.hash(terms));
    search_snapshot snap;

    if (!m_data->make_search_snapshot(us.get_region(), terms, &snap))
    {
        LOG(INFO) << "DROPPED"; // XXX
        return;
    }

    std::vector<sorted_search_item> top_n;

    if (num < UINT32_MAX)
    {
        top_n.reserve(num);
    }

    while (snap.has_next())
    {
        if (terms.matches(snap.key(), snap.value()))
        {
            top_n.push_back(sorted_search_item(snap.ref(), snap.key(), snap.value(), sort_by));
            std::push_heap(top_n.begin(), top_n.end(), cmp);

            if (top_n.size() > num)
            {
                std::pop_heap(top_n.begin(), top_n.end(), cmp);
                top_n.pop_back();
            }
        }

        snap.next();
    }

    size_t sz = m_comm->header_size() + sizeof(uint64_t) + sizeof(uint16_t) + sizeof(uint64_t);

    for (size_t i = 0; i < top_n.size(); ++i)
    {
        sz += sizeof(uint32_t) + top_n[i].key.size()
            + hyperdex::packspace(top_n[i].value);
    }

    uint64_t numitems = top_n.size();
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(m_comm->header_size());
    pa = pa << nonce << static_cast<uint16_t>(hyperdex::NET_SUCCESS) << numitems;

    for (size_t i = 0; i < top_n.size(); ++i)
    {
        pa = pa << top_n[i].key << top_n[i].value;
    }

    m_comm->send(us, client, hyperdex::RESP_SORTED_SEARCH, msg);
}

uint64_t
hyperdaemon :: searches :: hash(const search_id& si)
{
    return si.region.hash() + si.client.hash() + si.search_number;
}

hyperdaemon :: searches :: search_state :: search_state(const regionid& r,
                                                        std::auto_ptr<e::buffer> msg,
                                                        std::vector<attribute_check>* cks)
    : lock()
    , region(r)
    , backing(msg)
    , checks()
    , snap()
    , m_ref(0)
{
    checks.swap(*cks);
}

hyperdaemon :: searches :: search_state :: ~search_state() throw ()
{
}
#endif
