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

#define __STDC_LIMIT_MACROS

// Google Log
#include <glog/logging.h>

// HyperDex
#include "common/attribute_check.h"
#include "common/serialization.h"
#include "daemon/daemon.h"
#include "daemon/search_manager.h"
#include "datatypes/compare.h"

using hyperdex::search_manager;
using hyperdex::reconfigure_returncode;

/////////////////////////////// Search Manager ID //////////////////////////////

class search_manager::id
{
    public:
        id(const regionid re,
           const entityid cl,
           uint64_t sid);
        ~id() throw ();

    public:
        int compare(const id& other) const
        { return e::tuple_compare(region, client, search_id,
                                  other.region, other.client, other.search_id); }

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
        uint64_t search_id;
};

search_manager :: id :: id(const regionid re,
                           const entityid cl,
                           uint64_t sid)
    : region(re)
    , client(cl)
    , search_id(sid)
{
}

search_manager :: id :: ~id() throw ()
{
}

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

search_manager :: state :: state(const regionid& r,
                                 std::auto_ptr<e::buffer> msg,
                                 std::vector<attribute_check>* c)
    : lock()
    , region(r)
    , backing(msg)
    , checks()
    , snap()
    , m_ref(0)
{
    checks.swap(*c);
}

search_manager :: state :: ~state() throw ()
{
}

//////////////////////////////// Search Manager ////////////////////////////////

search_manager :: search_manager(daemon* d)
    : m_daemon(d)
    , m_searches(10)
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

void
search_manager :: start(const entityid& us,
                        const entityid& client,
                        std::auto_ptr<e::buffer> msg,
                        uint64_t nonce,
                        uint64_t search_id,
                        std::vector<attribute_check>* checks)
{
    id sid(us.get_region(), client, search_id);

    if (m_searches.contains(sid))
    {
        LOG(WARNING) << "received request for search " << search_id << " from client "
                     << client << " but the search is already in progress";
        return;
    }

    schema* sc = m_daemon->m_config.get_schema(us.get_space());
    assert(sc);
    e::intrusive_ptr<state> st = new state(us.get_region(), msg, checks);
    datalayer::returncode rc;
    rc = m_daemon->m_data.make_snapshot(st->region, &st->checks.front(), st->checks.size(), &st->snap);

    switch (rc)
    {
        case datalayer::SUCCESS:
            break;
        case datalayer::NOT_FOUND:
        case datalayer::BAD_ENCODING:
        case datalayer::CORRUPTION:
        case datalayer::IO_ERROR:
        case datalayer::LEVELDB_ERROR:
            LOG(ERROR) << "could not make snapshot for search:  " << rc;
            return;
        default:
            abort();
    }

    m_searches.insert(sid, st);
    next(us, client, search_id, nonce);
}

void
search_manager :: next(const entityid& us,
                       const entityid& client,
                       uint64_t nonce,
                       uint64_t search_id)
{
    id sid(us.get_region(), client, search_id);
    e::intrusive_ptr<state> st;

    if (!m_searches.lookup(sid, &st))
    {
        std::auto_ptr<e::buffer> msg(e::buffer::create(m_daemon->m_comm.header_size() + sizeof(uint64_t)));
        msg->pack_at(m_daemon->m_comm.header_size()) << nonce;
        m_daemon->m_comm.send(us, client, RESP_SEARCH_DONE, msg);
        return;
    }

    po6::threads::mutex::hold hold(&st->lock);

    if (st->snap.has_next())
    {
        e::slice key;
        std::vector<e::slice> val;
        uint64_t ver;
        st->snap.get(&key, &val, &ver);
        size_t sz = m_daemon->m_comm.header_size()
                  + sizeof(uint64_t)
                  + pack_size(key)
                  + pack_size(val);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(m_daemon->m_comm.header_size()) << nonce << key << val;
        m_daemon->m_comm.send(us, client, RESP_SEARCH_ITEM, msg);
        st->snap.next();
    }
    else
    {
        std::auto_ptr<e::buffer> msg(e::buffer::create(m_daemon->m_comm.header_size() + sizeof(uint64_t)));
        msg->pack_at(m_daemon->m_comm.header_size()) << nonce;
        m_daemon->m_comm.send(us, client, RESP_SEARCH_DONE, msg);
        stop(us, client, search_id);
    }
}

void
search_manager :: stop(const entityid& us,
                       const entityid& client,
                       uint64_t search_id)
{
    id sid(us.get_region(), client, search_id);
    m_searches.remove(sid);
}

namespace hyperdex
{

struct _sorted_search_params
{
    _sorted_search_params(schema* _sc,
                          uint16_t _sort_by,
                          bool _maximize)
        : sc(_sc), sort_by(_sort_by), maximize(_maximize) {}
    ~_sorted_search_params() throw () {}
    schema* sc;
    uint16_t sort_by;
    bool maximize;

    private:
        _sorted_search_params(const _sorted_search_params&);
        _sorted_search_params& operator = (const _sorted_search_params&);
};

struct _sorted_search_item
{
    _sorted_search_item(_sorted_search_params* p)
        : params(p), key(), value(), version(), ref() {}
    _sorted_search_item(const _sorted_search_item& other);
    ~_sorted_search_item() throw () {}
    _sorted_search_item& operator = (const _sorted_search_item& other);
    _sorted_search_params* params;
    e::slice key;
    std::vector<e::slice> value;
    uint64_t version;
    datalayer::reference ref;
};

_sorted_search_item :: _sorted_search_item(const _sorted_search_item& other)
    : params(other.params)
    , key(other.key)
    , value(other.value)
    , version(other.version)
    , ref(other.ref)
{
}

_sorted_search_item&
_sorted_search_item :: _sorted_search_item :: operator = (const _sorted_search_item& other)
{
    params = other.params;
    key = other.key;
    value = other.value;
    version = other.version;
    ref = other.ref;
    return *this;
}

bool
operator < (const _sorted_search_item& lhs, const _sorted_search_item& rhs)
{
    assert(lhs.params == rhs.params);
    _sorted_search_params* params = lhs.params;

    if (params->sort_by >= params->sc->attrs_sz)
    {
        return false;
    }

    int cmp = 0;

    if (params->sort_by == 0)
    {
        cmp = compare_as_type(lhs.key, rhs.key, params->sc->attrs[0].type);
    }
    else
    {
        cmp = compare_as_type(lhs.value[params->sort_by - 1],
                              rhs.value[params->sort_by - 1],
                              params->sc->attrs[params->sort_by].type);
    }

    if (params->maximize)
    {
        return cmp > 0;
    }
    else
    {
        return cmp < 0;
    }
}

} // namespace hyperdex

void
search_manager :: sorted_search(const entityid& us,
                                const entityid& client,
                                uint64_t nonce,
                                std::vector<attribute_check>* checks,
                                uint64_t limit,
                                uint16_t sort_by,
                                bool maximize)
{
    schema* sc = m_daemon->m_config.get_schema(us.get_space());
    assert(sc);
    datalayer::snapshot snap;
    datalayer::returncode rc;
    rc = m_daemon->m_data.make_snapshot(us.get_region(), &checks->front(), checks->size(), &snap);
    uint64_t result = 0;

    switch (rc)
    {
        case datalayer::SUCCESS:
            break;
        case datalayer::NOT_FOUND:
        case datalayer::BAD_ENCODING:
        case datalayer::CORRUPTION:
        case datalayer::IO_ERROR:
        case datalayer::LEVELDB_ERROR:
            LOG(ERROR) << "could not make snapshot for search:  " << rc;
            result = UINT64_MAX;
            break;
        default:
            abort();
    }

    _sorted_search_params params(sc, sort_by, maximize);
    std::vector<_sorted_search_item> top_n;
    top_n.reserve(limit);

    while (snap.has_next())
    {
        top_n.push_back(_sorted_search_item(&params));
        snap.get(&top_n.back().key, &top_n.back().value, &top_n.back().version, &top_n.back().ref);
        std::push_heap(top_n.begin(), top_n.end());

        if (top_n.size() > limit)
        {
            std::pop_heap(top_n.begin(), top_n.end());
            top_n.pop_back();
        }

        snap.next();
    }

    size_t sz = m_daemon->m_comm.header_size()
              + sizeof(uint64_t)
              + sizeof(uint64_t);

    for (size_t i = 0; i < top_n.size(); ++i)
    {
        sz += pack_size(top_n[i].key) + pack_size(top_n[i].value);
    }

    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(m_daemon->m_comm.header_size());
    pa = pa << nonce << static_cast<uint64_t>(top_n.size());

    for (size_t i = 0; i < top_n.size(); ++i)
    {
        pa = pa << top_n[i].key << top_n[i].value;
    }

    m_daemon->m_comm.send(us, client, RESP_SORTED_SEARCH, msg);
}

void
search_manager :: group_keyop(const entityid& us,
                              const entityid& client,
                              uint64_t nonce,
                              std::vector<attribute_check>* checks,
                              network_msgtype mt,
                              const e::slice& remain,
                              network_msgtype resp)
{
    schema* sc = m_daemon->m_config.get_schema(us.get_space());
    assert(sc);
    datalayer::snapshot snap;
    datalayer::returncode rc;
    rc = m_daemon->m_data.make_snapshot(us.get_region(), &checks->front(), checks->size(), &snap);
    uint64_t result = 0;

    switch (rc)
    {
        case datalayer::SUCCESS:
            break;
        case datalayer::NOT_FOUND:
        case datalayer::BAD_ENCODING:
        case datalayer::CORRUPTION:
        case datalayer::IO_ERROR:
        case datalayer::LEVELDB_ERROR:
            LOG(ERROR) << "could not make snapshot for search:  " << rc;
            result = UINT64_MAX;
            break;
        default:
            abort();
    }

    while (snap.has_next() && result < UINT64_MAX)
    {
        e::slice key;
        std::vector<e::slice> val;
        uint64_t ver;
        snap.get(&key, &val, &ver);
        size_t sz = m_daemon->m_comm.header_size()
                  + sizeof(uint64_t)
                  + pack_size(key)
                  + remain.size();
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        e::buffer::packer pa = msg->pack_at(m_daemon->m_comm.header_size());
        pa = pa << static_cast<uint64_t>(0) << key;
        pa = pa.copy(remain);

        // Figure out who to talk with.
        entityid dst_ent;
        instance dst_inst;

        if (m_daemon->m_config.point_leader_entity(us.get_space(), key, &dst_ent, &dst_inst))
        {
            m_daemon->m_comm.send(us, dst_ent, mt, msg);
        }
        else
        {
            LOG(ERROR) << "group_keyop could not compute point leader (serious bug; please report)";
        }

        snap.next();
    }

    size_t sz = m_daemon->m_comm.header_size()
              + sizeof(uint64_t)
              + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(m_daemon->m_comm.header_size()) << nonce << result;
    m_daemon->m_comm.send(us, client, resp, msg);
}

void
search_manager :: count(const entityid& us,
                        const entityid& client,
                        uint64_t nonce,
                        std::vector<attribute_check>* checks)
{
    schema* sc = m_daemon->m_config.get_schema(us.get_space());
    assert(sc);
    datalayer::snapshot snap;
    datalayer::returncode rc;
    rc = m_daemon->m_data.make_snapshot(us.get_region(), &checks->front(), checks->size(), &snap);
    uint64_t result = 0;

    switch (rc)
    {
        case datalayer::SUCCESS:
            break;
        case datalayer::NOT_FOUND:
        case datalayer::BAD_ENCODING:
        case datalayer::CORRUPTION:
        case datalayer::IO_ERROR:
        case datalayer::LEVELDB_ERROR:
            LOG(ERROR) << "could not make snapshot for search:  " << rc;
            result = UINT64_MAX;
            break;
        default:
            abort();
    }

    while (snap.has_next() && result < UINT64_MAX)
    {
        ++result;
        snap.next();
    }

    size_t sz = m_daemon->m_comm.header_size()
              + sizeof(uint64_t)
              + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(m_daemon->m_comm.header_size()) << nonce << result;
    m_daemon->m_comm.send(us, client, RESP_COUNT, msg);
}

uint64_t
search_manager :: hash(const id& sid)
{
    return sid.region.hash() + sid.client.hash() + sid.search_id;
}
