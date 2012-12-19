
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

// STL
#include <algorithm>

// HyperDex
#include "common/configuration.h"
#include "common/hash.h"
#include "common/serialization.h"

using hyperdex::configuration;
using hyperdex::region_id;
using hyperdex::schema;
using hyperdex::server_id;
using hyperdex::subspace_id;
using hyperdex::virtual_server_id;

configuration :: configuration()
    : m_version(0)
    , m_addresses_by_server_id()
    , m_region_ids_by_virtual()
    , m_server_ids_by_virtual()
    , m_schemas_by_region()
    , m_subspace_ids_by_region()
    , m_subspace_ids_for_prev()
    , m_subspace_ids_for_next()
    , m_heads_by_region()
    , m_tails_by_region()
    , m_next_by_virtual()
    , m_point_leaders_by_virtual()
    , m_spaces()
{
    refill_cache();
}

configuration :: configuration(const configuration& other)
    : m_version(other.m_version)
    , m_addresses_by_server_id(other.m_addresses_by_server_id)
    , m_region_ids_by_virtual(other.m_region_ids_by_virtual)
    , m_server_ids_by_virtual(other.m_server_ids_by_virtual)
    , m_schemas_by_region(other.m_schemas_by_region)
    , m_subspace_ids_by_region(other.m_subspace_ids_by_region)
    , m_subspace_ids_for_prev(other.m_subspace_ids_for_prev)
    , m_subspace_ids_for_next(other.m_subspace_ids_for_next)
    , m_heads_by_region(other.m_heads_by_region)
    , m_tails_by_region(other.m_tails_by_region)
    , m_next_by_virtual(other.m_next_by_virtual)
    , m_point_leaders_by_virtual(other.m_point_leaders_by_virtual)
    , m_spaces(other.m_spaces)
{
    refill_cache();
}

configuration :: ~configuration() throw ()
{
}

uint64_t
configuration :: version() const
{
    return m_version;
}

po6::net::location
configuration :: get_address(const server_id& id) const
{
    std::vector<uint64_location_t>::const_iterator it;
    it = std::lower_bound(m_addresses_by_server_id.begin(),
                          m_addresses_by_server_id.begin(),
                          uint64_location_t(id.get(), po6::net::location()));

    if (it != m_addresses_by_server_id.end() && it->first == id.get())
    {
        return it->second;
    }

    return po6::net::location();
}

region_id
configuration :: get_region_id(const virtual_server_id& id) const
{
    std::vector<pair_uint64_t>::const_iterator it;
    it = std::lower_bound(m_region_ids_by_virtual.begin(),
                          m_region_ids_by_virtual.end(),
                          pair_uint64_t(id.get(), 0));

    if (it != m_region_ids_by_virtual.end() && it->first == id.get())
    {
        return region_id(it->second);
    }

    return region_id();
}

server_id
configuration :: get_server_id(const virtual_server_id& id) const
{
    std::vector<pair_uint64_t>::const_iterator it;
    it = std::lower_bound(m_server_ids_by_virtual.begin(),
                          m_server_ids_by_virtual.end(),
                          pair_uint64_t(id.get(), 0));

    if (it != m_server_ids_by_virtual.end() && it->first == id.get())
    {
        return server_id(it->second);
    }

    return server_id();
}

const schema*
configuration :: get_schema(const char* sname) const
{
    for (size_t s = 0; s < m_spaces.size(); ++s)
    {
        if (strcmp(sname, m_spaces[s].name) == 0)
        {
            return &m_spaces[s].schema;
        }
    }

    return NULL;
}

const schema*
configuration :: get_schema(const region_id& ri) const
{
    std::vector<uint64_schema_t>::const_iterator it;
    it = std::lower_bound(m_schemas_by_region.begin(),
                          m_schemas_by_region.end(),
                          uint64_schema_t(ri.get(), NULL));

    if (it != m_schemas_by_region.end() && it->first == ri.get())
    {
        return it->second;
    }

    return NULL;
}

subspace_id
configuration :: subspace_of(const region_id& ri) const
{
    std::vector<pair_uint64_t>::const_iterator it;
    it = std::lower_bound(m_subspace_ids_by_region.begin(),
                          m_subspace_ids_by_region.end(),
                          pair_uint64_t(ri.get(), 0));

    if (it != m_subspace_ids_by_region.end() && it->first == ri.get())
    {
        return subspace_id(it->second);
    }

    return subspace_id();
}

subspace_id
configuration :: subspace_prev(const subspace_id& ss) const
{
    std::vector<pair_uint64_t>::const_iterator it;
    it = std::lower_bound(m_subspace_ids_for_prev.begin(),
                          m_subspace_ids_for_prev.end(),
                          pair_uint64_t(ss.get(), 0));

    if (it != m_subspace_ids_for_prev.end() && it->first == ss.get())
    {
        return subspace_id(it->second);
    }

    return subspace_id();
}

subspace_id
configuration :: subspace_next(const subspace_id& ss) const
{
    std::vector<pair_uint64_t>::const_iterator it;
    it = std::lower_bound(m_subspace_ids_for_next.begin(),
                          m_subspace_ids_for_next.end(),
                          pair_uint64_t(ss.get(), 0));

    if (it != m_subspace_ids_for_next.end() && it->first == ss.get())
    {
        return subspace_id(it->second);
    }

    return subspace_id();
}

virtual_server_id
configuration :: head_of_region(const region_id& ri) const
{
    std::vector<pair_uint64_t>::const_iterator it;
    it = std::lower_bound(m_heads_by_region.begin(),
                          m_heads_by_region.end(),
                          pair_uint64_t(ri.get(), 0));

    if (it != m_heads_by_region.end() && it->first == ri.get())
    {
        return virtual_server_id(it->second);
    }

    return virtual_server_id();
}

virtual_server_id
configuration :: tail_of_region(const region_id& ri) const
{
    std::vector<pair_uint64_t>::const_iterator it;
    it = std::lower_bound(m_tails_by_region.begin(),
                          m_tails_by_region.end(),
                          pair_uint64_t(ri.get(), 0));

    if (it != m_tails_by_region.end() && it->first == ri.get())
    {
        return virtual_server_id(it->second);
    }

    return virtual_server_id();
}

virtual_server_id
configuration :: next_in_region(const virtual_server_id& vsi) const
{
    std::vector<pair_uint64_t>::const_iterator it;
    it = std::lower_bound(m_next_by_virtual.begin(),
                          m_next_by_virtual.end(),
                          pair_uint64_t(vsi.get(), 0));

    if (it != m_next_by_virtual.end() && it->first == vsi.get())
    {
        return virtual_server_id(it->second);
    }

    return virtual_server_id();
}

bool
configuration :: is_point_leader(const virtual_server_id& e) const
{
    return std::binary_search(m_point_leaders_by_virtual.begin(),
                              m_point_leaders_by_virtual.end(),
                              e.get());
}

virtual_server_id
configuration :: point_leader(const char* sname, const e::slice& key)
{
    for (size_t s = 0; s < m_spaces.size(); ++s)
    {
        if (strcmp(sname, m_spaces[s].name) != 0)
        {
            continue;
        }

        uint64_t h;
        hash(m_spaces[s].schema, key, &h);

        for (size_t pl = 0; pl < m_spaces[s].subspaces[0].regions.size(); ++pl)
        {
            if (m_spaces[s].subspaces[0].regions[pl].lower_coord[0] <= h &&
                h < m_spaces[s].subspaces[0].regions[pl].upper_coord[0])
            {
                assert(!m_spaces[s].subspaces[0].regions[pl].replicas.empty());
                return m_spaces[s].subspaces[0].regions[pl].replicas[0].vsi;
            }
        }

        abort();
    }

    return virtual_server_id();
}

virtual_server_id
configuration :: point_leader(const region_id& rid, const e::slice& key)
{
    for (size_t s = 0; s < m_spaces.size(); ++s)
    {
        for (size_t ss = 0; ss < m_spaces[s].subspaces.size(); ++ss)
        {
            for (size_t r = 0; r < m_spaces[s].subspaces[ss].regions.size(); ++r)
            {
                if (m_spaces[s].subspaces[ss].regions[r].id != rid)
                {
                    continue;
                }

                uint64_t h;
                hash(m_spaces[s].schema, key, &h);

                for (size_t pl = 0; pl < m_spaces[s].subspaces[0].regions.size(); ++pl)
                {
                    if (m_spaces[s].subspaces[0].regions[pl].lower_coord[0] <= h &&
                        h < m_spaces[s].subspaces[0].regions[pl].upper_coord[0])
                    {
                        assert(!m_spaces[s].subspaces[0].regions[pl].replicas.empty());
                        return m_spaces[s].subspaces[0].regions[pl].replicas[0].vsi;
                    }
                }

                abort();
            }
        }
    }

    return virtual_server_id();
}

bool
configuration :: subspace_adjacent(const virtual_server_id& lhs, const virtual_server_id& rhs) const
{
    region_id rlhs = get_region_id(lhs);
    region_id rrhs = get_region_id(rhs);
    subspace_id slhs = subspace_of(rlhs);
    subspace_id srhs = subspace_of(rrhs);
    return subspace_next(slhs) == srhs &&
           lhs == tail_of_region(rlhs) &&
           rhs == head_of_region(rrhs);
}

void
configuration :: lookup_region(const subspace_id& ssid,
                               const std::vector<uint64_t>& hashes,
                               region_id* rid) const
{
    for (size_t s = 0; s < m_spaces.size(); ++s)
    {
        for (size_t ss = 0; ss < m_spaces[s].subspaces.size(); ++ss)
        {
            if (m_spaces[s].subspaces[ss].id != ssid)
            {
                continue;
            }

            assert(m_spaces[s].schema.attrs_sz == hashes.size());

            for (size_t r = 0; r < m_spaces[s].subspaces[ss].regions.size(); ++r)
            {
                bool matches = true;

                for (size_t a = 0; matches && a < m_spaces[s].subspaces[ss].attrs.size(); ++a)
                {
                    assert(a < m_spaces[s].schema.attrs_sz);
                    matches &= m_spaces[s].subspaces[ss].regions[r].lower_coord[a] <= hashes[m_spaces[s].subspaces[ss].attrs[a]] &&
                               hashes[m_spaces[s].subspaces[ss].attrs[a]] < m_spaces[s].subspaces[ss].regions[r].upper_coord[a];
                }

                if (matches)
                {
                    *rid = m_spaces[s].subspaces[ss].regions[r].id;
                    return;
                }
            }
        }
    }

    *rid = region_id();
}

// XXX THIS RETURNS ALL REGIONS IN THE SPACE AND DOES NO FILTERING
void
configuration :: lookup_search(const char* space,
                               const std::vector<attribute_check>& chks,
                               std::vector<virtual_server_id>* servers) const
{
    servers->clear();

    for (size_t s = 0; s < m_spaces.size(); ++s)
    {
        if (strcmp(space, m_spaces[s].name) != 0)
        {
            continue;
        }

        assert(!m_spaces[s].subspaces.empty());

        for (size_t r = 0; r < m_spaces[s].subspaces[0].regions.size(); ++r)
        {
            assert(!m_spaces[s].subspaces[0].regions[r].replicas.empty());
            servers->push_back(m_spaces[s].subspaces[0].regions[r].replicas[0].vsi);
        }
    }
}

configuration&
configuration :: operator = (const configuration& rhs)
{
    m_version = rhs.m_version;
    m_addresses_by_server_id = rhs.m_addresses_by_server_id;
    m_region_ids_by_virtual = rhs.m_region_ids_by_virtual;
    m_server_ids_by_virtual = rhs.m_server_ids_by_virtual;
    m_schemas_by_region = rhs.m_schemas_by_region;
    m_subspace_ids_by_region = rhs.m_subspace_ids_by_region;
    m_subspace_ids_for_prev = rhs.m_subspace_ids_for_prev;
    m_subspace_ids_for_next = rhs.m_subspace_ids_for_next;
    m_heads_by_region = rhs.m_heads_by_region;
    m_tails_by_region = rhs.m_tails_by_region;
    m_next_by_virtual = rhs.m_next_by_virtual;
    m_point_leaders_by_virtual = rhs.m_point_leaders_by_virtual;
    m_spaces = rhs.m_spaces;
    refill_cache();
    return *this;
}

void
configuration :: refill_cache()
{
    m_region_ids_by_virtual.clear();
    m_server_ids_by_virtual.clear();
    m_schemas_by_region.clear();
    m_subspace_ids_by_region.clear();
    m_subspace_ids_for_prev.clear();
    m_subspace_ids_for_next.clear();
    m_heads_by_region.clear();
    m_tails_by_region.clear();
    m_next_by_virtual.clear();
    m_point_leaders_by_virtual.clear();

    for (size_t w = 0; w < m_spaces.size(); ++w)
    {
        space& s(m_spaces[w]);

        for (size_t x = 0; x < s.subspaces.size(); ++x)
        {
            subspace& ss(s.subspaces[x]);

            if (x > 0)
            {
                m_subspace_ids_for_prev.push_back(std::make_pair(ss.id.get(),
                                                                 s.subspaces[x - 1].id.get()));
            }

            if (x + 1 < s.subspaces.size())
            {
                m_subspace_ids_for_next.push_back(std::make_pair(ss.id.get(),
                                                                 s.subspaces[x + 1].id.get()));
            }

            for (size_t y = 0; y < ss.regions.size(); ++y)
            {
                region& r(ss.regions[y]);
                assert(!r.replicas.empty());

                if (x == 0)
                {
                    m_point_leaders_by_virtual.push_back(r.replicas[0].vsi.get());
                }

                m_heads_by_region.push_back(std::make_pair(r.id.get(),
                                                           r.replicas[0].vsi.get()));
                m_tails_by_region.push_back(std::make_pair(r.id.get(),
                                                           r.replicas[r.replicas.size() - 1].vsi.get()));
                m_schemas_by_region.push_back(std::make_pair(r.id.get(), &s.schema));
                m_subspace_ids_by_region.push_back(std::make_pair(r.id.get(), ss.id.get()));

                for (size_t z = 0; z < r.replicas.size(); ++z)
                {
                    m_region_ids_by_virtual.push_back(std::make_pair(r.replicas[z].vsi.get(),
                                                                     r.id.get()));
                    m_server_ids_by_virtual.push_back(std::make_pair(r.replicas[z].vsi.get(),
                                                                     r.replicas[z].si.get()));

                    if (z + 1 < r.replicas.size())
                    {
                        m_next_by_virtual.push_back(std::make_pair(r.replicas[z].vsi.get(),
                                                                   r.replicas[z + 1].vsi.get()));
                    }
                }
            }
        }
    }

    std::sort(m_addresses_by_server_id.begin(), m_addresses_by_server_id.end());
    std::sort(m_region_ids_by_virtual.begin(), m_region_ids_by_virtual.end());
    std::sort(m_server_ids_by_virtual.begin(), m_server_ids_by_virtual.end());
    std::sort(m_schemas_by_region.begin(), m_schemas_by_region.end());
    std::sort(m_subspace_ids_by_region.begin(), m_subspace_ids_by_region.end());
    std::sort(m_subspace_ids_for_prev.begin(), m_subspace_ids_for_prev.end());
    std::sort(m_subspace_ids_for_next.begin(), m_subspace_ids_for_next.end());
    std::sort(m_heads_by_region.begin(), m_heads_by_region.end());
    std::sort(m_tails_by_region.begin(), m_tails_by_region.end());
    std::sort(m_next_by_virtual.begin(), m_next_by_virtual.end());
    std::sort(m_point_leaders_by_virtual.begin(), m_point_leaders_by_virtual.end());
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, configuration& c)
{
    uint64_t num_servers;
    uint64_t num_spaces;
    up = up >> c.m_version >> num_servers >> num_spaces;
    c.m_spaces.reserve(num_spaces);

    for (size_t i = 0; !up.error() && i < num_servers; ++i)
    {
        uint64_t id;
        po6::net::location loc;
        up = up >> id >> loc;
        c.m_addresses_by_server_id.push_back(std::make_pair(id, loc));
    }

    for (size_t i = 0; !up.error() && i < num_spaces; ++i)
    {
        space s;
        up = up >> s;
        c.m_spaces.push_back(s);
    }

    c.refill_cache();
    return up;
}
