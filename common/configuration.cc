
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
#include <algorithm>

// HyperDex
#include "common/configuration.h"
#include "common/hash.h"
#include "common/range_searches.h"
#include "common/serialization.h"

using hyperdex::capture_id;
using hyperdex::configuration;
using hyperdex::region_id;
using hyperdex::schema;
using hyperdex::server_id;
using hyperdex::subspace;
using hyperdex::subspace_id;
using hyperdex::virtual_server_id;

configuration :: configuration()
    : m_cluster(0)
    , m_version(0)
    , m_addresses_by_server_id()
    , m_region_ids_by_virtual()
    , m_server_ids_by_virtual()
    , m_schemas_by_region()
    , m_subspaces_by_region()
    , m_subspace_ids_by_region()
    , m_subspace_ids_for_prev()
    , m_subspace_ids_for_next()
    , m_heads_by_region()
    , m_tails_by_region()
    , m_next_by_virtual()
    , m_point_leaders_by_virtual()
    , m_spaces()
    , m_captures()
    , m_transfers()
{
    refill_cache();
}

configuration :: configuration(const configuration& other)
    : m_cluster(other.m_cluster)
    , m_version(other.m_version)
    , m_addresses_by_server_id(other.m_addresses_by_server_id)
    , m_region_ids_by_virtual(other.m_region_ids_by_virtual)
    , m_server_ids_by_virtual(other.m_server_ids_by_virtual)
    , m_schemas_by_region(other.m_schemas_by_region)
    , m_subspaces_by_region(other.m_subspaces_by_region)
    , m_subspace_ids_by_region(other.m_subspace_ids_by_region)
    , m_subspace_ids_for_prev(other.m_subspace_ids_for_prev)
    , m_subspace_ids_for_next(other.m_subspace_ids_for_next)
    , m_heads_by_region(other.m_heads_by_region)
    , m_tails_by_region(other.m_tails_by_region)
    , m_next_by_virtual(other.m_next_by_virtual)
    , m_point_leaders_by_virtual(other.m_point_leaders_by_virtual)
    , m_spaces(other.m_spaces)
    , m_captures(other.m_captures)
    , m_transfers(other.m_transfers)
{
    refill_cache();
}

configuration :: ~configuration() throw ()
{
}

uint64_t
configuration :: cluster() const
{
    return m_cluster;
}

uint64_t
configuration :: version() const
{
    return m_version;
}

void
configuration :: get_all_addresses(std::vector<std::pair<server_id, po6::net::location> >* addrs) const
{
    addrs->resize(m_addresses_by_server_id.size());

    for (size_t i = 0; i < m_addresses_by_server_id.size(); ++i)
    {
        (*addrs)[i].first = server_id(m_addresses_by_server_id[i].first);
        (*addrs)[i].second = m_addresses_by_server_id[i].second;
    }
}

po6::net::location
configuration :: get_address(const server_id& id) const
{
    std::vector<uint64_location_t>::const_iterator it;
    it = std::lower_bound(m_addresses_by_server_id.begin(),
                          m_addresses_by_server_id.end(),
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
            return &m_spaces[s].sc;
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

const subspace*
configuration :: get_subspace(const region_id& ri) const
{
    std::vector<uint64_subspace_t>::const_iterator it;
    it = std::lower_bound(m_subspaces_by_region.begin(),
                          m_subspaces_by_region.end(),
                          uint64_subspace_t(ri.get(), NULL));

    if (it != m_subspaces_by_region.end() && it->first == ri.get())
    {
        return it->second;
    }

    return NULL;
}

virtual_server_id
configuration :: get_virtual(const region_id& ri, const server_id& si) const
{
    for (size_t w = 0; w < m_spaces.size(); ++w)
    {
        const space& s(m_spaces[w]);

        for (size_t x = 0; x < s.subspaces.size(); ++x)
        {
            const subspace& ss(s.subspaces[x]);

            for (size_t y = 0; y < ss.regions.size(); ++y)
            {
                const region& r(ss.regions[y]);

                if (r.id != ri)
                {
                    continue;
                }

                for (size_t z = 0; z < r.replicas.size(); ++z)
                {
                    if (r.replicas[z].si == si)
                    {
                        return r.replicas[z].vsi;
                    }
                }

                return virtual_server_id();
            }
        }
    }

    return virtual_server_id();
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

void
configuration :: point_leaders(const server_id& si, std::vector<region_id>* servers) const
{
    for (size_t s = 0; s < m_spaces.size(); ++s)
    {
        for (size_t ss = 0; ss < m_spaces[s].subspaces.size(); ++ss)
        {
            for (size_t r = 0; r < m_spaces[s].subspaces[ss].regions.size(); ++r)
            {
                if (!m_spaces[s].subspaces[ss].regions[r].replicas.empty() &&
                    m_spaces[s].subspaces[ss].regions[r].replicas[0].si == si)
                {
                    servers->push_back(m_spaces[s].subspaces[ss].regions[r].id);
                }
            }
        }
    }
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
        hash(m_spaces[s].sc, key, &h);

        for (size_t pl = 0; pl < m_spaces[s].subspaces[0].regions.size(); ++pl)
        {
            if (m_spaces[s].subspaces[0].regions[pl].lower_coord[0] <= h &&
                h <= m_spaces[s].subspaces[0].regions[pl].upper_coord[0])
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
                hash(m_spaces[s].sc, key, &h);

                for (size_t pl = 0; pl < m_spaces[s].subspaces[0].regions.size(); ++pl)
                {
                    if (m_spaces[s].subspaces[0].regions[pl].lower_coord[0] <= h &&
                        h <= m_spaces[s].subspaces[0].regions[pl].upper_coord[0])
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
configuration :: captures(std::vector<hyperdex::capture>* cs) const
{
    *cs = m_captures;
}

bool
configuration :: is_captured_region(const capture_id& ci) const
{
    for (size_t i = 0; i < m_captures.size(); ++i)
    {
        if (m_captures[i].id == ci)
        {
            return true;
        }
    }

    return false;
}

capture_id
configuration :: capture_for(const region_id& ri) const
{
    for (size_t i = 0; i < m_captures.size(); ++i)
    {
        if (m_captures[i].rid == ri)
        {
            return m_captures[i].id;
        }
    }

    return capture_id();
}

bool
configuration :: is_server_blocked_by_live_transfer(const server_id& si, const region_id& id) const
{
    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        if (m_transfers[i].src != si || m_transfers[i].rid != id)
        {
            continue;
        }

        virtual_server_id tail = tail_of_region(m_transfers[i].rid);

        if (tail == m_transfers[i].vdst)
        {
            return true;
        }

        assert(tail == m_transfers[i].vsrc);
    }

    return false;
}

bool
configuration :: is_transfer_live(const hyperdex::transfer_id& id) const
{
    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        if (m_transfers[i].id != id)
        {
            continue;
        }

        return m_transfers[i].vdst == tail_of_region(m_transfers[i].rid);
    }

    return false;
}

void
configuration :: transfer_in_regions(const hyperdex::server_id& si, std::vector<hyperdex::transfer>* transfers) const
{
    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        if (m_transfers[i].dst == si)
        {
            transfers->push_back(m_transfers[i]);
        }
    }
}

void
configuration :: transfer_out_regions(const hyperdex::server_id& si, std::vector<hyperdex::transfer>* transfers) const
{
    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        if (m_transfers[i].src == si)
        {
            transfers->push_back(m_transfers[i]);
        }
    }
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

            assert(m_spaces[s].sc.attrs_sz == hashes.size());

            for (size_t r = 0; r < m_spaces[s].subspaces[ss].regions.size(); ++r)
            {
                bool matches = true;

                for (size_t a = 0; matches && a < m_spaces[s].subspaces[ss].attrs.size(); ++a)
                {
                    assert(a < m_spaces[s].sc.attrs_sz);
                    matches &= m_spaces[s].subspaces[ss].regions[r].lower_coord[a] <= hashes[m_spaces[s].subspaces[ss].attrs[a]] &&
                               hashes[m_spaces[s].subspaces[ss].attrs[a]] <= m_spaces[s].subspaces[ss].regions[r].upper_coord[a];
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

void
configuration :: lookup_search(const char* space_name,
                               const std::vector<hyperdex::attribute_check>& chks,
                               std::vector<virtual_server_id>* servers) const
{
    const space* s = NULL;

    for (size_t i = 0; i < m_spaces.size(); ++i)
    {
        if (strcmp(space_name, m_spaces[i].name) == 0)
        {
            s = &m_spaces[i];
            break;
        }
    }

    if (!s)
    {
        servers->clear();
        return;
    }

    std::vector<hyperdex::range> ranges;
    range_searches(chks, &ranges);

    for (size_t i = 0; i < ranges.size(); ++i)
    {
        if (ranges[i].invalid)
        {
            servers->clear();
            return;
        }
    }

    bool initialized = false;
    std::vector<virtual_server_id> smallest_server_set;

    for (size_t i = 0; i < s->subspaces.size(); ++i)
    {
        std::vector<virtual_server_id> this_server_set;

        for (size_t j = 0; j < s->subspaces[i].regions.size(); ++j)
        {
            const region& reg(s->subspaces[i].regions[j]);

            if (reg.replicas.empty())
            {
                continue;
            }

            bool exclude = false;

            for (size_t k = 0; !exclude && k < ranges.size(); ++k)
            {
                assert(reg.lower_coord.size() == reg.upper_coord.size());
                uint16_t attr = UINT16_MAX;

                for (size_t l = 0; l < s->subspaces[i].attrs.size(); ++l)
                {
                    if (s->subspaces[i].attrs[l] == ranges[k].attr)
                    {
                        attr = l;
                        break;
                    }
                }

                if (attr == UINT16_MAX)
                {
                    continue;
                }

                if (attr >= reg.lower_coord.size() ||
                    reg.lower_coord[attr] > reg.upper_coord[attr])
                {
                    servers->clear();
                    return;
                }

                if (ranges[k].type == HYPERDATATYPE_STRING &&
                    ranges[k].has_start && ranges[k].has_end &&
                    ranges[k].start == ranges[k].end)
                {
                    uint64_t h = hash(ranges[k].type, ranges[k].start);

                    if (reg.lower_coord[attr] > h ||
                        reg.upper_coord[attr] < h)
                    {
                        exclude = true;
                    }
                }

                if (ranges[k].type == HYPERDATATYPE_INT64 ||
                    ranges[k].type == HYPERDATATYPE_FLOAT)
                {
                    if (ranges[k].has_start)
                    {
                        uint64_t h = hash(ranges[k].type, ranges[k].start);

                        if (reg.upper_coord[attr] < h)
                        {
                            exclude = true;
                        }
                    }

                    if (ranges[k].has_end)
                    {
                        uint64_t h = hash(ranges[k].type, ranges[k].end);

                        if (reg.lower_coord[attr] > h)
                        {
                            exclude = true;
                        }
                    }
                }
            }

            if (!exclude)
            {
                this_server_set.push_back(reg.replicas.back().vsi);
            }
        }

        if (!initialized ||
            (!this_server_set.empty() &&
             this_server_set.size() <= smallest_server_set.size()))
        {
            smallest_server_set.swap(this_server_set);
            initialized = true;
        }
    }

    servers->swap(smallest_server_set);
}

void
configuration :: dump(std::ostream& out) const
{
    out << "cluster " << m_cluster << "\n";
    out << "version " << m_version << "\n";

    for (size_t i = 0; i < m_addresses_by_server_id.size(); ++i)
    {
        out << "server "
            << m_addresses_by_server_id[i].first << " "
            << m_addresses_by_server_id[i].second << " "
            << "available" << "\n";
    }

    for (size_t w = 0; w < m_spaces.size(); ++w)
    {
        const space& s(m_spaces[w]);
        out << "space " << s.id.get() << " " << s.name << "\n";
        out << "  fault_tolerance " << s.fault_tolerance << "\n";
        out << "  schema" << "\n";

        for (size_t i = 0; i < s.sc.attrs_sz; ++i)
        {
            out << "    attribute "
                << s.sc.attrs[i].name << " "
                << s.sc.attrs[i].type << "\n";
        }

        for (size_t x = 0; x < s.subspaces.size(); ++x)
        {
            const subspace& ss(s.subspaces[x]);
            out << "  subspace " << ss.id.get() << "\n";
            out << "    attributes";

            for (size_t i = 0; i < ss.attrs.size(); ++i)
            {
                out << " " << s.sc.attrs[ss.attrs[i]].name;
            }

            out << "\n";
            out << "    indices";

            for (size_t i = 0; i < ss.indices.size(); ++i)
            {
                out << " " << s.sc.attrs[ss.indices[i]].name;
            }

            out << "\n";

            for (size_t y = 0; y < ss.regions.size(); ++y)
            {
                const region& r(ss.regions[y]);
                out << "    region " << r.id.get() << " lower=<";
                bool first = true;

                for (size_t i = 0; i < r.lower_coord.size(); ++i)
                {
                    out << (first ? "" : ", ") << r.lower_coord[i];
                    first = false;
                }

                out << "> upper=<";
                first = true;

                for (size_t i = 0; i < r.upper_coord.size(); ++i)
                {
                    out << (first ? "" : ", ") << r.upper_coord[i];
                    first = false;
                }

                out << "> replicas=[";
                first = true;

                for (size_t z = 0; z < r.replicas.size(); ++z)
                {
                    const replica& rr(r.replicas[z]);
                    out << (first ? "" : ", ") << "(id=" << rr.si << " vid=" << rr.vsi << ")";
                    first = false;
                }

                out << "]" << std::endl;
            }
        }
    }

    for (size_t i = 0; i < m_captures.size(); ++i)
    {
        out << m_captures[i] << std::endl;
    }

    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        out << m_transfers[i] << std::endl;
    }
}

configuration&
configuration :: operator = (const configuration& rhs)
{
    if (this == &rhs)
    {
        return *this;
    }

    m_cluster = rhs.m_cluster;
    m_version = rhs.m_version;
    m_addresses_by_server_id = rhs.m_addresses_by_server_id;
    m_region_ids_by_virtual = rhs.m_region_ids_by_virtual;
    m_server_ids_by_virtual = rhs.m_server_ids_by_virtual;
    m_schemas_by_region = rhs.m_schemas_by_region;
    m_subspaces_by_region = rhs.m_subspaces_by_region;
    m_subspace_ids_by_region = rhs.m_subspace_ids_by_region;
    m_subspace_ids_for_prev = rhs.m_subspace_ids_for_prev;
    m_subspace_ids_for_next = rhs.m_subspace_ids_for_next;
    m_heads_by_region = rhs.m_heads_by_region;
    m_tails_by_region = rhs.m_tails_by_region;
    m_next_by_virtual = rhs.m_next_by_virtual;
    m_point_leaders_by_virtual = rhs.m_point_leaders_by_virtual;
    m_spaces = rhs.m_spaces;
    m_captures = rhs.m_captures;
    m_transfers = rhs.m_transfers;
    refill_cache();
    return *this;
}

void
configuration :: refill_cache()
{
    m_region_ids_by_virtual.clear();
    m_server_ids_by_virtual.clear();
    m_schemas_by_region.clear();
    m_subspaces_by_region.clear();
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
                m_schemas_by_region.push_back(std::make_pair(r.id.get(), &s.sc));
                m_subspaces_by_region.push_back(std::make_pair(r.id.get(), &ss));
                m_subspace_ids_by_region.push_back(std::make_pair(r.id.get(), ss.id.get()));

                if (r.replicas.empty())
                {
                    continue;
                }

                if (x == 0)
                {
                    m_point_leaders_by_virtual.push_back(r.replicas[0].vsi.get());
                }

                m_heads_by_region.push_back(std::make_pair(r.id.get(),
                                                           r.replicas[0].vsi.get()));
                m_tails_by_region.push_back(std::make_pair(r.id.get(),
                                                           r.replicas[r.replicas.size() - 1].vsi.get()));

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

    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        transfer& xfer(m_transfers[i]);
        m_region_ids_by_virtual.push_back(std::make_pair(xfer.vsrc.get(), xfer.rid.get()));
        m_server_ids_by_virtual.push_back(std::make_pair(xfer.vdst.get(), xfer.dst.get()));
    }

    std::sort(m_addresses_by_server_id.begin(), m_addresses_by_server_id.end());
    std::sort(m_region_ids_by_virtual.begin(), m_region_ids_by_virtual.end());
    std::sort(m_server_ids_by_virtual.begin(), m_server_ids_by_virtual.end());
    std::sort(m_schemas_by_region.begin(), m_schemas_by_region.end());
    std::sort(m_subspaces_by_region.begin(), m_subspaces_by_region.end());
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
    uint64_t num_captures;
    uint64_t num_transfers;
    up = up >> c.m_cluster >> c.m_version
            >> num_servers >> num_spaces
            >> num_captures >> num_transfers;
    c.m_addresses_by_server_id.clear();
    c.m_addresses_by_server_id.reserve(num_servers);

    for (size_t i = 0; !up.error() && i < num_servers; ++i)
    {
        uint64_t id;
        po6::net::location loc;
        up = up >> id >> loc;
        c.m_addresses_by_server_id.push_back(std::make_pair(id, loc));
    }

    c.m_spaces.clear();
    c.m_spaces.reserve(num_spaces);

    for (size_t i = 0; !up.error() && i < num_spaces; ++i)
    {
        space s;
        up = up >> s;
        c.m_spaces.push_back(s);
    }

    c.m_captures.clear();
    c.m_captures.reserve(num_captures);

    for (size_t i = 0; !up.error() && i < num_captures; ++i)
    {
        capture cap;
        up = up >> cap;
        c.m_captures.push_back(cap);
    }

    c.m_transfers.clear();
    c.m_transfers.reserve(num_transfers);

    for (size_t i = 0; !up.error() && i < num_transfers; ++i)
    {
        transfer xfer;
        up = up >> xfer;
        c.m_transfers.push_back(xfer);
    }

    c.refill_cache();
    return up;
}
