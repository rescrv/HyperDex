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

// C
#include <cassert>
#include <stdint.h>

// STL
#include <algorithm>
#include <sstream>
#include <vector>

// e
#include <e/convert.h>

// HyperDex
#include "hyperdex/hyperdex/configuration.h"

const uint32_t hyperdex::configuration::CLIENTSPACE = UINT32_MAX;
const uint32_t hyperdex::configuration::TRANSFERSPACE = UINT32_MAX - 1;

hyperdex :: configuration :: configuration()
    : m_hosts()
    , m_space_assignment()
    , m_spaces()
    , m_subspaces()
    , m_regions()
    , m_entities()
    , m_transfers()
    , m_repl_hashers()
    , m_disk_hashers()
{
}

hyperdex :: configuration :: ~configuration() throw ()
{
}

bool
hyperdex :: configuration :: add_line(const std::string& line)
{
    std::istringstream istr(line);
    std::string s;
    std::string command;
    istr >> command;

    if (command == "host")
    {
        std::string id;
        po6::net::ipaddr ip;
        uint16_t iport;
        uint16_t iver;
        uint16_t oport;
        uint16_t over;
        std::string trailing;
        istr >> id >> ip >> iport >> iver >> oport >> over;

        if (istr.eof() && !istr.bad() && !istr.fail()
                && m_hosts.find(id) == m_hosts.end())
        {
            m_hosts[id] = instance(po6::net::location(ip, iport), iver,
                                   po6::net::location(ip, oport), over);
            return true;
        }
        else
        {
            return false;
        }
    }
    else if (command == "space")
    {
        std::string name;
        std::string id_s;
        uint32_t id;
        std::string dim;
        std::vector<std::string> dims;
        istr >> id_s >> name;

        try
        {
            id = e::convert::to_uint32_t(id_s);
        }
        catch (...)
        {
            return false;
        }

        while (istr.good())
        {
            istr >> dim;

            if (!istr.fail())
            {
                dims.push_back(dim);
            }
        }

        if (istr.eof() && !istr.bad() && !istr.fail()
                && m_space_assignment.find(name) == m_space_assignment.end()
                && m_spaces.find(spaceid(id)) == m_spaces.end()
                && dims.size() > 0)
        {
            m_space_assignment[name] = spaceid(id);
            m_spaces.insert(std::make_pair(spaceid(id), dims));
            return true;
        }
        else
        {
            return false;
        }
    }
    else if (command == "subspace")
    {
        std::string spacename;
        uint32_t subspacenum;
        std::string hash;
        std::vector<std::string> hashes;
        istr >> spacename >> subspacenum;

        while (istr.good())
        {
            istr >> hash;

            if (!istr.fail())
            {
                hashes.push_back(hash);
            }
        }

        std::map<std::string, spaceid>::iterator sai = m_space_assignment.find(spacename);
        std::map<spaceid, std::vector<std::string> >::iterator si;

        if (istr.eof() && !istr.bad() && !istr.fail()
                && sai != m_space_assignment.end()
                && (si = m_spaces.find(sai->second)) != m_spaces.end()
                && std::distance(m_subspaces.lower_bound(subspaceid(si->first, 0)),
                                 m_subspaces.upper_bound(subspaceid(si->first, UINT16_MAX))) == subspacenum
                && hashes.size() > 0)
        {
            const std::vector<std::string>& spacedims(si->second);
            std::vector<hyperspacehashing::hash_t> repl_funcs(spacedims.size(), hyperspacehashing::NONE);
            std::vector<hyperspacehashing::hash_t> disk_funcs(spacedims.size(), hyperspacehashing::NONE);
            std::vector<bool> bitmask(spacedims.size(), true);

            if (spacedims.size() * 2 != hashes.size())
            {
                return false;
            }

            for (size_t i = 0; i < spacedims.size(); ++i)
            {
                if (subspacenum == 0 && i > 0 && hashes[2 * i] != "none")
                {
                    return false;
                }

                if (subspacenum == 0 && i == 0 && hashes[2 * i] == "none")
                {
                    return false;
                }

                if (hashes[2 * i] == "equality")
                {
                    repl_funcs[i] = hyperspacehashing::EQUALITY;
                }
                else if (hashes[2 * i] == "range")
                {
                    repl_funcs[i] = hyperspacehashing::RANGE;
                }
                else if (hashes[2 * i] == "none")
                {
                    bitmask[i] = false;
                    repl_funcs[i] = hyperspacehashing::NONE;
                }
                else
                {
                    return false;
                }

                if (hashes[2 * i + 1] == "equality")
                {
                    disk_funcs[i] = hyperspacehashing::EQUALITY;
                }
                else if (hashes[2 * i + 1] == "range")
                {
                    disk_funcs[i] = hyperspacehashing::RANGE;
                }
                else if (hashes[2 * i + 1] == "none")
                {
                    disk_funcs[i] = hyperspacehashing::NONE;
                }
                else
                {
                    return false;
                }
            }

            m_subspaces.insert(std::make_pair(subspaceid(si->first, subspacenum), bitmask));
            m_repl_hashers.insert(std::make_pair(subspaceid(si->first, subspacenum),
                                                 hyperspacehashing::prefix::hasher(repl_funcs)));
            m_disk_hashers.insert(std::make_pair(subspaceid(si->first, subspacenum),
                                                 hyperspacehashing::mask::hasher(disk_funcs)));
            return true;
        }
        else
        {
            return false;
        }
    }
    else if (command == "region")
    {
        std::string spacename;
        uint32_t subspacenum;
        std::string prefix_s;
        uint8_t prefix;
        std::string mask_s;
        uint64_t mask;
        std::string host;
        std::vector<std::string> _hosts;

        istr >> spacename >> subspacenum >> prefix_s >> mask_s;

        try
        {
            prefix = e::convert::to_uint8_t(prefix_s);
            mask = e::convert::to_uint64_t(mask_s);
        }
        catch (...)
        {
            return false;
        }

        while (istr.good())
        {
            istr >> host;

            if (!istr.fail())
            {
                _hosts.push_back(host);
            }
        }

        // Check for valid hosts lists
        for (size_t i = 0; i < _hosts.size(); ++i)
        {
            // If we find the same host anywhere else after this point
            if (std::find(_hosts.begin() + i + 1, _hosts.end(), _hosts[i]) != _hosts.end())
            {
                return false;
            }
            // If we find a host that hasn't been declared
            if (m_hosts.find(_hosts[i]) == m_hosts.end())
            {
                return false;
            }
        }

        std::map<std::string, spaceid>::iterator sai = m_space_assignment.find(spacename);
        std::map<spaceid, std::vector<std::string> >::iterator si;
        std::map<subspaceid, std::vector<bool> >::iterator ssi;

        if (istr.eof() && !istr.bad() && !istr.fail()
                && sai != m_space_assignment.end()
                && (si = m_spaces.find(sai->second)) != m_spaces.end()
                && (ssi = m_subspaces.find(subspaceid(si->first, subspacenum))) != m_subspaces.end())
        {
            regionid r(ssi->first, prefix, mask);
            bool overlaps = false;
            typedef std::set<hyperdex::regionid>::const_iterator regiter;
            regiter iter = m_regions.begin();
            hyperspacehashing::prefix::coordinate c = r.coord();

            for (; iter != m_regions.end(); ++iter)
            {
                overlaps |= ssi->first == iter->get_subspace() && c.intersects(iter->coord());
            }

            if (!overlaps)
            {
                m_regions.insert(r);

                for (size_t i = 0; i < _hosts.size(); ++i)
                {
                    m_entities.insert(std::make_pair(entityid(r, i), m_hosts[_hosts[i]]));
                }

                return true;
            }
            else
            {
                return false;
            }
        }
        else
        {
            return false;
        }
    }
    else if (command == "transfer")
    {
        std::string spacename;
        uint32_t subspacenum;
        std::string xfer_id_s;
        uint16_t xfer_id;
        std::string prefix_s;
        uint8_t prefix;
        std::string mask_s;
        uint64_t mask;
        std::string host;

        istr >> xfer_id_s >> spacename >> subspacenum >> prefix_s >> mask_s;

        try
        {
            xfer_id = e::convert::to_uint16_t(xfer_id_s);
            prefix = e::convert::to_uint8_t(prefix_s);
            mask = e::convert::to_uint64_t(mask_s);
        }
        catch (...)
        {
            return false;
        }

        istr >> host;

        if (istr.fail() || istr.bad())
        {
            return false;
        }

        // If we find a host that hasn't been declared
        if (m_hosts.find(host) == m_hosts.end())
        {
            return false;
        }

        std::map<std::string, spaceid>::iterator sai = m_space_assignment.find(spacename);
        std::map<spaceid, std::vector<std::string> >::iterator si;
        std::map<subspaceid, std::vector<bool> >::iterator ssi;

        if (istr.eof() && !istr.bad() && !istr.fail()
                && sai != m_space_assignment.end()
                && (si = m_spaces.find(sai->second)) != m_spaces.end()
                && (ssi = m_subspaces.find(subspaceid(si->first, subspacenum))) != m_subspaces.end())
        {
            regionid r(ssi->first, prefix, mask);

            if (m_regions.find(r) == m_regions.end())
            {
                return false;
            }

            m_transfers.insert(std::make_pair(xfer_id, r));
            m_entities.insert(std::make_pair(entityid(UINT32_MAX - 1, xfer_id, 0, 0, 0), m_hosts[host]));
            return true;
        }
        else
        {
            return false;
        }
    }
    else
    {
        return false;
    }
}

size_t
hyperdex :: configuration :: dimensions(const spaceid& s)
                             const
{
    std::map<spaceid, std::vector<std::string> >::const_iterator si;
    si = m_spaces.find(s);

    if (si != m_spaces.end())
    {
        return si->second.size();
    }

    return 0;
}

std::vector<std::string>
hyperdex :: configuration :: dimension_names(const spaceid& s) const
{
    std::map<spaceid, std::vector<std::string> >::const_iterator si;
    si = m_spaces.find(s);

    if (si == m_spaces.end())
    {
        return std::vector<std::string>();
    }

    return si->second;
}

hyperdex::spaceid
hyperdex :: configuration :: space(const char* spacename) const
{
    std::string s(spacename);
    std::map<std::string, spaceid>::const_iterator sai;
    sai = m_space_assignment.find(s);

    if (sai == m_space_assignment.end())
    {
        return spaceid();
    }

    return sai->second;
}

size_t
hyperdex :: configuration :: subspaces(const spaceid& s)
                             const
{
    std::map<subspaceid, std::vector<bool> >::const_iterator lower;
    std::map<subspaceid, std::vector<bool> >::const_iterator upper;
    lower = m_subspaces.lower_bound(subspaceid(s, 0));
    upper = m_subspaces.upper_bound(subspaceid(s, UINT16_MAX));
    size_t count = 0;

    for (; lower != upper; ++lower)
    {
        ++count;
    }

    return count;
}

hyperdex::entityid
hyperdex :: configuration :: entityfor(const instance& i, const regionid& r)
                             const
{
    std::map<entityid, instance>::const_iterator lower;
    std::map<entityid, instance>::const_iterator upper;
    lower = m_entities.lower_bound(entityid(r, 0));
    upper = m_entities.upper_bound(entityid(r, UINT8_MAX));

    for (; lower != upper; ++lower)
    {
        if (lower->second == i)
        {
            return lower->first;
        }
    }

    return entityid();
}

hyperdex::instance
hyperdex :: configuration :: instancefor(const entityid& e)
                             const
{
    std::map<entityid, instance>::const_iterator ent;
    ent = m_entities.find(e);

    if (ent != m_entities.end())
    {
        return ent->second;
    }

    return instance();
}

void
hyperdex :: configuration :: instance_versions(instance* i)
                             const
{
    std::map<std::string, instance>::const_iterator h;

    for (h = m_hosts.begin(); h != m_hosts.end(); ++h)
    {
        if (h->second.inbound == i->inbound &&
                h->second.outbound == i->outbound)
        {
            *i = h->second;
            return;
        }
    }

    i->inbound_version = 0;
    i->outbound_version = 0;
}

std::set<hyperdex::regionid>
hyperdex :: configuration :: regions_for(const instance& i)
                             const
{
    std::set<regionid> ret;
    std::map<entityid, instance>::const_iterator e;

    for (e = m_entities.begin(); e != m_entities.end(); ++e)
    {
        if (e->second == i)
        {
            ret.insert(e->first.get_region());
        }
    }

    return ret;
}

hyperdex::entityid
hyperdex :: configuration :: sloppy_lookup(const entityid& ent)
                             const
{
    std::map<entityid, instance>::const_iterator e;

    for (e = m_entities.begin(); e != m_entities.end(); ++e)
    {
        if (e->first.get_subspace() == ent.get_subspace()
                && e->first.coord().contains(ent.coord())
                && e->first.number == ent.number)
        {
            return e->first;
        }
    }

    return entityid();
}

bool
hyperdex :: configuration :: in_region(const instance& i, const regionid& r)
                             const
{
    return entityfor(i, r) != entityid();
}

bool
hyperdex :: configuration :: is_client(const entityid& e)
                             const
{
    return e.space == UINT32_MAX;
}

bool
hyperdex :: configuration :: is_point_leader(const entityid& e)
                             const
{
    return e.subspace == 0 && e.number == 0;
}

bool
hyperdex :: configuration :: chain_adjacent(const entityid& f,
                                            const entityid& s)
                             const
{
    return f.get_region() == s.get_region() && f.number + 1 == s.number;
}

bool
hyperdex :: configuration :: chain_has_next(const entityid& e)
                             const
{
    return instancefor(chain_next(e)) != instance();
}

bool
hyperdex :: configuration :: chain_has_prev(const entityid& e)
                             const
{
    return e.number > 0;
}

hyperdex::entityid
hyperdex :: configuration :: chain_next(const entityid& e)
                             const
{
    return entityid(e.get_region(), e.number + 1);
}

hyperdex::entityid
hyperdex :: configuration :: chain_prev(const entityid& e)
                             const
{
    if (e.number == 0)
    {
        return entityid();
    }

    return entityid(e.get_region(), e.number - 1);
}

hyperdex::entityid
hyperdex :: configuration :: headof(const regionid& r) const
{
    typedef std::map<hyperdex::entityid, hyperdex::instance>::const_iterator mapiter;
    mapiter i = m_entities.begin();
    hyperspacehashing::prefix::coordinate c = r.coord();

    for (; i != m_entities.end(); ++i)
    {
        if (r.get_subspace() == i->first.get_subspace() &&
                c.intersects(i->first.get_region().coord()))
        {
            return i->first;
        }
    }

    return entityid();
}

hyperdex::entityid
hyperdex :: configuration :: tailof(const regionid& r) const
{
    typedef std::map<hyperdex::entityid, hyperdex::instance>::const_reverse_iterator mapiter;
    mapiter i = m_entities.rbegin();
    hyperspacehashing::prefix::coordinate c = r.coord();

    for (; i != m_entities.rend(); ++i)
    {
        if (r.get_subspace() == i->first.get_subspace() &&
                c.intersects(i->first.get_region().coord()))
        {
            return i->first;
        }
    }

    return entityid();
}

hyperspacehashing::mask::hasher
hyperdex :: configuration :: disk_hasher(const subspaceid& subspace) const
{
    std::map<subspaceid, hyperspacehashing::mask::hasher>::const_iterator hashiter;
    hashiter = m_disk_hashers.find(subspace);
    assert(hashiter != m_disk_hashers.end());
    return hashiter->second;
}

hyperspacehashing::prefix::hasher
hyperdex :: configuration :: repl_hasher(const subspaceid& subspace) const
{
    std::map<subspaceid, hyperspacehashing::prefix::hasher>::const_iterator hashiter;
    hashiter = m_repl_hashers.find(subspace);
    assert(hashiter != m_repl_hashers.end());
    return hashiter->second;
}

bool
hyperdex :: configuration :: point_leader_entity(const spaceid& s,
                                                 const e::slice& key,
                                                 hyperdex::entityid* ent,
                                                 hyperdex::instance* inst) const
{
    subspaceid ssi(s, 0);
    std::map<subspaceid, hyperspacehashing::prefix::hasher>::const_iterator hashiter;
    hashiter = m_repl_hashers.find(ssi);
    assert(hashiter != m_repl_hashers.end());
    hyperspacehashing::prefix::coordinate coord = hashiter->second.hash(key);
    hyperdex::regionid point_leader(ssi, coord.prefix, coord.point);
    *ent = headof(point_leader);
    *inst = instancefor(*ent);
    return *inst != instance();
}

std::map<hyperdex::entityid, hyperdex::instance>
hyperdex :: configuration :: search_entities(const spaceid& si,
                                             const hyperspacehashing::search& s) const
{
    std::map<entityid, instance>::const_iterator start;
    std::map<entityid, instance>::const_iterator end;
    start = m_entities.lower_bound(hyperdex::entityid(si.space, 0, 0, 0, 0));
    end   = m_entities.upper_bound(hyperdex::entityid(si.space, UINT16_MAX, UINT8_MAX, UINT64_MAX, UINT8_MAX));
    return _search_entities(start, end, s);
}

std::map<hyperdex::entityid, hyperdex::instance>
hyperdex :: configuration :: search_entities(const subspaceid& ssi,
                                             const hyperspacehashing::search& s) const
{
    std::map<entityid, instance>::const_iterator start;
    std::map<entityid, instance>::const_iterator end;
    start = m_entities.lower_bound(hyperdex::entityid(ssi.space, ssi.subspace, 0, 0, 0));
    end   = m_entities.upper_bound(hyperdex::entityid(ssi.space, ssi.subspace, UINT8_MAX, UINT64_MAX, UINT8_MAX));
    return _search_entities(start, end, s);
}

std::map<hyperdex::entityid, hyperdex::instance>
hyperdex :: configuration :: _search_entities(std::map<entityid, instance>::const_iterator iter,
                                              std::map<entityid, instance>::const_iterator end,
                                              const hyperspacehashing::search& s) const
{
    typedef std::map<uint16_t, std::map<hyperdex::entityid, hyperdex::instance> > candidates_map;
    candidates_map candidates;

    bool hashed = false;
    uint16_t hashed_subspace = 0;
    hyperspacehashing::prefix::search_coordinate sc;
    hyperdex::regionid prevreg;

    for (; iter != end; ++iter)
    {
        std::map<subspaceid, hyperspacehashing::prefix::hasher>::const_iterator hashiter;
        hashiter = m_repl_hashers.find(iter->first.get_subspace());
        assert(hashiter != m_repl_hashers.end());

        if (!hashed || hashed_subspace != iter->first.subspace)
        {
            sc = hashiter->second.hash(s);
            hashed = true;
            hashed_subspace = iter->first.subspace;
        }

        if (iter->first.get_region() != prevreg && sc.matches(iter->first.coord()))
        {
            candidates[iter->first.subspace].insert(*iter);
            prevreg = iter->first.get_region();
        }
    }

    bool set = false;
    std::map<hyperdex::entityid, hyperdex::instance> ret;

    for (candidates_map::iterator c = candidates.begin(); c != candidates.end(); ++c)
    {
        if (c->second.size() < ret.size() || !set)
        {
            ret.swap(c->second);
            set = true;
        }
    }

    return ret;
}
