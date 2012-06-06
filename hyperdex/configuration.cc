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
    : m_config_text("")
    , m_version(0)
    , m_hosts()
    , m_space_assignment()
    , m_spaces()
    , m_space_sizes()
    , m_entities()
    , m_repl_hashers()
    , m_disk_hashers()
    , m_transfers()
    , m_transfers_by_num(65536)
    , m_quiesce(false)
    , m_quiesce_state_id("")
    , m_shutdown(false)
{
}

hyperdex :: configuration :: configuration(const std::string& _config_text,
                                           uint64_t ver,
                                           const std::vector<instance>& hosts,
                                           const std::map<std::string, spaceid>& space_assignment,
                                           const std::map<spaceid, std::vector<attribute> >& spaces,
                                           const std::map<spaceid, uint16_t>& space_sizes,
                                           const std::map<entityid, instance>& entities,
                                           const std::map<subspaceid, hyperspacehashing::prefix::hasher>& repl_hashers,
                                           const std::map<subspaceid, hyperspacehashing::mask::hasher>& disk_hashers,
                                           const std::map<std::pair<instance, uint16_t>, hyperdex::regionid>& transfers,
                                           bool _quiesce, const std::string& _quiesce_state_id,
                                           bool _shutdown)
    : m_config_text(_config_text)
    , m_version(ver)
    , m_hosts(hosts)
    , m_space_assignment(space_assignment)
    , m_spaces(spaces)
    , m_space_sizes(space_sizes)
    , m_entities(entities)
    , m_repl_hashers(repl_hashers)
    , m_disk_hashers(disk_hashers)
    , m_transfers(transfers)
    , m_transfers_by_num(65536)
    , m_quiesce(_quiesce)
    , m_quiesce_state_id(_quiesce_state_id)
    , m_shutdown(_shutdown)
{
    std::map<std::pair<instance, uint16_t>, hyperdex::regionid>::iterator it;

    for (it = m_transfers.begin(); it != m_transfers.end(); ++it)
    {
        assert(m_transfers_by_num[it->first.second] == instance());
        m_transfers_by_num[it->first.second] = it->first.first;
    }
}

hyperdex :: configuration :: ~configuration() throw ()
{
}

size_t
hyperdex :: configuration :: dimensions(const spaceid& s)
                             const
{
    std::map<spaceid, std::vector<attribute> >::const_iterator si;
    si = m_spaces.find(s);

    if (si != m_spaces.end())
    {
        return si->second.size();
    }

    return 0;
}

std::vector<hyperdex::attribute>
hyperdex :: configuration :: dimension_names(const spaceid& s) const
{
    std::map<spaceid, std::vector<attribute> >::const_iterator si;
    si = m_spaces.find(s);

    if (si == m_spaces.end())
    {
        return std::vector<attribute>();
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
    std::map<spaceid, uint16_t>::const_iterator si;

    if ((si = m_space_sizes.find(s)) == m_space_sizes.end())
    {
        return -1;
    }

    return si->second;
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
    std::vector<instance>::const_iterator h;

    for (h = m_hosts.begin(); h != m_hosts.end(); ++h)
    {
        if (h->address == i->address &&
            h->inbound_port == i->inbound_port &&
            h->outbound_port == i->outbound_port)
        {
            *i = *h;
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

hyperdex::instance
hyperdex :: configuration :: instancefortransfer(uint16_t xfer_id) const
{
    return m_transfers_by_num[xfer_id];
}

uint16_t
hyperdex :: configuration :: transfer_id(const regionid& reg) const
{
    std::map<std::pair<instance, uint16_t>, hyperdex::regionid>::const_iterator t;

    for (t = m_transfers.begin(); t != m_transfers.end(); ++t)
    {
        if (t->second == reg)
        {
            return t->first.second;
        }
    }

    return 0;
}

std::map<uint16_t, hyperdex::regionid>
hyperdex :: configuration :: transfers_to(const instance& inst) const
{
    std::map<uint16_t, hyperdex::regionid> ret;
    std::map<std::pair<instance, uint16_t>, hyperdex::regionid>::const_iterator lower;
    std::map<std::pair<instance, uint16_t>, hyperdex::regionid>::const_iterator upper;
    lower = m_transfers.lower_bound(std::make_pair(inst, 0));
    upper = m_transfers.upper_bound(std::make_pair(inst, UINT16_MAX));

    for (; lower != upper; ++lower)
    {
        ret.insert(std::make_pair(lower->first.second, lower->second));
    }

    return ret;
}

std::map<uint16_t, hyperdex::regionid>
hyperdex :: configuration :: transfers_from(const instance& inst) const
{
    std::map<uint16_t, hyperdex::regionid> ret;
    std::map<std::pair<instance, uint16_t>, hyperdex::regionid>::const_iterator t;
    t = m_transfers.begin();

    for (; t != m_transfers.end(); ++t)
    {
        // If the region has gone live and the tail of the region is now the
        // recipient of the transfer.
        if (instancefor(tailof(t->second)) == t->first.first)
        {
            entityid ent = tailof(t->second);
            assert(chain_has_prev(ent));
            ent = chain_prev(ent);

            if (instancefor(ent) == inst)
            {
                ret.insert(std::make_pair(t->first.second, t->second));
            }
        }
        // Otherwise the tail of the region is the sender of the transfer.
        else
        {
            if (instancefor(tailof(t->second)) == inst)
            {
                ret.insert(std::make_pair(t->first.second, t->second));
            }
        }
    }

    return ret;
}

bool 
hyperdex :: configuration :: quiesce() const
{
    return m_quiesce;
}

std::string 
hyperdex :: configuration :: quiesce_state_id() const
{
    return m_quiesce_state_id;
}

bool 
hyperdex :: configuration :: shutdown() const
{
    return m_shutdown;
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

