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
#include <stdint.h>

// STL
#include <algorithm>
#include <sstream>
#include <vector>

// Google Log
#include <glog/logging.h>

// e
#include <e/convert.h>

// Configuration
#include <configuration/configuration.h>

// HyperDex
#include <hyperdex/hyperspace.h>

hyperdex :: configuration :: configuration()
    : m_hosts()
    , m_space_assignment()
    , m_spaces()
    , m_subspaces()
    , m_regions()
    , m_entities()
    , m_transfers()
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
        std::string dim;
        std::vector<std::string> dims;
        istr >> spacename >> subspacenum;

        while (istr.good())
        {
            istr >> dim;

            if (!istr.fail())
            {
                dims.push_back(dim);
            }
        }

        std::map<std::string, spaceid>::iterator sai = m_space_assignment.find(spacename);
        std::map<spaceid, std::vector<std::string> >::iterator si;

        if (istr.eof() && !istr.bad() && !istr.fail()
                && sai != m_space_assignment.end()
                && (si = m_spaces.find(sai->second)) != m_spaces.end()
                && std::distance(m_subspaces.lower_bound(subspaceid(si->first, 0)),
                                 m_subspaces.upper_bound(subspaceid(si->first, UINT16_MAX))) == subspacenum
                && dims.size() > 0
                && (subspacenum != 0 || dims.size() == 1))
        {
            const std::vector<std::string>& spacedims(si->second);
            std::vector<bool> bitmask(spacedims.size(), false);

            for (size_t i = 0; i < dims.size(); ++i)
            {
                std::vector<std::string>::const_iterator d;
                d = std::find(spacedims.begin(), spacedims.end(), dims[i]);

                if (d == spacedims.end())
                {
                    return false;
                }
                else
                {
                    bitmask[d - spacedims.begin()] = true;
                }
            }

            m_subspaces.insert(std::make_pair(subspaceid(si->first, subspacenum), bitmask));
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
        std::vector<std::string> hosts;

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
                hosts.push_back(host);
            }
        }

        // Check for valid hosts lists
        for (size_t i = 0; i < hosts.size(); ++i)
        {
            // If we find the same host anywhere else after this point
            if (std::find(hosts.begin() + i + 1, hosts.end(), hosts[i]) != hosts.end())
            {
                return false;
            }
            // If we find a host that hasn't been declared
            if (m_hosts.find(hosts[i]) == m_hosts.end())
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

            if (hyperdex::nonoverlapping(m_regions, r))
            {
                m_regions.insert(r);

                for (size_t i = 0; i < hosts.size(); ++i)
                {
                    m_entities.insert(std::make_pair(entityid(r, i), m_hosts[hosts[i]]));
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

hyperdex::entityid
hyperdex :: configuration :: headof(const regionid& r) const
{
    typedef std::map<hyperdex::entityid, hyperdex::instance>::const_iterator mapiter;
    mapiter i = m_entities.begin();

    for (; i != m_entities.end(); ++i)
    {
        if (overlap(i->first.get_region(), r))
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

    for (; i != m_entities.rend(); ++i)
    {
        if (overlap(i->first.get_region(), r))
        {
            return i->first;
        }
    }

    return entityid();
}

hyperdex::instance
hyperdex :: configuration :: lookup(const entityid& e) const
{
    typedef std::map<hyperdex::entityid, hyperdex::instance>::const_iterator mapiter;
    mapiter i = m_entities.find(e);

    if (i != m_entities.end())
    {
        return i->second;
    }

    return instance();
}

bool
hyperdex :: configuration :: subspaces(const spaceid& s, size_t* sz) const
{
    std::map<subspaceid, std::vector<bool> >::const_iterator lower;
    std::map<subspaceid, std::vector<bool> >::const_iterator upper;
    lower = m_subspaces.lower_bound(subspaceid(s, 0));
    upper = m_subspaces.upper_bound(subspaceid(s, UINT16_MAX));

    if (lower != upper)
    {
        *sz = std::distance(lower, upper);
        return true;
    }
    else
    {
        return false;
    }
}

bool
hyperdex :: configuration :: dimensionality(const spaceid& s, size_t* sz) const
{
    std::map<spaceid, std::vector<std::string> >::const_iterator space;
    space = m_spaces.find(s);

    if (space != m_spaces.end())
    {
        *sz = space->second.size();
        return true;
    }
    else
    {
        return false;
    }
}

bool
hyperdex :: configuration :: dimensions(const subspaceid& ss,
                                        std::vector<bool>* dims)
                             const
{
    std::map<subspaceid, std::vector<bool> >::const_iterator subspace;
    subspace = m_subspaces.find(ss);

    if (subspace != m_subspaces.end())
    {
        *dims = subspace->second;
        return true;
    }
    else
    {
        return false;
    }
}

std::map<uint16_t, hyperdex::regionid>
hyperdex :: configuration :: transfers_to(const instance& i)
                             const
{
    std::map<uint16_t, hyperdex::regionid> ret;

    for (std::map<entityid, instance>::const_iterator e = m_entities.begin();
            e != m_entities.end(); ++e)
    {
        if (e->first.space == UINT32_MAX - 1 && e->second == i)
        {
            uint16_t xfer_id = e->first.subspace;
            ret.insert(std::make_pair(xfer_id, m_transfers.find(xfer_id)->second));
        }
    }

    return ret;
}

std::map<uint16_t, hyperdex::regionid>
hyperdex :: configuration :: transfers_from(const instance& i)
                             const
{
    std::map<uint16_t, regionid> ret;

    for (std::map<uint16_t, regionid>::const_iterator t = m_transfers.begin();
            t != m_transfers.end(); ++t)
    {
        // This should always succeed.
        std::map<entityid, instance>::const_iterator tail = m_entities.find(tailof(t->second));
        assert(tail != m_entities.end());

        // See if there is a predecessor to tail.
        std::map<entityid, instance>::const_iterator pred = m_entities.find(entityid(tail->first.get_region(), tail->first.number - 1));

        // Read the transfer entity.
        std::map<entityid, instance>::const_iterator trans = m_entities.find(entityid(UINT32_MAX - 1, t->first, 0, 0, 0));
        assert(trans != m_entities.end());

        // If the entity to which the transfer is happening is the tail and
        // we are the predecessor.
        if (tail->second == trans->second && pred != m_entities.end() && pred->second == i)
        {
            ret.insert(*t);
        }
        else if (tail->second == trans->second && pred == m_entities.end())
        {
            // XXX
            LOG(ERROR) << "There is an error in the configuration object's logic.";
        }
        else if (tail->second == i)
        {
            ret.insert(*t);
        }
    }

    return ret;
}

std::map<hyperdex::regionid, size_t>
hyperdex :: configuration :: regions() const
{
    std::map<regionid, size_t> ret;

    for (std::set<regionid>::iterator i = m_regions.begin();
            i != m_regions.end(); ++i)
    {
        ret[*i] = m_spaces.find(i->get_space())->second.size();
    }

    return ret;
}
