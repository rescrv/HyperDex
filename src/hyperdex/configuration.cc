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

// STL
#include <sstream>
#include <vector>

// Google Log
#include <glog/logging.h>

// e
#include <e/convert.h>

// HyperDex
#include <hyperdex/configuration.h>
#include <hyperdex/hyperspace.h>

hyperdex :: configuration :: configuration()
    : m_hosts()
    , m_space_assignment()
    , m_spaces()
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
        std::vector<std::string> dimensions;
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
                dimensions.push_back(dim);
            }
        }

        if (istr.eof() && !istr.bad() && !istr.fail()
                && m_space_assignment.find(name) == m_space_assignment.end()
                && m_spaces.find(id) == m_spaces.end()
                && dimensions.size() > 0)
        {
            m_space_assignment[name] = id;
            m_spaces.insert(std::make_pair(id, hyperdex::space(name, dimensions)));
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
        std::vector<std::string> dimensions;
        istr >> spacename >> subspacenum;

        while (istr.good())
        {
            istr >> dim;

            if (!istr.fail())
            {
                dimensions.push_back(dim);
            }
        }

        std::map<std::string, uint32_t>::iterator sai = m_space_assignment.find(spacename);
        std::map<uint32_t, hyperdex::space>::iterator si;

        if (istr.eof() && !istr.bad() && !istr.fail()
                && sai != m_space_assignment.end()
                && (si = m_spaces.find(sai->second)) != m_spaces.end()
                && si->second.subspaces.size() == subspacenum
                && dimensions.size() > 0
                && (subspacenum != 0 || dimensions.size() == 1))
        {
            const std::vector<std::string>& spacedims(si->second.dimensions);

            for (size_t i = 0; i < dimensions.size(); ++i)
            {
                if (std::find(spacedims.begin(), spacedims.end(), dimensions[i]) == spacedims.end())
                {
                    return false;
                }
            }

            si->second.subspaces.push_back(hyperdex::subspace(dimensions));
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

        hyperdex::region r(prefix, mask, hosts);
        std::map<std::string, uint32_t>::iterator sai = m_space_assignment.find(spacename);
        std::map<uint32_t, hyperdex::space>::iterator si;

        if (istr.eof() && !istr.bad() && !istr.fail()
                && sai != m_space_assignment.end()
                && (si = m_spaces.find(sai->second)) != m_spaces.end()
                && subspacenum < si->second.subspaces.size()
                && si->second.subspaces[subspacenum].regions.find(r) == si->second.subspaces[subspacenum].regions.end())
        {
            if (hyperdex::nonoverlapping(si->second.subspaces[subspacenum].regions, r))
            {
                si->second.subspaces[subspacenum].regions.insert(r);
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
    else
    {
        return false;
    }
}

bool
hyperdex :: configuration :: is_complete() const
{
    // XXX
    return false;
}

std::map<hyperdex::entity, hyperdex::configuration::instance>
hyperdex :: configuration :: entity_mapping() const
{
    typedef std::map<uint32_t, hyperdex::space>::const_iterator space_iterator;
    typedef std::vector<hyperdex::subspace>::const_iterator subspace_iterator;
    typedef std::set<hyperdex::region>::const_iterator region_iterator;
    typedef std::vector<std::string>::const_iterator host_iterator;
    std::map<hyperdex::entity, hyperdex::configuration::instance> ret;

    for (space_iterator si = m_spaces.begin(); si != m_spaces.end(); ++si)
    {
        uint32_t space = si->first;

        for (subspace_iterator ssi = si->second.subspaces.begin();
                ssi != si->second.subspaces.end(); ++ssi)
        {
            uint16_t subspace = ssi - si->second.subspaces.begin();

            for (region_iterator ri = ssi->regions.begin();
                    ri != ssi->regions.end(); ++ri)
            {
                uint8_t prefix = ri->prefix;
                uint64_t mask = ri->mask;

                for (host_iterator hi = ri->hosts.begin();
                        hi != ri->hosts.end(); ++hi)
                {
                    uint8_t number = hi - ri->hosts.begin();
                    ret.insert(std::make_pair(hyperdex::entity(space, subspace, prefix, mask, number),
                                              m_hosts.find(*hi)->second));
                }
            }
        }
    }

    return ret;
}
