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

// C
#include <cassert>

// HyperDex
#include <hyperdex/datalayer.h>

using std::string;
using std::tr1::shared_ptr;
using std::vector;

hyperdex :: datalayer :: datalayer()
    : m_lock()
    , m_zones()
{
}

void
hyperdex :: datalayer :: create(const string& instanceid,
                                uint32_t table,
                                uint16_t subspace,
                                uint16_t zone,
                                uint16_t numcolumns)
{
    m_lock.wrlock();

    // Check if the zone already exists.
    if (m_zones.find(zoneid(table, subspace, zone)) != m_zones.end())
    {
        m_lock.unlock();
        throw std::runtime_error("Zone already exists.");
    }

    shared_ptr<hyperdex::zone> zd(new hyperdex::zone(instanceid, numcolumns));
    m_zones.insert(std::make_pair(zoneid(table, subspace, zone), zd));
    m_lock.unlock();
}

void
hyperdex :: datalayer :: drop(const string& instanceid,
                              uint32_t table,
                              uint16_t subspace,
                              uint16_t zone)
{
    m_lock.wrlock();

    // Check if the zone actually exists.
    std::map<zoneid, std::tr1::shared_ptr<hyperdex::zone> >::iterator i;
    i = m_zones.find(zoneid(table, subspace, zone));

    if (i == m_zones.end())
    {
        m_lock.unlock();
        throw std::runtime_error("Zone does not exist.");
    }

    if (i->second->instanceid() != instanceid)
    {
        m_lock.unlock();
        throw std::runtime_error("Zone instanceid does not match.");
    }

    m_zones.erase(i);
    m_lock.unlock();
}

void
hyperdex :: datalayer :: reattach(const string& /*oldinstanceid*/,
                                  const string& /*newinstanceid*/,
                                  uint32_t /*table*/,
                                  uint16_t /*subspace*/,
                                  uint16_t /*zone*/)
{
    assert(false); // XXX Actually implement later.
}

bool
hyperdex :: datalayer :: get(uint32_t table, uint16_t subspace, uint16_t zone,
                             const vector<char>& key,
                             vector<vector<char> >* value)
{
    m_lock.rdlock();

    // Check to see if the zone actually exists.
    std::map<zoneid, std::tr1::shared_ptr<hyperdex::zone> >::iterator i;
    i = m_zones.find(zoneid(table, subspace, zone));

    if (i == m_zones.end())
    {
        // Throw if it doesn't.
        m_lock.unlock();
        throw std::runtime_error("Unknown zone.");
    }

    bool ret;

    try
    {
        ret = i->second->get(key, value);
    }
    catch (...)
    {
        m_lock.unlock();
        throw;
    }

    m_lock.unlock();
    return ret;
}

bool
hyperdex :: datalayer :: put(uint32_t table, uint16_t subspace, uint16_t zone,
                             const vector<char>& key,
                             const vector<vector<char> >& value)
{
    m_lock.rdlock();

    // Check to see if the zone actually exists.
    std::map<zoneid, std::tr1::shared_ptr<hyperdex::zone> >::iterator i;
    i = m_zones.find(zoneid(table, subspace, zone));

    if (i == m_zones.end())
    {
        // Throw if it doesn't.
        m_lock.unlock();
        throw std::runtime_error("Unknown zone.");
    }

    bool ret;

    try
    {
        ret = i->second->put(key, value);
    }
    catch (...)
    {
        m_lock.unlock();
        throw;
    }

    m_lock.unlock();
    return ret;
}

bool
hyperdex :: datalayer :: del(uint32_t table, uint16_t subspace, uint16_t zone,
                             const vector<char>& key)
{
    m_lock.rdlock();

    // Check to see if the zone actually exists.
    std::map<zoneid, std::tr1::shared_ptr<hyperdex::zone> >::iterator i;
    i = m_zones.find(zoneid(table, subspace, zone));

    if (i == m_zones.end())
    {
        // Throw if it doesn't.
        m_lock.unlock();
        throw std::runtime_error("Unknown zone.");
    }

    bool ret;

    try
    {
        ret = i->second->del(key);
    }
    catch (...)
    {
        m_lock.unlock();
        throw;
    }

    m_lock.unlock();
    return ret;
}

bool
hyperdex :: datalayer :: zoneid :: operator < (const zoneid& rhs)
                                   const
{
    const zoneid& lhs(*this);

    if (lhs.table < rhs.table)
    {
        return true;
    }
    else if (lhs.table > rhs.table)
    {
        return false;
    }

    if (lhs.subspace < rhs.subspace)
    {
        return true;
    }
    else if (lhs.subspace > rhs.subspace)
    {
        return false;
    }

    return lhs.zone < rhs.zone;
}
