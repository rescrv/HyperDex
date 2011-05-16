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

using po6::threads::rwlock;
using std::tr1::shared_ptr;

hyperdex :: datalayer :: datalayer()
    : m_lock()
    , m_zones()
{
}

void
hyperdex :: datalayer :: create(uint32_t table,
                                uint16_t subspace,
                                uint8_t prefix,
                                uint64_t mask,
                                uint16_t numcolumns)
{
    rwlock::wrhold hold(&m_lock);

    if (m_zones.find(zoneid(table, subspace, prefix, mask)) != m_zones.end())
    {
        throw std::runtime_error("Zone already exists.");
    }

    shared_ptr<hyperdex::zone> zd(new hyperdex::zone(numcolumns));
    m_zones.insert(std::make_pair(zoneid(table, subspace, prefix, mask), zd));
}

void
hyperdex :: datalayer :: drop(uint32_t table,
                              uint16_t subspace,
                              uint8_t prefix,
                              uint64_t mask)
{
    rwlock::wrhold hold(&m_lock);

    // Check if the zone actually exists.
    std::map<zoneid, shared_ptr<hyperdex::zone> >::iterator i;
    i = m_zones.find(zoneid(table, subspace, prefix, mask));

    if (i == m_zones.end())
    {
        throw std::runtime_error("Zone does not exist.");
    }

    m_zones.erase(i);
}

bool
hyperdex :: datalayer :: get(uint32_t table,
                             uint16_t subspace,
                             uint8_t prefix,
                             uint64_t mask,
                             const e::buffer& key,
                             std::vector<e::buffer>* value)
{
    rwlock::rdhold hold(&m_lock);

    // Check to see if the zone actually exists.
    std::map<zoneid, shared_ptr<hyperdex::zone> >::iterator i;
    i = m_zones.find(zoneid(table, subspace, prefix, mask));

    if (i == m_zones.end())
    {
        throw std::runtime_error("Unknown zone.");
    }

    return i->second->get(key, value);
}

bool
hyperdex :: datalayer :: put(uint32_t table,
                             uint16_t subspace,
                             uint8_t prefix,
                             uint64_t mask,
                             const e::buffer& key,
                             const std::vector<e::buffer>& value)
{
    rwlock::rdhold hold(&m_lock);

    // Check to see if the zone actually exists.
    std::map<zoneid, shared_ptr<hyperdex::zone> >::iterator i;
    i = m_zones.find(zoneid(table, subspace, prefix, mask));

    if (i == m_zones.end())
    {
        throw std::runtime_error("Unknown zone.");
    }

    return i->second->put(key, value);
}

bool
hyperdex :: datalayer :: del(uint32_t table,
                             uint16_t subspace,
                             uint8_t prefix,
                             uint64_t mask,
                             const e::buffer& key)
{
    rwlock::rdhold hold(&m_lock);

    // Check to see if the zone actually exists.
    std::map<zoneid, shared_ptr<hyperdex::zone> >::iterator i;
    i = m_zones.find(zoneid(table, subspace, prefix, mask));

    if (i == m_zones.end())
    {
        throw std::runtime_error("Unknown zone.");
    }

    return i->second->del(key);
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

    if (lhs.prefix < rhs.prefix)
    {
        return true;
    }
    else if (lhs.prefix > rhs.prefix)
    {
        return false;
    }

    return lhs.mask < rhs.mask;
}
