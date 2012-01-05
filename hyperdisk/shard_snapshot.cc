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

// HyperspaceHashing
#include "hyperspacehashing/hyperspacehashing/mask.h"

// HyperDisk
#include "hyperdisk/shard.h"
#include "hyperdisk/shard_snapshot.h"

hyperdisk :: shard_snapshot :: shard_snapshot(uint32_t offset, shard* s)
    : m_shard(s)
    , m_limit(offset)
    , m_entry(0)
    , m_valid(true)
    , m_parsed(false)
    , m_coord()
    , m_version()
    , m_key()
    , m_value()
{
    valid();
}

hyperdisk :: shard_snapshot :: shard_snapshot(const shard_snapshot& other)
    : m_shard(other.m_shard)
    , m_limit(other.m_limit)
    , m_entry(other.m_entry)
    , m_valid(other.m_valid)
    , m_parsed(other.m_parsed)
    , m_coord(other.m_coord)
    , m_version(other.m_version)
    , m_key(other.m_key)
    , m_value(other.m_value)
{
}

hyperdisk :: shard_snapshot :: ~shard_snapshot() throw ()
{
}

bool
hyperdisk :: shard_snapshot :: valid()
{
    hyperspacehashing::mask::coordinate coord;
    return valid(coord);
}

bool
hyperdisk :: shard_snapshot :: valid(const hyperspacehashing::mask::coordinate& coord)
{
    uint32_t offset = 0;
    uint32_t invalid = 0;

    while (m_entry < SEARCH_INDEX_ENTRIES)
    {
        offset = m_shard->m_search_log[m_entry].offset;
        invalid = m_shard->m_search_log[m_entry].invalid;

        // If the m_valid flag is set; the offset is within the subsection of
        // data we may observe; and the data was never invalidated, or was
        // invalidated after we scanned it, then we may return true;
        if ((m_shard->m_search_log[m_entry].lower & coord.secondary_lower_mask) == coord.secondary_lower_hash &&
            (m_shard->m_search_log[m_entry].upper & coord.secondary_upper_mask) == coord.secondary_upper_hash &&
            (m_shard->m_search_log[m_entry].primary & coord.primary_mask) == coord.primary_hash &&
            m_valid && offset > 0 && offset < m_limit &&
            (invalid == 0 || invalid >= m_limit))
        {
            m_parsed = false;
            m_coord = hyperspacehashing::mask::coordinate(UINT64_MAX, m_shard->m_search_log[m_entry].primary,
                                                          UINT64_MAX, m_shard->m_search_log[m_entry].lower,
                                                          UINT64_MAX, m_shard->m_search_log[m_entry].upper);
            return true;
        }

        // If offset is 0, then we know that there are no more entries further
        // on.  Stop iterating.  If offset is >= m_limit, we know that the
        // operation (and all succeeding it) happened after the snapshot.
        if (offset == 0 || offset >= m_limit)
        {
            m_entry = SEARCH_INDEX_ENTRIES;
            m_valid = false;
            break;
        }

        ++m_entry;
        m_valid = true;
    }

    return false;
}

void
hyperdisk :: shard_snapshot :: next()
{
    m_valid = false;
    m_parsed = false;
}

uint64_t
hyperdisk :: shard_snapshot :: version()
{
    if (!m_parsed)
    {
        parse();
    }

    return m_version;
}

const e::slice&
hyperdisk :: shard_snapshot :: key()
{
    if (!m_parsed)
    {
        parse();
    }

    return m_key;
}

const std::vector<e::slice>&
hyperdisk :: shard_snapshot :: value()
{
    if (!m_parsed)
    {
        parse();
    }

    return m_value;
}

void
hyperdisk :: shard_snapshot :: parse()
{
    uint32_t offset = m_shard->m_search_log[m_entry].offset;
    assert(offset);
    m_version = m_shard->data_version(offset);
    size_t key_size = m_shard->data_key_size(offset);
    m_shard->data_key(offset, key_size, &m_key);
    m_shard->data_value(offset, key_size, &m_value);
    m_parsed = true;
}

hyperdisk::shard_snapshot&
hyperdisk :: shard_snapshot :: operator = (const shard_snapshot& rhs)
{
    if (this != &rhs)
    {
        m_shard = rhs.m_shard;
        m_limit = rhs.m_limit;
        m_entry = rhs.m_entry;
        m_valid = rhs.m_valid;
    }

    return *this;
}
