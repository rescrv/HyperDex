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

// HyperDisk
#include "hyperdisk/hyperdisk/snapshot.h"
#include "hyperdisk/log_entry.h"
#include "hyperdisk/shard_snapshot.h"
#include "hyperdisk/shard_vector.h"

hyperdisk :: snapshot :: snapshot(const hyperspacehashing::mask::coordinate& coord,
                                  e::intrusive_ptr<shard_vector> shards,
                                  std::vector<hyperdisk::shard_snapshot>* ss)
    : m_ref(0)
    , m_coord(coord)
    , m_shards(shards)
    , m_snaps()
{
    m_snaps.swap(*ss);
}

hyperdisk :: snapshot :: ~snapshot() throw ()
{
}

bool
hyperdisk :: snapshot :: valid()
{
    while (!m_snaps.empty())
    {
        if (m_snaps.back().valid(m_coord))
        {
            return true;
        }
        else
        {
            m_snaps.pop_back();
        }
    }

    return false;
}

void
hyperdisk :: snapshot :: next()
{
    if (!m_snaps.empty())
    {
        m_snaps.back().next();
    }
}

hyperspacehashing::mask::coordinate
hyperdisk :: snapshot :: coordinate()
{
    assert(!m_snaps.empty());
    return m_snaps.back().coordinate();
}

uint64_t
hyperdisk :: snapshot :: version()
{
    assert(!m_snaps.empty());
    return m_snaps.back().version();
}

const e::slice&
hyperdisk :: snapshot :: key()
{
    assert(!m_snaps.empty());
    return m_snaps.back().key();
}

const std::vector<e::slice>&
hyperdisk :: snapshot :: value()
{
    assert (!m_snaps.empty());
    return m_snaps.back().value();
}

hyperdisk :: rolling_snapshot :: rolling_snapshot(const e::locking_iterable_fifo<log_entry>::iterator& iter,
                                                  const e::intrusive_ptr<snapshot>& snap)
    : m_ref(0)
    , m_iter(iter)
    , m_snap(snap)
{
    valid();
}

hyperdisk :: rolling_snapshot :: ~rolling_snapshot() throw ()
{
}

bool
hyperdisk :: rolling_snapshot :: valid()
{
    return m_snap->valid() || m_iter.valid();
}

void
hyperdisk :: rolling_snapshot :: next()
{
    if (m_snap->valid())
    {
        m_snap->next();
    }
    else if (m_iter.valid())
    {
        m_iter.next();
    }
}

bool
hyperdisk :: rolling_snapshot :: has_value()
{
    if (m_snap->valid())
    {
        return true;
    }
    else if (m_iter.valid())
    {
        return m_iter->is_put;
    }
    else
    {
        return false;
    }
}

uint64_t
hyperdisk :: rolling_snapshot :: version()
{
    if (m_snap->valid())
    {
        return m_snap->version();
    }
    else if (m_iter.valid())
    {
        return m_iter->version;
    }
    else
    {
        return uint64_t();
    }
}

const e::slice&
hyperdisk :: rolling_snapshot :: key()
{
    if (m_snap->valid())
    {
        return m_snap->key();
    }
    else if (m_iter.valid())
    {
        return m_iter->key;
    }
    else
    {
        abort();
    }
}

const std::vector<e::slice>&
hyperdisk :: rolling_snapshot :: value()
{
    if (m_snap->valid())
    {
        return m_snap->value();
    }
    else if (m_iter.valid())
    {
        return m_iter->value;
    }
    else
    {
        abort();
    }
}
