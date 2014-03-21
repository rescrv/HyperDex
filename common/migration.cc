// Copyright (c) 2012, Cornell University
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

// HyperDex
#include "common/migration.h"

using hyperdex::migration;

migration :: migration()
    : id()
    , space_from()
    , space_to()
    , outstanding_regions()
{
}

migration :: ~migration() throw ()
{
}

migration&
migration :: operator = (const migration& rhs)
{
    id = rhs.id;
    space_from = rhs.space_from;
    space_to = rhs.space_to;
    outstanding_regions = rhs.outstanding_regions;
}

bool migration :: operator < (const migration& rhs) const
{
    if (id < rhs.id) { return true; }
    else if (id > rhs.id) { return false; }

    if (space_from < rhs.space_from) { return true; }
    else if (space_from > rhs.space_from) { return false; }

    if (space_to < rhs.space_to) { return true; }
    else if (space_to > rhs.space_to) { return false; }

    return false;
}

migration :: migration(migration_id  _id,
                       space_id _space_from,
                       space_id _space_to)
    : id(id)
    , space_from(_space_from)
    , space_to(_space_to)
{
}

std::ostream&
hyperdex :: operator << (std::ostream& lhs, const migration& rhs)
{
    return lhs << "migration(id=" << rhs.id
               << ", space_from=" << rhs.space_from
               << ", space_to=" << rhs.space_to;
}

e::buffer::packer
hyperdex :: operator << (e::buffer::packer pa, const migration& m)
{
    size_t num_outstanding_regions = m.outstanding_regions.size();
    pa = pa << m.id.get() << m.space_from.get() << m.space_to.get() << num_outstanding_regions;
    for (size_t i = 0; i < num_outstanding_regions; ++i)
    {
        pa = pa << m.outstanding_regions[i];
    }

    return pa;
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, migration& m)
{
    uint64_t mid, space_from_id, space_to_id;
    size_t num_outstanding_regions;
    up >> mid >> space_from_id >> space_to_id >> num_outstanding_regions;
    m.id = migration_id(mid);
    m.space_from = space_id(space_from_id);
    m.space_to = space_id(space_to_id);
    m.outstanding_regions.resize(num_outstanding_regions);
    for (size_t i = 0; !up.error() && i < num_outstanding_regions; ++i)
    {
        up = up >> m.outstanding_regions[i];
    }
    return up;
}

size_t
hyperdex :: pack_size(const migration& m)
{
    size_t sz = sizeof(uint64_t)  // migration id
              + sizeof(uint64_t)  // space_from
              + sizeof(uint64_t)  // space_to
              + sizeof(size_t);  // num_outstanding_regions

    for (size_t i = 0; i < m.outstanding_regions.size(); ++i)
    {
        sz += pack_size(m.outstanding_regions[i]);
    }

    return sz;
}
