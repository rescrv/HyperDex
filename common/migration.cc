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
    pa = pa << m.space_from << m.space_to << m.outstanding_regions;
    return pa;
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, migration& m)
{
    space_id s_space_from, s_space_to;
    std::vector<region_id> outstanding_regions;
    up >> s_space_from >> s_space_to >> outstanding_regions;
    m.space_from = s_space_from;
    m.space_to = s_space_to;
    m.outstanding_regions = outstanding_regions;
    return up;
}

size_t
hyperdex :: pack_size(const migration& m)
{
    return 2 * sizeof(uint64_t) + m.outstanding_regions.size() * sizeof(uint64_t);
}
