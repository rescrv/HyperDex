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

#ifndef hyperdisk_coordinate_h_
#define hyperdisk_coordinate_h_

// C
#include <stdint.h>

// STL
#include <iostream>

// e
#include <e/tuple_compare.h>

namespace hyperdisk
{

class coordinate
{
    public:
        coordinate();
        coordinate(uint32_t primary_mask, uint32_t primary_hash,
                   uint32_t secondary_mask, uint32_t secondary_hash);
        coordinate(const coordinate& other);
        ~coordinate() throw ();

    public:
        bool contains(const coordinate& other) const;
        bool primary_contains(const coordinate& other) const;
        bool secondary_contains(const coordinate& other) const;

    public:
        uint32_t primary_mask;
        uint32_t primary_hash;
        uint32_t secondary_mask;
        uint32_t secondary_hash;
};

inline
coordinate :: coordinate()
    : primary_mask()
    , primary_hash()
    , secondary_mask()
    , secondary_hash()
{
}

inline
coordinate :: coordinate(uint32_t pm, uint32_t ph,
                         uint32_t sm, uint32_t sh)
    : primary_mask(pm)
    , primary_hash(ph)
    , secondary_mask(sm)
    , secondary_hash(sh)
{
}

inline
coordinate :: coordinate(const coordinate& other)
    : primary_mask(other.primary_mask)
    , primary_hash(other.primary_hash)
    , secondary_mask(other.secondary_mask)
    , secondary_hash(other.secondary_hash)
{
}

inline
coordinate :: ~coordinate() throw ()
{
}

inline bool
coordinate :: contains(const coordinate& other) const
{
    return primary_contains(other) && secondary_contains(other);
}

inline bool
coordinate :: primary_contains(const coordinate& other) const
{
    return (primary_mask & other.primary_mask) == primary_mask &&
           (primary_hash & primary_mask) == (other.primary_hash & primary_mask);
}

inline bool
coordinate :: secondary_contains(const coordinate& other) const
{
    return (secondary_mask & other.secondary_mask) == secondary_mask &&
           (secondary_hash & secondary_mask) == (other.secondary_hash & secondary_mask);
}

inline std::ostream&
operator << (std::ostream& lhs, const coordinate& rhs)
{
    lhs << "coordinate(" << rhs.primary_mask << ", " << rhs.primary_hash
        << ", " << rhs.secondary_mask << ", " << rhs.secondary_hash << ")";
    return lhs;
}

} // namespace hyperdisk

#endif // hyperdisk_coordinate_h_
