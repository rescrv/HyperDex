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

#ifndef hyperdex_regionid_h_
#define hyperdex_regionid_h_

// C
#include <stdint.h>

// C++
#include <iostream>

namespace hyperdex
{

class regionid
{
    public:
        regionid(uint32_t t, uint16_t s, uint8_t p, uint64_t m)
            : space(t)
            , subspace(s)
            , prefix(p)
            , mask(m)
        {
        }

    public:
        int compare(const regionid& other) const;

    public:
        bool operator < (const regionid& other) const
        { return compare(other) < 0; }
        bool operator == (const regionid& other) const
        { return compare(other) == 0; }
        bool operator != (const regionid& other) const
        { return compare(other) != 0; }

    public:
        const uint32_t space;
        const uint16_t subspace;
        const uint8_t prefix;
        const uint64_t mask;
};

inline std::ostream&
operator << (std::ostream& lhs, const regionid& rhs)
{
    std::ios_base::fmtflags fl = lhs.flags();
    lhs << "region(space=" << rhs.space
        << ", subspace=" << rhs.subspace
        << ", prefix=" << rhs.prefix
        << ", mask=" << std::showbase << std::hex << rhs.mask << ")";
    lhs.flags(fl);
    return lhs;
}

} // namespace hyperdex

#endif // hyperdex_regionid_h_
