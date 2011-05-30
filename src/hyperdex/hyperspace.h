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

#ifndef hyperdex_hyperspace_h_
#define hyperdex_hyperspace_h_

// C
#include <cassert>
#include <stdint.h>

// HyperDex
#include <hyperdex/city.h>
#include <hyperdex/ids.h>

namespace hyperdex
{

// A spacing of 1 corresponds to the identity function.  A spacing of 0 is
// undefined.  A spacing > 64 is undefined.
inline uint64_t
spacing(uint64_t number, uint8_t spacing)
{
    uint64_t ret = 0;

    for (uint8_t i = 0; i * spacing < 64; ++i)
    {
        // Don't do this inline so that it will use the full 64-bit int.
        // It's also much easier to read this way.
        uint64_t bit = 1;
        bit <<= (63 - i);
        bit &= number;
        bit >>= (i * (spacing - 1));
        ret |= bit;
    }

    return ret;
}

// Interlace in a modified morton order (high-order bits preserved).
inline uint64_t
interlace(const std::vector<uint64_t>& nums)
{
    uint64_t ret = 0;

    for (size_t i = 0; i < nums.size(); ++i)
    {
        uint64_t spaced = spacing(nums[i], nums.size());
        ret |= (spaced >> i);
    }

    return ret;
}

// Compute a 64-bit point in space from the 64-bits representing each
// dimension.
inline uint64_t
makepoint(const std::vector<uint64_t>& hashes, const std::vector<bool>& mask)
{
    assert(hashes.size() == mask.size());
    std::vector<uint64_t> used;

    for (size_t i = 0; i < hashes.size(); ++i)
    {
        if (mask[i])
        {
            used.push_back(hashes[i]);
        }
    }

    return interlace(used);
}

inline uint64_t
prefixmask(uint8_t prefix)
{
    uint64_t m = 0;

    for (uint8_t i = 0; i < prefix; ++i)
    {
        m = m << 1;
        m = m | 0x0000000000000001;
    }

    m <<= 64 - prefix;
    return m;
}

template <typename R, typename P>
bool
overlap(const R& r1, const P& r2)
{
    uint8_t prefix = std::min(r1.prefix, r2.prefix);
    uint64_t mask = prefixmask(prefix);
    return (r1.space == r2.space
            && r1.subspace == r2.subspace
            && (r1.mask & mask) == (r2.mask & mask));
}

template <typename T>
bool
nonoverlapping(const T& regions)
{
    for (typename T::iterator i = regions.begin(); i != regions.end(); ++i)
    {
        typename T::iterator j = i;
       
        for (++ j; j != regions.end(); ++j)
        {
            if (overlap(*i, *j))
            {
                return false;
            }
        }
    }

    return true;
}

template <typename T, typename R>
bool
nonoverlapping(const T& regions, R region)
{
    for (typename T::iterator i = regions.begin(); i != regions.end(); ++i)
    {
        if (overlap(*i, region))
        {
            return false;
        }
    }

    return true;
}

} // namespace hyperdex

#endif // hyperdex_hyperspace_h_
