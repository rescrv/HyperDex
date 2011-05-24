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

namespace hyperdex
{

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

template <typename T>
bool
nonoverlapping(const T& regions)
{
    for (typename T::iterator i = regions.begin(); i != regions.end(); ++i)
    {
        typename T::iterator j = i;
       
        for (++ j; j != regions.end(); ++j)
        {
            uint64_t mask = prefixmask(std::min(i->prefix, j->prefix));

            if (i->space == j->space && i->subspace == j->subspace
                    && (i->mask & mask) == (j->mask & mask))
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
        uint8_t prefix = std::min(i->prefix, region.prefix);
        uint64_t mask = prefixmask(prefix);

        if (i->space == region.space && i->subspace == region.subspace
                && (i->mask & mask) == (region.mask & mask))
        {
            return false;
        }
    }

    return true;
}

} // namespace hyperdex

#endif // hyperdex_hyperspace_h_
