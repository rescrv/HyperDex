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

#ifndef hyperdex_replication_clientop_h_
#define hyperdex_replication_clientop_h_

// e
#include <e/intrusive_ptr.h>

namespace hyperdex
{
namespace replication
{

class clientop
{
    public:
        static uint64_t hash(const replication::clientop& co);

    public:
        clientop();
        clientop(const regionid& r, const entityid& f, uint32_t n);

    public:
        bool operator < (const clientop& rhs) const;
        bool operator == (const clientop& rhs) const
        {
            return region == rhs.region && from == rhs.from && nonce == rhs.nonce;
        }

    public:
        regionid region;
        entityid from;
        uint32_t nonce;
};

uint64_t
clientop :: hash(const clientop& co)
{
    uint64_t nonce = co.nonce;
    return co.region.hash() ^ co.from.hash() ^ nonce;
}

inline
clientop :: clientop()
    : region()
    , from()
    , nonce()
{
}

inline
clientop :: clientop(const regionid& r,
                     const entityid& f,
                     uint32_t n)
    : region(r)
    , from(f)
    , nonce(n)
{
}

inline bool
clientop :: operator < (const clientop& rhs) const
{
    const clientop& lhs(*this);

    if (lhs.region < rhs.region)
    {
        return true;
    }
    else if (lhs.region == rhs.region)
    {
        if (lhs.from < rhs.from)
        {
            return true;
        }
        else if (lhs.from == rhs.from)
        {
            return lhs.nonce < rhs.nonce;
        }
        else
        {
            return false;
        }
    }
    else
    {
        return false;
    }
}

} // namespace replication
} // namespace hyperdex

#endif // hyperdex_replication_clientop_h_
