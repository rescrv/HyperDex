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

// HyperDex
#include <hyperdex/region.h>

hyperdex :: region :: region(uint8_t p, uint64_t m, std::vector<std::string> h)
    : prefix(p)
    , mask(m)
    , hosts(h)
{
    // XXX zero mask after prefix.
}

hyperdex :: region :: region(const region& other)
    : prefix(other.prefix)
    , mask(other.mask)
    , hosts(other.hosts)
{
}

bool
hyperdex :: region :: operator < (const region& rhs) const
{
    const region& lhs(*this);

    if (lhs.mask < rhs.mask)
    {
        return true;
    }
    else if (lhs.mask > rhs.mask)
    {
        return false;
    }

    return lhs.prefix > rhs.prefix;
}

hyperdex :: region&
hyperdex :: region :: operator = (const region& rhs)
{
    if (this != &rhs)
    {
        prefix = rhs.prefix;
        mask = rhs.mask;
        hosts = rhs.hosts;
    }

    return *this;
}
