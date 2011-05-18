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

// C
#include <cassert>
#include <cstdlib>
#include <cstring>

// POSIX
#include <arpa/inet.h>

// STL
#include <stdexcept>

// e
#include <e/buffer.h>

// HyperDex
#include <hyperdex/entity.h>

using e::buffer;

hyperdex :: entity :: entity(uint32_t _space,
                             uint16_t _subspace,
                             uint8_t _prefix,
                             uint64_t _mask,
                             uint8_t _number)
    : space(_space)
    , subspace(_subspace)
    , prefix(_prefix)
    , mask(_mask)
    , number(_number)
{
}

hyperdex :: entity :: ~entity()
{
}

int
hyperdex :: entity :: compare(const entity& rhs) const
{
    const entity& lhs(*this);

    if (lhs.space < rhs.space)
    {
        return -1;
    }
    else if (lhs.space > rhs.space)
    {
        return 1;
    }

    if (lhs.subspace < rhs.subspace)
    {
        return -1;
    }
    else if (lhs.subspace > rhs.subspace)
    {
        return 1;
    }

    if (lhs.prefix < rhs.prefix)
    {
        return -1;
    }
    else if (lhs.prefix > rhs.prefix)
    {
        return 1;
    }

    if (lhs.mask < rhs.mask)
    {
        return -1;
    }
    else if (lhs.mask > rhs.mask)
    {
        return 1;
    }

    return lhs.number - rhs.number;
}
