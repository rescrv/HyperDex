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

#define __STDC_LIMIT_MACROS

// HyperDex
#include "common/range.h"

using hyperdex::range;

range :: range()
    : attr(UINT16_MAX)
    , type(HYPERDATATYPE_GARBAGE)
    , start()
    , end()
    , has_start(false)
    , has_end(false)
    , invalid(true)
{
}

range :: range(const range& other)
    : attr(other.attr)
    , type(other.type)
    , start(other.start)
    , end(other.end)
    , has_start(other.has_start)
    , has_end(other.has_end)
    , invalid(other.invalid)
{
}

range :: ~range() throw ()
{
}

range&
range :: operator = (const range& rhs)
{
    attr = rhs.attr;
    type = rhs.type;
    start = rhs.start;
    end = rhs.end;
    has_start = rhs.has_start;
    has_end = rhs.has_end;
    invalid = rhs.invalid;
    return *this;
}

bool
range :: operator < (const range& rhs) const
{
    const range& lhs(*this);
    return lhs.attr < rhs.attr;
}
