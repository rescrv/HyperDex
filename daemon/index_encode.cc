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

// C
#include <cassert>
#include <cmath>
#include <cstdlib>

// Linux
#ifdef __APPLE__
#include <osx/ieee754.h>
#else
#include <ieee754.h>
#endif

// C++
#include <iostream>

// e
#include <e/endian.h>

// HyperDex
#include "common/float_encode.h"
#include "daemon/index_encode.h"

// We flip the sign bit and shift appropriately to make sure that INT64_MIN
// corresponds to 0 and INT64_MAX corresponds to UINT64_MAX.
char*
hyperdex :: index_encode_int64(int64_t in, char* ptr)
{
    uint64_t out = static_cast<uint64_t>(in);
    out += in >= 0 ? 0x8000000000000000ULL : INT64_MIN;
    return e::pack64be(out, ptr);
}

char*
hyperdex :: index_encode_double(double x, char* ptr)
{
    return e::pack64be(float_encode(x), ptr);
}

void
hyperdex :: index_encode_bump(char* _ptr, char* _end)
{
    assert(_ptr);
    assert(_ptr < _end);
    uint8_t* ptr = reinterpret_cast<uint8_t*>(_end) - 1;
    uint8_t* end = reinterpret_cast<uint8_t*>(_ptr);

    for (; ptr >= end; --ptr)
    {
        if (*ptr < 255)
        {
            ++(*ptr);
            return;
        }
        else
        {
            *ptr = 0;
        }
    }

    abort();
}
