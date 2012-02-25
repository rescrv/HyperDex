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
#include <ctime>

// HyperspaceHashing
#include "hyperspacehashing/cfloat.h"

using hyperspacehashing::cfloat;

// Validate that (a < b) => (cfloat(a) <= cfloat(b)) and
//               (a > b) => (cfloat(a) >= cfloat(b)) and
//               (a == b) => (cfloat(a) == cfloat(b))
//
// This will abort on failure.
void
validate(uint64_t a, uint64_t b);

int
main(int, char* [])
{
    uint64_t enc;

    // Randomly go through some other numbers.
    unsigned int seed = time(NULL);
    uint64_t old = 0;

    for (size_t c = 0; c < 100; ++c)
    {
        int random = rand_r(&seed);
        uint64_t cur = UINT64_MAX * (static_cast<double>(random) / RAND_MAX);
        validate(old, cur);
        validate(cur, cur);
        validate(cur, 0);
        validate(cur, UINT64_MAX);

        for (uint64_t i = cur; i < cur + 1000; ++i)
        {
            validate(cur, i);
        }

        old = cur;
    }

    return EXIT_SUCCESS;
}

void
validate(uint64_t a, uint64_t b)
{
    for (size_t sz = 1; sz <= 64; ++sz)
    {
        uint64_t enc_a = cfloat(a, sz);
        uint64_t enc_b = cfloat(b, sz);

        if (a < b && enc_a > enc_b)
        {
            abort();
        }

        if (a > b && enc_a < enc_b)
        {
            abort();
        }

        if (a == b && enc_a != enc_b)
        {
            abort();
        }
    }
}
