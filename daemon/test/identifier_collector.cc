// Copyright (c) 2013, Cornell University
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
#include <cmath>
#include <stdint.h>

// HyperDex
#include "test/th.h"
#include "daemon/identifier_collector.h"

using hyperdex::identifier_collector;
using hyperdex::region_id;

TEST(IdentifierCollector, Test)
{
    e::garbage_collector gc;
    identifier_collector ic(&gc);
    uint64_t id;
    // adopt a region
    region_id ri(1);
    ic.adopt(&ri, 1);
    // first try, nothing collected
    id = ic.lower_bound(ri);
    ASSERT_EQ(id, 1U);
    // collect one
    ic.collect(ri, 1);
    id = ic.lower_bound(ri);
    ASSERT_EQ(id, 2U);
    // collect three
    ic.collect(ri, 3);
    id = ic.lower_bound(ri);
    ASSERT_EQ(id, 2U);
    // collect three again
    ic.collect(ri, 3);
    id = ic.lower_bound(ri);
    ASSERT_EQ(id, 2U);
    // collect two
    ic.collect(ri, 2);
    id = ic.lower_bound(ri);
    ASSERT_EQ(id, 4U);
    // resize
    region_id ris[2];
    ris[0] = region_id(2);
    ris[1] = region_id(1);
    ic.adopt(ris, 2);
    // check lower bound
    id = ic.lower_bound(ri);
    ASSERT_EQ(id, 4U);
    // bump!
    ic.bump(ri, 9);
    id = ic.lower_bound(ri);
    ASSERT_EQ(id, 9U);
    id = ic.lower_bound(ri);
    ASSERT_EQ(id, 9U);

    for (uint64_t i = 9; i < 65536; ++i)
    {
        ic.collect(ri, i);
        id = ic.lower_bound(ri);
        ASSERT_EQ(id, i + 1);
    }
}
