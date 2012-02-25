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
#include "hyperspacehashing/hyperspacehashing/hashes.h"
#include "hyperspacehashing/hyperspacehashing/mask.h"
#include "hyperspacehashing/hyperspacehashing/prefix.h"
#include "hyperspacehashing/hyperspacehashing/search.h"
#include "hyperspacehashing/hashes_internal.h"

using hyperspacehashing::hash_t;
using hyperspacehashing::EQUALITY;
using hyperspacehashing::NONE;
using namespace hyperspacehashing;

void
validate(const std::vector<hash_t> hf, const e::slice& key, const std::vector<e::slice>& value);

#define TESTPOS(X) \
    assert(hf.size() == ((X) + 1)); \
    assert(value.size() == (X)); \
    hf[(X)] = static_cast<hash_t>(v##X); \
    value[(X) - 1] = e::slice(&nums[(X)], sizeof(nums[(X)])); \
    validate(hf, key, value)

#define EXPAND \
    hf.push_back(NONE); \
    value.push_back(e::slice())

#define SHRINK \
    value.pop_back(); \
    hf.pop_back()

int
main(int, char* [])
{
    srand(time(NULL));

    for (size_t i = 0; i < 100; ++i)
    {
        uint32_t nums[5];

        for (int j = 0; j < 5; ++j)
        {
            nums[j] = static_cast<uint32_t>(rand());
        }

        for (size_t k = EQUALITY; k <= NONE; ++k)
        {
            std::vector<hash_t> hf(1);
            hf[0] = static_cast<hash_t>(k);

            std::vector<e::slice> value;
            e::slice key(&nums[0], sizeof(nums[0]));
            validate(hf, key, value);
            EXPAND;

            for (size_t v1 = EQUALITY; v1 <= NONE; ++v1)
            {
                TESTPOS(1);
                EXPAND;

                for (size_t v2 = EQUALITY; v2 <= NONE; ++v2)
                {
                    TESTPOS(2);
                    EXPAND;

                    for (size_t v3 = EQUALITY; v3 <= NONE; ++v3)
                    {
                        TESTPOS(3);
                        EXPAND;

                        for (size_t v4 = EQUALITY; v4 <= NONE; ++v4)
                        {
                            TESTPOS(4);
                        }

                        SHRINK;
                    }

                    SHRINK;
                }

                SHRINK;
            }
        }
    }

    return EXIT_SUCCESS;
}

void
validate(const std::vector<hash_t> hf, const e::slice& key, const std::vector<e::slice>& value)
{
    prefix::hasher ph(hf);
    prefix::coordinate pc = ph.hash(key, value);
    mask::hasher mh(hf);
    mask::coordinate mc = mh.hash(key, value);

    // Do an equality search for each attribute
    for (size_t i = 0; i < value.size() + 1; ++i)
    {
        search s(value.size() + 1);

        if (i == 0)
        {
            s.equality_set(0, key);
        }
        else
        {
            s.equality_set(i, value[i - 1]);
        }

        prefix::search_coordinate psc = ph.hash(s);
        mask::coordinate msc = mh.hash(s);
        assert(psc.matches(pc));
        assert(msc.intersects(mc));
    }

    // Do a range search for each attribute
    for (size_t i = 0; i < value.size() + 1; ++i)
    {
        search s(value.size() + 1);
        uint64_t num;

        if (i == 0)
        {
            num = lendian(key);
            s.range_set(0, num, num + 1);
        }
        else
        {
            num = lendian(value[i - 1]);
            s.range_set(i, num, num + 1);
        }

        prefix::search_coordinate psc = ph.hash(s);
        mask::coordinate msc = mh.hash(s);
        assert(psc.matches(pc));
        assert(msc.intersects(mc));
    }

    // Do an equality/range search for two attributes
    for (size_t i = 0; i < value.size() + 1; ++i)
    {
        for (size_t j = 0; j < value.size() + 1; ++j)
        {
            if (i == j)
            {
                continue;
            }

            search s(value.size() + 1);

            if (i == 0)
            {
                s.equality_set(0, key);
            }
            else
            {
                s.equality_set(i, value[i - 1]);
            }

            if (j == 0)
            {
                uint64_t num = lendian(key);
                s.range_set(0, num, num + 1);
            }
            else
            {
                uint64_t num = lendian(value[j - 1]);
                s.range_set(j, num, num + 1);
            }

            prefix::search_coordinate psc = ph.hash(s);
            mask::coordinate msc = mh.hash(s);
            assert(psc.matches(pc));
            assert(msc.intersects(mc));
        }
    }
}
