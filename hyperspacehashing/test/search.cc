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
#include "hashes_internal.h"
#include "hyperspacehashing/hashes.h"
#include "hyperspacehashing/mask.h"
#include "hyperspacehashing/prefix.h"
#include "hyperspacehashing/search.h"

using hyperspacehashing::hash_t;
using hyperspacehashing::EQUALITY;
using hyperspacehashing::NONE;
using namespace hyperspacehashing;

void
validate(const std::vector<hash_t> hf, const e::buffer& key, const std::vector<e::buffer>& value);

int
main(int, char* [])
{
    srand(time(NULL));

    for (size_t i = 0; i < 10; ++i)
    {
        uint32_t nums[5];

        for (int j = 0; j < 5; ++j)
        {
            nums[i] = static_cast<uint32_t>(rand());
        }

        for (size_t k = EQUALITY; k <= NONE; ++k)
        {
            std::vector<hash_t> hf(1);
            hf[0] = static_cast<hash_t>(k);

            std::vector<e::buffer> value;
            e::buffer key;
            key.pack() << nums[0];
            validate(hf, key, value);
            hf.push_back(NONE);
            value.push_back(e::buffer());

            for (size_t v1 = EQUALITY; v1 <= NONE; ++v1)
            {
                assert(hf.size() == 2);
                assert(value.size() == 1);
                hf[1] = static_cast<hash_t>(v1);
                value[0].pack() << nums[1];
                validate(hf, key, value);
                hf.push_back(NONE);
                value.push_back(e::buffer());

                for (size_t v2 = EQUALITY; v2 <= NONE; ++v2)
                {
                    assert(hf.size() == 3);
                    assert(value.size() == 2);
                    hf[2] = static_cast<hash_t>(v2);
                    value[1].pack() << nums[2];
                    validate(hf, key, value);
                    hf.push_back(NONE);
                    value.push_back(e::buffer());

                    for (size_t v3 = EQUALITY; v3 <= NONE; ++v3)
                    {
                        assert(hf.size() == 4);
                        assert(value.size() == 3);
                        hf[3] = static_cast<hash_t>(v3);
                        value[2].pack() << nums[3];
                        validate(hf, key, value);
                        hf.push_back(NONE);
                        value.push_back(e::buffer());

                        for (size_t v4 = EQUALITY; v4 <= NONE; ++v4)
                        {
                            assert(hf.size() == 5);
                            assert(value.size() == 4);
                            hf[4] = static_cast<hash_t>(v4);
                            value[3].pack() << nums[4];
                            validate(hf, key, value);
                        }

                        value.pop_back();
                        hf.pop_back();
                    }

                    value.pop_back();
                    hf.pop_back();
                }

                value.pop_back();
                hf.pop_back();
            }
        }
    }

    return EXIT_SUCCESS;
}

void
validate(const std::vector<hash_t> hf, const e::buffer& key, const std::vector<e::buffer>& value)
{
    prefix::hasher ph(hf);
    prefix::coordinate pc = ph.hash(key, value);
    //mask::hasher mh(hf);
    //mask::coordinate mc = mh.hash(key, value);

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
        //mask::search_coordinate msc = mh.hash(s);
        assert(psc.matches(pc));
        //assert(msc.matches(mc));
        //assert(msc.matches(key, value));
    }

    // Do a range search for each attribute
    for (size_t i = 0; i < value.size() + 1; ++i)
    {
        search s(value.size() + 1);

        if (i == 0)
        {
            uint64_t num = lendian(key);
            s.range_set(0, num, num + 1);
        }
        else
        {
            uint64_t num = lendian(value[i - 1]);
            s.range_set(i, num, num + 1);
        }

        prefix::search_coordinate psc = ph.hash(s);
        //mask::search_coordinate msc = mh.hash(s);
        assert(psc.matches(pc));
        //assert(msc.matches(mc));
        //assert(msc.matches(key, value));
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
            //mask::search_coordinate msc = mh.hash(s);
            assert(psc.matches(pc));
            //assert(msc.matches(mc));
            //assert(msc.matches(key, value));
        }
    }
}
