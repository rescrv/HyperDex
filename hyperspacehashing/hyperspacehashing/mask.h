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

#ifndef hyperspacehashing_mask_h_
#define hyperspacehashing_mask_h_

// STL
#include <iostream>

// HyperspaceHashing
#include <hyperspacehashing/hashes.h>
#include <hyperspacehashing/search.h>

namespace hyperspacehashing
{
namespace mask
{

class coordinate
{
    public:
        coordinate();
        coordinate(uint64_t pmask, uint64_t phash,
                   uint64_t slmask, uint64_t slhash,
                   uint64_t sumask, uint64_t suhash);
        coordinate(const coordinate& other);
        ~coordinate() throw ();

    public:
        bool intersects(const coordinate& other) const;
        bool primary_intersects(const coordinate& other) const;
        bool secondary_intersects(const coordinate& other) const;

    public:
        bool operator == (const coordinate& rhs) const;

    public:
        uint64_t primary_mask;
        uint64_t primary_hash;
        uint64_t secondary_lower_mask;
        uint64_t secondary_lower_hash;
        uint64_t secondary_upper_mask;
        uint64_t secondary_upper_hash;
};

class hasher
{
    public:
        hasher(const std::vector<hash_t>& funcs);
        hasher(const hasher& other);
        ~hasher() throw ();

    public:
        coordinate hash(const e::slice& key) const;
        coordinate hash(const e::slice& key, const std::vector<e::slice>& value) const;
        coordinate hash(const std::vector<e::slice>& value) const;
        coordinate hash(const search& s) const;

    public:
        hasher& operator = (const hasher& rhs);

    private:
        std::vector<hash_t> m_funcs;
        unsigned int m_num;
        std::vector<unsigned int> m_nums;
        std::vector<unsigned int> m_space;
};

} // namespace mask
} // namespace hyperspacehashing

#endif // hyperspacehashing_mask_h_
