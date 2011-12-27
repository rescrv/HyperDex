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

#ifndef hyperspacehashing_range_match_h_
#define hyperspacehashing_range_match_h_

// HyperspaceHashing
#include "hyperspacehashing/mask.h"
#include "hyperspacehashing/prefix.h"

namespace hyperspacehashing
{

class range_match
{
    public:
        range_match(unsigned int idx,
                    uint64_t lower, uint64_t upper,
                    uint64_t cmask,
                    uint64_t clower, uint64_t cupper);

    public:
        bool matches(const prefix::coordinate& coord) const;
        bool matches(const e::slice& key,
                     const std::vector<e::slice>& value) const;

    public:
        unsigned int m_idx;
        uint64_t m_lower;
        uint64_t m_upper;
        uint64_t m_cmask;
        uint64_t m_clower;
        uint64_t m_cupper;
};

} // namespace hyperspacehashing

#endif // hyperspacehashing_range_match_h
