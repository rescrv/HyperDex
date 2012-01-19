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

#ifndef hyperspacehashing_search_h_
#define hyperspacehashing_search_h_

// STL
#include <vector>

// e
#include <e/bitfield.h>
#include <e/buffer.h>
#include <e/slice.h>

namespace hyperspacehashing
{

class search
{
    public:
        search(size_t n);
        ~search() throw ();

    public:
        bool sanity_check() const;
        size_t size() const;
        bool is_equality(size_t idx) const;
        const e::slice& equality_value(size_t idx) const;
        bool is_range(size_t idx) const;
        void range_value(size_t idx, uint64_t* lower, uint64_t* upper) const;
        bool matches(const e::slice& key, const std::vector<e::slice>& value) const;
        size_t packed_size() const;

    // It is an error to call equality_set or range_set on an index which has
    // already been provided as an index to equality_set or range_set.  It will
    // fail an assertion.  This is to prevent misconceptions about the way in
    // which these two interact.
    public:
        void equality_set(size_t idx, const e::slice& val);
        void range_set(size_t idx, uint64_t start, uint64_t end);

    private:
        friend e::buffer::packer operator << (e::buffer::packer lhs, const search& rhs);
        friend e::buffer::unpacker operator >> (e::buffer::unpacker lhs, search& rhs);

    private:
        e::bitfield m_equality_bits;
        std::vector<e::slice> m_equality;
        e::bitfield m_range_bits;
        std::vector<uint64_t> m_range_lower;
        std::vector<uint64_t> m_range_upper;
};

e::buffer::packer
operator << (e::buffer::packer lhs, const search& rhs);

e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, search& rhs);

} // namespace hyperspacehashing

#endif // hyperspacehashing_search_h_
