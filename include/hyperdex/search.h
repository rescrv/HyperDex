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

#ifndef hyperdex_search_h_
#define hyperdex_search_h_

// C
#include <cassert>

// STL
#include <vector>

// e
#include <e/bitfield.h>
#include <e/buffer.h>

namespace hyperdex
{

class search
{
    public:
        search(size_t n = 0);
        ~search();

    public:
        void set(size_t idx, const e::buffer& val);
        void unset(size_t idx);
        void clear();

    public:
        bool matches(const e::buffer& key, const std::vector<e::buffer>& value) const;
        bool is_specified(size_t idx) const
        {
            return m_mask.get(idx);
        }

        // This is undefined if !m_mask[idx]
        const e::buffer& dimension(size_t idx) const
        {
            return m_values[idx];
        }

        size_t size() const
        {
            assert(m_values.size() == m_mask.bits());
            return m_values.size();
        }

        uint32_t secondary_point() const;
        uint32_t secondary_mask() const;
        uint64_t replication_point(const std::vector<bool>& dims) const;
        uint64_t replication_mask(const std::vector<bool>& dims) const;

    private:
        friend e::packer& operator << (e::packer& lhs, const search& rhs);
        friend e::unpacker& operator >> (e::unpacker& lhs, search& rhs);

    private:
        std::vector<e::buffer> m_values;
        e::bitfield m_mask;
};

inline e::packer&
operator << (e::packer& lhs, const search& rhs)
{
    lhs << rhs.m_mask << rhs.m_values;
    return lhs;
}

inline e::unpacker&
operator >> (e::unpacker& lhs, search& rhs)
{
    lhs >> rhs.m_mask >> rhs.m_values;
    return lhs;
}

} // namespace hyperdex

#endif // hyperdex_search_h_
