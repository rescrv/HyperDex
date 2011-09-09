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

#ifndef hyperspacehashing_equality_wildcard_h_
#define hyperspacehashing_equality_wildcard_h_

// STL
#include <vector>

// e
#include <e/bitfield.h>
#include <e/buffer.h>

namespace hyperspacehashing
{

class equality_wildcard
{
    public:
        equality_wildcard(size_t n);
        ~equality_wildcard() throw ();

    public:
        size_t size() const { return m_value.size(); }
        bool isset(size_t idx) const { return m_mask.get(idx); }
        const e::buffer& get(size_t idx) const { return m_value[idx]; }

    public:
        void set(size_t idx, const e::buffer& val);
        void unset(size_t idx);
        void clear();

    private:
        e::bitfield m_mask;
        std::vector<e::buffer> m_value;
};

e::packer&
operator << (e::packer& lhs, const equality_wildcard& rhs);

e::unpacker&
operator >> (e::unpacker& lhs, equality_wildcard& rhs);

} // namespace hyperspacehashing

#endif // hyperspacehashing_equality_wildcard_h_
