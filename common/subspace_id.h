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

#ifndef hyperdex_common_subspace_id_h_
#define hyperdex_common_subspace_id_h_

// C
#include <stdint.h>

namespace hyperdex
{

class subspace_id
{
    public:
        subspace_id() : m_id() {}
        explicit subspace_id(uint64_t id) : m_id(id) {}

    public:
        uint64_t get() const { return m_id; }
        uint64_t hash() const { return m_id; }

    private:
        uint64_t m_id;
}; 

inline std::ostream&
operator << (std::ostream& lhs, const subspace_id& rhs)
{
    return lhs << "subspace(" << rhs.get() << ")";
}

inline bool
operator == (const subspace_id& lhs, const subspace_id& rhs)
{
    return lhs.get() == rhs.get();
}

inline bool
operator != (const subspace_id& lhs, const subspace_id& rhs)
{
    return lhs.get() != rhs.get();
}

} // namespace hyperdex

#endif // hyperdex_common_subspace_id_h_
