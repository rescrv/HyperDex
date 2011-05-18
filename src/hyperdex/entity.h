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

#ifndef hyperdex_entity_h_
#define hyperdex_entity_h_

#define __STDC_LIMIT_MACROS

// C
#include <cstddef>
#include <stdint.h>

// e
#include <e/buffer.h>

namespace hyperdex
{

class entity
{
    public:
        static const size_t SERIALIZEDSIZE = sizeof(uint32_t) + sizeof(uint16_t)
                                           + sizeof(uint8_t) + sizeof(uint64_t)
                                           + sizeof(uint8_t);

    public:
        entity(uint32_t space = 0, uint16_t subspace = 0, uint8_t prefix = 0,
               uint64_t mask = 0, uint8_t number = 0);
        ~entity();

    public:
        // Assume buf is large enough (SERIALIZEDSIZE characters) to hold the data.
        int compare(const entity& rhs) const;

    public:
        bool operator < (const entity& rhs) const { return compare(rhs) < 0; }
        bool operator <= (const entity& rhs) const { return compare(rhs) <= 0; }
        bool operator == (const entity& rhs) const { return compare(rhs) == 0; }
        bool operator >= (const entity& rhs) const { return compare(rhs) >= 0; }
        bool operator > (const entity& rhs) const { return compare(rhs) > 0; }
        bool operator != (const entity& rhs) const { return compare(rhs) != 0; }

    public:
        const uint32_t space;
        const uint16_t subspace;
        const uint8_t prefix;
        const uint64_t mask;
        const uint8_t number;
};

const entity CLIENT = entity(UINT32_MAX, UINT16_MAX, UINT8_MAX, UINT64_MAX, UINT8_MAX);

// Serialize the buffer.
inline e::buffer::packer&
operator << (e::buffer::packer& lhs, const entity& rhs)
{
    lhs << rhs.space << rhs.subspace << rhs.prefix << rhs.mask << rhs.number;
    return lhs;
}

inline e::buffer::unpacker&
operator >> (e::buffer::unpacker& lhs, entity& rhs)
{
    lhs >> const_cast<uint32_t&>(rhs.space)
        >> const_cast<uint16_t&>(rhs.subspace)
        >> const_cast<uint8_t&>(rhs.prefix)
        >> const_cast<uint64_t&>(rhs.mask)
        >> const_cast<uint8_t&>(rhs.number);
    return lhs;
}

} // namespace hyperdex

#endif // hyperdex_entity_h_
