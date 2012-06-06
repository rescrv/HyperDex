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

#ifndef hyperdex_ids_h_
#define hyperdex_ids_h_

// C++
#include <iostream>

// e
#include <e/buffer.h>
#include <e/tuple_compare.h>

// HyperspaceHashing
#include "hyperspacehashing/hyperspacehashing/prefix.h"

namespace hyperdex
{

// Ideally I'd like these to be related via subclassing, but it screws up the
// comparison operators when used with STL containers.

class spaceid
{
    public:
        static const size_t SERIALIZEDSIZE = sizeof(uint32_t);

    public:
        spaceid()
            : space(0)
        {
        }

        spaceid(uint32_t s)
            : space(s)
        {
        }

        virtual ~spaceid()
        {
        }

    public:
        uint64_t hash() const
        {
            return space;
        }

    public:
        uint32_t space;
};

class subspaceid
{
    public:
        static const size_t SERIALIZEDSIZE = sizeof(uint32_t) + sizeof(uint16_t);

    public:
        subspaceid()
            : space(0)
            , subspace(0)
        {
        }

        subspaceid(uint32_t s, uint16_t ss)
            : space(s)
            , subspace(ss)
        {
        }

        subspaceid(spaceid s, uint16_t ss)
            : space(s.space)
            , subspace(ss)
        {
        }

        ~subspaceid()
        {
        }

    public:
        spaceid get_space() const
        {
            return spaceid(space);
        }

        uint64_t hash() const
        {
            return (space << 16) | subspace;
        }

    public:
        uint32_t space;
        uint16_t subspace;
};

class regionid
{
    public:
        static const size_t SERIALIZEDSIZE = sizeof(uint32_t) + sizeof(uint16_t)
                                           + sizeof(uint8_t) + sizeof(uint64_t);

    public:
        regionid()
            : mask(0)
            , space(0)
            , subspace(0)
            , prefix(0)
        {
        }

        regionid(uint32_t s, uint16_t ss, uint8_t p, uint64_t m)
            : mask(m)
            , space(s)
            , subspace(ss)
            , prefix(p)
        {
        }

        regionid(subspaceid ss, uint8_t p, uint64_t m)
            : mask(m)
            , space(ss.space)
            , subspace(ss.subspace)
            , prefix(p)
        {
        }

        ~regionid()
        {
        }

    public:
        subspaceid get_subspace() const
        {
            return subspaceid(space, subspace);
        }

        spaceid get_space() const
        {
            return spaceid(space);
        }

        uint64_t hash() const
        {
            return ((space << 16) | subspace) | mask;
        }

    public:
        // XXX Turn this into a member of the object
        hyperspacehashing::prefix::coordinate coord() const
        { return hyperspacehashing::prefix::coordinate(prefix, mask); }

    public:
        uint64_t mask;
        uint32_t space;
        uint16_t subspace;
        uint8_t prefix;
};

class entityid
{
    public:
        static const size_t SERIALIZEDSIZE = sizeof(uint32_t) + sizeof(uint16_t)
                                           + sizeof(uint8_t) + sizeof(uint64_t)
                                           + sizeof(uint8_t);

    public:
        entityid()
            : mask(0)
            , space(0)
            , subspace(0)
            , prefix(0)
            , number(0)
        {
        }

        entityid(uint32_t s, uint16_t ss,
                 uint8_t p, uint64_t m, uint8_t n)
            : mask(m)
            , space(s)
            , subspace(ss)
            , prefix(p)
            , number(n)
        {
        }

        entityid(regionid r, uint8_t n)
            : mask(r.mask)
            , space(r.space)
            , subspace(r.subspace)
            , prefix(r.prefix)
            , number(n)
        {
        }

        ~entityid()
        {
        }

    public:
        regionid get_region() const
        {
            return regionid(space, subspace, prefix, mask);
        }

        subspaceid get_subspace() const
        {
            return subspaceid(space, subspace);
        }

        spaceid get_space() const
        {
            return spaceid(space);
        }

        uint64_t hash() const
        {
            uint64_t newprefix = prefix;
            newprefix <<= 8;
            return (((space << 16) | subspace) | mask) + (newprefix + number);
        }

    public:
        // XXX Turn this into a member of the object
        hyperspacehashing::prefix::coordinate coord() const
        { return hyperspacehashing::prefix::coordinate(prefix, mask); }

    public:
        uint64_t mask;
        uint32_t space;
        uint16_t subspace;
        uint8_t prefix;
        uint8_t number;
};

// Serialization functions.
inline e::buffer::packer
operator << (e::buffer::packer lhs, const spaceid& rhs)
{
    return lhs << rhs.space;
}

inline e::buffer::packer
operator << (e::buffer::packer lhs, const subspaceid& rhs)
{
    return lhs << rhs.get_space() << rhs.subspace;
}

inline e::buffer::packer
operator << (e::buffer::packer lhs, const regionid& rhs)
{
    return lhs << rhs.get_subspace() << rhs.prefix << rhs.mask;
}

inline e::buffer::packer
operator << (e::buffer::packer lhs, const entityid& rhs)
{
    return lhs << rhs.get_region() << rhs.number;
}

// Deserialization functions.
inline e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, spaceid& rhs)
{
    return lhs >> rhs.space;
}

inline e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, subspaceid& rhs)
{
    return lhs >> rhs.space >> rhs.subspace;
}

inline e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, regionid& rhs)
{
    return lhs >> rhs.space >> rhs.subspace
               >> rhs.prefix >> rhs.mask;
}

inline e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, entityid& rhs)
{
    return lhs >> rhs.space >> rhs.subspace
               >> rhs.prefix >> rhs.mask >> rhs.number;
}

// Comparison functions
inline int
compare(const spaceid& lhs, const spaceid& rhs)
{
    return e::tuple_compare(lhs.space,
                            rhs.space);
}

inline int
compare(const subspaceid& lhs, const subspaceid& rhs)
{
    return e::tuple_compare(lhs.space, lhs.subspace,
                            rhs.space, rhs.subspace);
}

inline int
compare(const regionid& lhs, const regionid& rhs)
{
    // XXX Order by mask then prefix.
    return e::tuple_compare(lhs.space, lhs.subspace, lhs.prefix, lhs.mask,
                            rhs.space, rhs.subspace, rhs.prefix, rhs.mask);
}

inline int
compare(const entityid& lhs, const entityid& rhs)
{
    // XXX Order by mask then prefix.
    return e::tuple_compare(lhs.space, lhs.subspace, lhs.prefix, lhs.mask, lhs.number,
                            rhs.space, rhs.subspace, rhs.prefix, rhs.mask, rhs.number);
}

inline bool operator < (const spaceid& lhs, const spaceid& rhs) { return compare(lhs, rhs) < 0; }
inline bool operator <= (const spaceid& lhs, const spaceid& rhs) { return compare(lhs, rhs) <= 0; }
inline bool operator == (const spaceid& lhs, const spaceid& rhs) { return compare(lhs, rhs) == 0; }
inline bool operator != (const spaceid& lhs, const spaceid& rhs) { return compare(lhs, rhs) != 0; }
inline bool operator >= (const spaceid& lhs, const spaceid& rhs) { return compare(lhs, rhs) >= 0; }
inline bool operator > (const spaceid& lhs, const spaceid& rhs) { return compare(lhs, rhs) > 0; }

inline bool operator < (const subspaceid& lhs, const subspaceid& rhs) { return compare(lhs, rhs) < 0; }
inline bool operator <= (const subspaceid& lhs, const subspaceid& rhs) { return compare(lhs, rhs) <= 0; }
inline bool operator == (const subspaceid& lhs, const subspaceid& rhs) { return compare(lhs, rhs) == 0; }
inline bool operator != (const subspaceid& lhs, const subspaceid& rhs) { return compare(lhs, rhs) != 0; }
inline bool operator >= (const subspaceid& lhs, const subspaceid& rhs) { return compare(lhs, rhs) >= 0; }
inline bool operator > (const subspaceid& lhs, const subspaceid& rhs) { return compare(lhs, rhs) > 0; }

inline bool operator < (const regionid& lhs, const regionid& rhs) { return compare(lhs, rhs) < 0; }
inline bool operator <= (const regionid& lhs, const regionid& rhs) { return compare(lhs, rhs) <= 0; }
inline bool operator == (const regionid& lhs, const regionid& rhs) { return compare(lhs, rhs) == 0; }
inline bool operator != (const regionid& lhs, const regionid& rhs) { return compare(lhs, rhs) != 0; }
inline bool operator >= (const regionid& lhs, const regionid& rhs) { return compare(lhs, rhs) >= 0; }
inline bool operator > (const regionid& lhs, const regionid& rhs) { return compare(lhs, rhs) > 0; }

inline bool operator < (const entityid& lhs, const entityid& rhs) { return compare(lhs, rhs) < 0; }
inline bool operator <= (const entityid& lhs, const entityid& rhs) { return compare(lhs, rhs) <= 0; }
inline bool operator == (const entityid& lhs, const entityid& rhs) { return compare(lhs, rhs) == 0; }
inline bool operator != (const entityid& lhs, const entityid& rhs) { return compare(lhs, rhs) != 0; }
inline bool operator >= (const entityid& lhs, const entityid& rhs) { return compare(lhs, rhs) >= 0; }
inline bool operator > (const entityid& lhs, const entityid& rhs) { return compare(lhs, rhs) > 0; }

// Output stream operators
inline std::ostream&
operator << (std::ostream& lhs, const spaceid& rhs)
{
    std::ios_base::fmtflags fl = lhs.flags();
    lhs << "space(space=" << rhs.space
        << ")";
    lhs.flags(fl);
    return lhs;
}

inline std::ostream&
operator << (std::ostream& lhs, const subspaceid& rhs)
{
    std::ios_base::fmtflags fl = lhs.flags();
    lhs << "subspace(space=" << rhs.space
        << ", subspace=" << rhs.subspace
        << ")";
    lhs.flags(fl);
    return lhs;
}

inline std::ostream&
operator << (std::ostream& lhs, const regionid& rhs)
{
    std::ios_base::fmtflags fl = lhs.flags();
    lhs << "region(space=" << rhs.space
        << ", subspace=" << rhs.subspace
        << ", prefix=" << (unsigned int) rhs.prefix
        << ", mask=" << rhs.mask
        << ")";
    lhs.flags(fl);
    return lhs;
}

inline std::ostream&
operator << (std::ostream& lhs, const entityid& rhs)
{
    std::ios_base::fmtflags fl = lhs.flags();
    lhs << "entity(space=" << rhs.space
        << ", subspace=" << rhs.subspace
        << ", prefix=" << (unsigned int) rhs.prefix
        << ", mask=" << rhs.mask
        << ", number=" << (unsigned int) rhs.number
        << ")";
    lhs.flags(fl);
    return lhs;
}

} // namespace hyperdex

#endif // hyperdex_ids_h_
