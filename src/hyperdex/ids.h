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

namespace hyperdex
{

class spaceid
{
    public:
        static const size_t SERIALIZEDSIZE = sizeof(uint32_t);

    public:
        spaceid(uint32_t s = 0)
            : space(s)
        {
        }

        virtual ~spaceid()
        {
        }

    public:
        template <typename T> bool operator < (const T& rhs) const { return compare(*this, rhs) < 0; }
        template <typename T> bool operator <= (const T& rhs) const { return compare(*this, rhs) <= 0; }
        template <typename T> bool operator == (const T& rhs) const { return compare(*this, rhs) == 0; }
        template <typename T> bool operator >= (const T& rhs) const { return compare(*this, rhs) >= 0; }
        template <typename T> bool operator > (const T& rhs) const { return compare(*this, rhs) > 0; }
        template <typename T> bool operator != (const T& rhs) const { return compare(*this, rhs) != 0; }

    public:
        uint32_t space;
};

class subspaceid : public spaceid
{
    public:
        static const size_t SERIALIZEDSIZE = spaceid::SERIALIZEDSIZE + sizeof(uint16_t);

    public:
        subspaceid(uint32_t s = 0, uint16_t ss = 0)
            : spaceid(s)
            , subspace(ss)
        {
        }

        ~subspaceid()
        {
        }

    public:
        uint16_t subspace;
};

class regionid : public subspaceid
{
    public:
        static const size_t SERIALIZEDSIZE = subspaceid::SERIALIZEDSIZE + sizeof(uint8_t) + sizeof(uint64_t);

    public:
        regionid(uint32_t s = 0, uint16_t ss = 0, uint8_t p = 0, uint64_t m = 0)
            : subspaceid(s, ss)
            , prefix(p)
            , mask(m)
        {
        }

        ~regionid()
        {
        }

    public:
        uint8_t prefix;
        uint64_t mask;
};

class entityid : public regionid
{
    public:
        static const size_t SERIALIZEDSIZE = regionid::SERIALIZEDSIZE + sizeof(uint8_t);

    public:
        entityid(uint32_t s = 0, uint16_t ss = 0,
                 uint8_t p = 0, uint64_t m = 0, uint8_t n = 0)
            : regionid(s, ss, p, m)
            , number(n)
        {
        }

        ~entityid()
        {
        }

    public:
        uint8_t number;
};

// Serialization functions.
inline e::buffer::packer&
operator << (e::buffer::packer& lhs, const spaceid& rhs)
{
    lhs << rhs.space;
    return lhs;
}

inline e::buffer::packer&
operator << (e::buffer::packer& lhs, const subspaceid& rhs)
{
    lhs << dynamic_cast<const spaceid&>(rhs) << rhs.subspace;
    return lhs;
}

inline e::buffer::packer&
operator << (e::buffer::packer& lhs, const regionid& rhs)
{
    lhs << dynamic_cast<const subspaceid&>(rhs) << rhs.prefix << rhs.mask;
    return lhs;
}

inline e::buffer::packer&
operator << (e::buffer::packer& lhs, const entityid& rhs)
{
    lhs << dynamic_cast<const regionid&>(rhs) << rhs.number;
    return lhs;
}

// Deserialization functions.
inline e::buffer::unpacker&
operator >> (e::buffer::unpacker& lhs, spaceid& rhs)
{
    lhs >> rhs.space;
    return lhs;
}

inline e::buffer::unpacker&
operator >> (e::buffer::unpacker& lhs, subspaceid& rhs)
{
    lhs >> dynamic_cast<spaceid&>(rhs) >> rhs.subspace;
    return lhs;
}

inline e::buffer::unpacker&
operator >> (e::buffer::unpacker& lhs, regionid& rhs)
{
    lhs >> dynamic_cast<subspaceid&>(rhs) >> rhs.prefix >> rhs.mask;
    return lhs;
}

inline e::buffer::unpacker&
operator >> (e::buffer::unpacker& lhs, entityid& rhs)
{
    lhs >> dynamic_cast<regionid&>(rhs) >> rhs.number;
    return lhs;
}

// Comparison functions
inline int
compare(const spaceid& lhs, const spaceid& rhs)
{
    return lhs.space - rhs.space;
}

inline int
compare(const subspaceid& lhs, const subspaceid& rhs)
{
    int super = compare(dynamic_cast<const spaceid&>(lhs), dynamic_cast<const spaceid&>(rhs));

    if (super)
    {
        return super;
    }

    return lhs.subspace - rhs.subspace;
}

inline int
compare(const regionid& lhs, const regionid& rhs)
{
    int super = compare(dynamic_cast<const subspaceid&>(lhs), dynamic_cast<const subspaceid&>(rhs));

    if (super)
    {
        return super;
    }

    if (lhs.prefix < rhs.prefix)
    {
        return -1;
    }
    else if (lhs.prefix > rhs.prefix)
    {
        return 1;
    }
    else
    {
        return lhs.mask - rhs.mask;
    }
}

inline int
compare(const entityid& lhs, const entityid& rhs)
{
    int super = compare(dynamic_cast<const regionid&>(lhs), dynamic_cast<const regionid&>(rhs));

    if (super)
    {
        return super;
    }

    return lhs.number - rhs.number;
}

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
        << ", prefix=" << rhs.prefix
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
        << ", prefix=" << rhs.prefix
        << ", mask=" << rhs.mask
        << ", number=" << rhs.number
        << ")";
    lhs.flags(fl);
    return lhs;
}

} // namespace hyperdex

#endif // hyperdex_ids_h_
