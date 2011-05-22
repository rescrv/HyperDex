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

// Ideally I'd like these to be related via subclassing, but it screws up the
// comparison operators when used with STL containers.

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
        uint32_t space;
};

class subspaceid
{
    public:
        static const size_t SERIALIZEDSIZE = sizeof(uint32_t) + sizeof(uint16_t);

    public:
        subspaceid(uint32_t s = 0, uint16_t ss = 0)
            : space(s)
            , subspace(ss)
        {
        }

        subspaceid(spaceid s, uint16_t ss = 0)
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
        regionid(uint32_t s = 0, uint16_t ss = 0, uint8_t p = 0, uint64_t m = 0)
            : space(s)
            , subspace(ss)
            , prefix(p)
            , mask(m)
        {
        }

        regionid(subspaceid ss, uint8_t p = 0, uint64_t m = 0)
            : space(ss.space)
            , subspace(ss.subspace)
            , prefix(p)
            , mask(m)
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

    public:
        uint32_t space;
        uint16_t subspace;
        uint8_t prefix;
        uint64_t mask;
};

class entityid
{
    public:
        static const size_t SERIALIZEDSIZE = sizeof(uint32_t) + sizeof(uint16_t)
                                           + sizeof(uint8_t) + sizeof(uint64_t)
                                           + sizeof(uint8_t);

    public:
        entityid(uint32_t s = 0, uint16_t ss = 0,
                 uint8_t p = 0, uint64_t m = 0, uint8_t n = 0)
            : space(s)
            , subspace(ss)
            , prefix(p)
            , mask(m)
            , number(n)
        {
        }

        entityid(regionid r, uint8_t n = 0)
            : space(r.space)
            , subspace(r.subspace)
            , prefix(r.prefix)
            , mask(r.mask)
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

    public:
        uint32_t space;
        uint16_t subspace;
        uint8_t prefix;
        uint64_t mask;
        uint8_t number;
};

// Serialization functions.
inline e::packer&
operator << (e::packer& lhs, const spaceid& rhs)
{
    lhs << rhs.space;
    return lhs;
}

inline e::packer&
operator << (e::packer& lhs, const subspaceid& rhs)
{
    lhs << rhs.get_space() << rhs.subspace;
    return lhs;
}

inline e::packer&
operator << (e::packer& lhs, const regionid& rhs)
{
    lhs << rhs.get_subspace() << rhs.prefix << rhs.mask;
    return lhs;
}

inline e::packer&
operator << (e::packer& lhs, const entityid& rhs)
{
    lhs << rhs.get_region() << rhs.number;
    return lhs;
}

// Deserialization functions.
inline e::unpacker&
operator >> (e::unpacker& lhs, spaceid& rhs)
{
    lhs >> rhs.space;
    return lhs;
}

inline e::unpacker&
operator >> (e::unpacker& lhs, subspaceid& rhs)
{
    lhs >> rhs.space >> rhs.subspace;
    return lhs;
}

inline e::unpacker&
operator >> (e::unpacker& lhs, regionid& rhs)
{
    lhs >> rhs.space >> rhs.subspace >> rhs.prefix >> rhs.mask;
    return lhs;
}

inline e::unpacker&
operator >> (e::unpacker& lhs, entityid& rhs)
{
    lhs >> rhs.space >> rhs.subspace >> rhs.prefix >> rhs.mask >> rhs.number;
    return lhs;
}

// Comparison functions
inline int
compare(const spaceid& lhs, const spaceid& rhs)
{
    if (lhs.space < rhs.space)
    {
        return -1;
    }
    else if (lhs.space > rhs.space)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

inline int
compare(const subspaceid& lhs, const subspaceid& rhs)
{
    int super = compare(lhs.get_space(), rhs.get_space());

    if (super)
    {
        return super;
    }

    if (lhs.subspace < rhs.subspace)
    {
        return -1;
    }
    else if (lhs.subspace > rhs.subspace)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

inline int
compare(const regionid& lhs, const regionid& rhs)
{
    int super = compare(lhs.get_subspace(), rhs.get_subspace());

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
        if (lhs.mask < rhs.mask)
        {
            return -1;
        }
        else if (lhs.mask > rhs.mask)
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }
}

inline int
compare(const entityid& lhs, const entityid& rhs)
{
    int super = compare(lhs.get_region(), rhs.get_region());

    if (super)
    {
        return super;
    }

    if (lhs.number < rhs.number)
    {
        return -1;
    }
    else if (lhs.number > rhs.number)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

// Operators
template <typename T>
bool
operator < (const T& lhs, const T& rhs)
{
    return compare(lhs, rhs) < 0;
}

template <typename T>
bool
operator <= (const T& lhs, const T& rhs)
{
    return compare(lhs, rhs) <= 0;
}

template <typename T>
bool
operator == (const T& lhs, const T& rhs)
{
    return compare(lhs, rhs) == 0;
}

template <typename T>
bool
operator >= (const T& lhs, const T& rhs)
{
    return compare(lhs, rhs) >= 0;
}

template <typename T>
bool
operator > (const T& lhs, const T& rhs)
{
    return compare(lhs, rhs) > 0;
}

template <typename T>
bool
operator != (const T& lhs, const T& rhs)
{
    return compare(lhs, rhs) != 0;
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
