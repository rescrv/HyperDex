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

// HyperDex
#include "common/serialization.h"

e::buffer::packer
hyperdex :: operator << (e::buffer::packer lhs, const po6::net::ipaddr& rhs)
{
    assert(rhs.family() == AF_INET || rhs.family() == AF_INET6 || rhs.family() == AF_UNSPEC);
    uint8_t type;
    uint8_t data[16];
    memset(data, 0, 16);

    if (rhs.family() == AF_INET)
    {
        type = 4;
        sockaddr_in sa;
        rhs.pack(&sa, 0);
        memmove(data, &sa.sin_addr.s_addr, 4);
    }
    else if (rhs.family() == AF_INET6)
    {
        type = 6;
        sockaddr_in6 sa;
        rhs.pack(&sa, 0);
#ifdef _MSC_VER
        memmove(data, &sa.sin6_addr.u.Byte, 16);
#elif defined __APPLE__
        memmove(data, &sa.sin6_addr.__u6_addr.__u6_addr8, 16);
#else
        memmove(data, &sa.sin6_addr.__in6_u.__u6_addr8, 16);
#endif

    }
    else
    {
        type = 0;
    }

    lhs = lhs << type;
    return lhs.copy(e::slice(data, 16));
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, po6::net::ipaddr& rhs)
{
    uint8_t type;
    lhs = lhs >> type;

    if (lhs.remain() < 16)
    {
        return lhs.as_error();
    }

    e::slice rem = lhs.as_slice();

    if (type == 4)
    {
        in_addr ia;
        memmove(&ia.s_addr, rem.data(), 4);
        rhs = po6::net::ipaddr(ia);
        return lhs.advance(16);
    }
    else if (type == 6)
    {
        in6_addr ia;
#ifdef _MSC_VER
        memmove(ia.u.Byte, rem.data(), 16);
#elif defined __APPLE__
        memmove(ia.__u6_addr.__u6_addr8, rem.data(), 16);
#else
        memmove(ia.__in6_u.__u6_addr8, rem.data(), 16);
#endif
        rhs = po6::net::ipaddr(ia);
        return lhs.advance(16);
    }
    else if (type == 0)
    {
        return lhs.advance(16);
    }
    else
    {
        return lhs.as_error();
    }
}

size_t
hyperdex :: pack_size(const po6::net::ipaddr&)
{
    return 17; // One byte for family, and 4/16 for address
}

e::buffer::packer
hyperdex :: operator << (e::buffer::packer lhs, const po6::net::location& rhs)
{
    return lhs << rhs.address << rhs.port;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, po6::net::location& rhs)
{
    return lhs >> rhs.address >> rhs.port;
}

size_t
hyperdex :: pack_size(const po6::net::location& rhs)
{
    return pack_size(rhs.address) + sizeof(uint16_t);
}

e::buffer::packer
hyperdex :: operator << (e::buffer::packer lhs, const po6::net::hostname& rhs)
{
    return lhs << e::slice(rhs.address.data(), rhs.address.size()) << rhs.port;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, po6::net::hostname& rhs)
{
    e::slice address;
    lhs = lhs >> address >> rhs.port;
    rhs.address = std::string(reinterpret_cast<const char*>(address.data()), address.size());
    return lhs;
}

size_t
hyperdex :: pack_size(const po6::net::hostname& rhs)
{
    return sizeof(uint32_t) + rhs.address.size() + sizeof(uint16_t);
}

e::buffer::packer
hyperdex :: operator << (e::buffer::packer lhs, const attribute_check& rhs)
{
    return lhs << rhs.attr
               << rhs.value
               << rhs.datatype
               << rhs.predicate;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, attribute_check& rhs)
{
    return lhs >> rhs.attr
               >> rhs.value
               >> rhs.datatype
               >> rhs.predicate;
}

size_t
hyperdex :: pack_size(const attribute_check& rhs)
{
    return sizeof(uint16_t)
         + sizeof(uint32_t)
         + rhs.value.size()
         + pack_size(rhs.datatype)
         + pack_size(rhs.predicate);
}

e::buffer::packer
hyperdex :: operator << (e::buffer::packer lhs, const funcall_t& rhs)
{
    uint8_t name = static_cast<uint8_t>(rhs);
    return lhs << name;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, funcall_t& rhs)
{
    uint8_t name;
    lhs = lhs >> name;
    rhs = static_cast<funcall_t>(name);
    return lhs;
}

size_t
hyperdex :: pack_size(const funcall_t&)
{
    return sizeof(uint8_t);
}

e::buffer::packer
hyperdex :: operator << (e::buffer::packer lhs, const funcall& rhs)
{
    return lhs << rhs.attr << rhs.name
               << rhs.arg1 << rhs.arg1_datatype
               << rhs.arg2 << rhs.arg2_datatype;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, funcall& rhs)
{
    return lhs >> rhs.attr >> rhs.name
               >> rhs.arg1 >> rhs.arg1_datatype
               >> rhs.arg2 >> rhs.arg2_datatype;
}

size_t
hyperdex :: pack_size(const funcall& m)
{
    return sizeof(uint16_t) + pack_size(m.name)
         + sizeof(uint32_t) + m.arg1.size() + pack_size(m.arg1_datatype)
         + sizeof(uint32_t) + m.arg2.size() + pack_size(m.arg2_datatype);
}

e::buffer::packer
hyperdex :: operator << (e::buffer::packer lhs, const hyperdatatype& rhs)
{
    uint16_t r = static_cast<uint16_t>(rhs);
    return lhs << r;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, hyperdatatype& rhs)
{
    uint16_t r;
    lhs = lhs >> r;
    rhs = static_cast<hyperdatatype>(r);
    return lhs;
}

size_t
hyperdex :: pack_size(const hyperdatatype&)
{
    return sizeof(uint16_t);
}

e::buffer::packer
hyperdex :: operator << (e::buffer::packer lhs, const hyperpredicate& rhs)
{
    uint16_t r = static_cast<uint16_t>(rhs);
    return lhs << r;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, hyperpredicate& rhs)
{
    uint16_t r;
    lhs = lhs >> r;
    rhs = static_cast<hyperpredicate>(r);
    return lhs;
}

size_t
hyperdex :: pack_size(const hyperpredicate&)
{
    return sizeof(uint16_t);
}

size_t
hyperdex :: pack_size(const e::slice& s)
{
    return sizeof(uint32_t) + s.size();
}
