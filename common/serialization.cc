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
hyperdex :: operator << (e::buffer::packer lhs, const attribute_check& rhs)
{
    return lhs << rhs.attr
               << rhs.value
               << rhs.datatype
               << rhs.pred;
}

e::buffer::unpacker
hyperdex :: operator >> (e::buffer::unpacker lhs, attribute_check& rhs)
{
    return lhs >> rhs.attr
               >> rhs.value
               >> rhs.datatype
               >> rhs.pred;
}

size_t
hyperdex :: pack_size(const attribute_check& rhs)
{
    return sizeof(uint16_t)
         + sizeof(uint32_t)
         + rhs.value.size()
         + pack_size(rhs.datatype)
         + pack_size(rhs.pred);
}

e::buffer::packer
hyperdex :: operator << (e::buffer::packer lhs, const funcall_t& rhs)
{
    uint8_t name = static_cast<uint8_t>(rhs);
    return lhs << name;
}

e::buffer::unpacker
hyperdex :: operator >> (e::buffer::unpacker lhs, funcall_t& rhs)
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

e::buffer::unpacker
hyperdex :: operator >> (e::buffer::unpacker lhs, funcall& rhs)
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

e::buffer::unpacker
hyperdex :: operator >> (e::buffer::unpacker lhs, hyperdatatype& rhs)
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
hyperdex :: operator << (e::buffer::packer lhs, const predicate& rhs)
{
    uint8_t r = static_cast<uint8_t>(rhs);
    return lhs << r;
}

e::buffer::unpacker
hyperdex :: operator >> (e::buffer::unpacker lhs, predicate& rhs)
{
    uint8_t r;
    lhs = lhs >> r;
    rhs = static_cast<predicate>(r);
    return lhs;
}

size_t
hyperdex :: pack_size(const predicate&)
{
    return sizeof(uint8_t);
}
