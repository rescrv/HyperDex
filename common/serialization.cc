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

e::packer
hyperdex :: operator << (e::packer lhs, const hyperdatatype& rhs)
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

e::packer
hyperdex :: operator << (e::packer lhs, const hyperpredicate& rhs)
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
