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
#include "common/capture.h"

using hyperdex::capture;

capture :: capture()
    : id()
    , rid()
{
}

capture :: capture(const hyperdex::capture_id& _id,
                   const hyperdex::region_id& _rid)
    : id(_id)
    , rid(_rid)
{
}

capture :: capture(const capture& other)
    : id(other.id)
    , rid(other.rid)
{
}

capture :: ~capture() throw ()
{
}

capture&
capture :: operator = (const capture& rhs)
{
    id = rhs.id;
    rid = rhs.rid;
    return *this;
}

std::ostream&
hyperdex :: operator << (std::ostream& lhs, const capture& rhs)
{
    return lhs << "capture(id=" << rhs.id
               << ", region=" << rhs.rid << ")";
}

e::buffer::packer
hyperdex :: operator << (e::buffer::packer pa, const capture& c)
{
    return pa << c.id.get() << c.rid.get();
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, capture& c)
{
    uint64_t id;
    uint64_t rid;
    up = up >> id >> rid;
    c.id = capture_id(id);
    c.rid = region_id(rid);
    return up;
}

size_t
hyperdex :: pack_size(const capture&)
{
    return sizeof(uint64_t) + sizeof(uint64_t);
}
