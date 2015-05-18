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
#include "common/transfer.h"

using hyperdex::transfer;

transfer :: transfer()
    : id()
    , rid()
    , src()
    , vsrc()
    , dst()
    , vdst()
{
}

transfer :: transfer(const hyperdex::transfer_id& _id,
                     const hyperdex::region_id& _rid,
                     const hyperdex::server_id& _src,
                     const hyperdex::virtual_server_id& _vsrc,
                     const hyperdex::server_id& _dst,
                     const hyperdex::virtual_server_id& _vdst)
    : id(_id)
    , rid(_rid)
    , src(_src)
    , vsrc(_vsrc)
    , dst(_dst)
    , vdst(_vdst)
{
}

transfer :: transfer(const transfer& other)
    : id(other.id)
    , rid(other.rid)
    , src(other.src)
    , vsrc(other.vsrc)
    , dst(other.dst)
    , vdst(other.vdst)
{
}

transfer :: ~transfer() throw ()
{
}

transfer&
transfer :: operator = (const transfer& rhs)
{
    id = rhs.id;
    rid = rhs.rid;
    src = rhs.src;
    vsrc = rhs.vsrc;
    dst = rhs.dst;
    vdst = rhs.vdst;
    return *this;
}

bool
transfer :: operator < (const transfer& rhs) const
{
    if (id < rhs.id)
    {
        return true;
    }
    else if (id > rhs.id)
    {
        return false;
    }

    if (rid < rhs.rid)
    {
        return true;
    }
    else if (rid > rhs.rid)
    {
        return false;
    }

    if (src < rhs.src)
    {
        return true;
    }
    else if (src > rhs.src)
    {
        return false;
    }

    if (vsrc < rhs.vsrc)
    {
        return true;
    }
    else if (vsrc > rhs.vsrc)
    {
        return false;
    }

    if (dst < rhs.dst)
    {
        return true;
    }
    else if (dst > rhs.dst)
    {
        return false;
    }

    return vdst < rhs.vdst;
}

std::ostream&
hyperdex :: operator << (std::ostream& lhs, const transfer& rhs)
{
    return lhs << "transfer(id=" << rhs.id
               << ", region=" << rhs.rid
               << ", src=" << rhs.src
               << ", vsrc=" << rhs.vsrc
               << ", dst=" << rhs.dst
               << ", vdst=" << rhs.vdst << ")";
}

e::packer
hyperdex :: operator << (e::packer pa, const transfer& t)
{
    return pa << t.id << t.rid << t.src << t.vsrc << t.dst << t.vdst;
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, transfer& t)
{
    return up >> t.id >> t.rid >> t.src >> t.vsrc >> t.dst >> t.vdst;
}

size_t
hyperdex :: pack_size(const transfer&)
{
    return 6 * sizeof(uint64_t);
}
