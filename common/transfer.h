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

#ifndef hyperdex_common_transfer_h_
#define hyperdex_common_transfer_h_

// e
#include <e/buffer.h>

// HyperDex
#include "namespace.h"
#include "common/ids.h"

BEGIN_HYPERDEX_NAMESPACE

class transfer
{
    public:
        transfer();
        transfer(const transfer_id& id,
                 const region_id& rid,
                 const server_id& src,
                 const virtual_server_id& vsrc,
                 const server_id& dst,
                 const virtual_server_id& vdst);
        transfer(const transfer&);
        ~transfer() throw ();

    public:
        transfer& operator = (const transfer&);
        bool operator < (const transfer&) const;

    public:
        transfer_id id;
        region_id rid;
        server_id src;
        virtual_server_id vsrc;
        server_id dst;
        virtual_server_id vdst;
};

std::ostream&
operator << (std::ostream& lhs, const transfer& rhs);

e::buffer::packer
operator << (e::buffer::packer, const transfer& t);
e::unpacker
operator >> (e::unpacker, transfer& t);
size_t
pack_size(const transfer& t);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_transfer_h_
