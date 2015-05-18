// Copyright (c) 2013, Cornell University
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
#include "common/ids.h"

#define CREATE_ID(TYPE) \
    std::ostream& \
    operator << (std::ostream& lhs, const TYPE ## _id& rhs) \
    { \
        return lhs << #TYPE "(" << rhs.get() << ")"; \
    } \
    e::packer \
    operator << (e::packer pa, const TYPE ## _id& rhs) \
    { \
        return pa << rhs.get(); \
    } \
    e::unpacker \
    operator >> (e::unpacker up, TYPE ## _id& rhs) \
    { \
        uint64_t id; \
        up = up >> id; \
        rhs = TYPE ## _id(id); \
        return up; \
    }

BEGIN_HYPERDEX_NAMESPACE

CREATE_ID(index)
CREATE_ID(region)
CREATE_ID(replica_set)
CREATE_ID(server)
CREATE_ID(space)
CREATE_ID(subspace)
CREATE_ID(transfer)
CREATE_ID(virtual_server)

END_HYPERDEX_NAMESPACE

#undef CREATE_ID
