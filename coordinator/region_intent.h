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

#ifndef hyperdex_coordinator_region_intent_h_
#define hyperdex_coordinator_region_intent_h_

// HyperDex
#include "namespace.h"

BEGIN_HYPERDEX_NAMESPACE

class region_intent
{
    public:
        region_intent();
        region_intent(const region_id& id);
        region_intent(const region_intent&);

    public:
        region_intent& operator = (const region_intent&);

    public:
        region_id id;
        std::vector<server_id> replicas;
        uint64_t checkpoint;
};

inline size_t
pack_size(const region_intent& ri)
{
    return pack_size(ri.id) + pack_size(ri.replicas) + sizeof(ri.checkpoint);
}

inline e::packer
operator << (e::packer pa, const region_intent& ri)
{
    return pa << ri.id << ri.replicas << ri.checkpoint;
}

inline e::unpacker
operator >> (e::unpacker up, region_intent& ri)
{
    return up >> ri.id >> ri.replicas >> ri.checkpoint;
}

inline
region_intent :: region_intent()
    : id()
    , replicas()
    , checkpoint(0)
{
}

inline
region_intent :: region_intent(const region_id& ri)
    : id(ri)
    , replicas()
    , checkpoint(0)
{
}

inline
region_intent :: region_intent(const region_intent& other)
    : id(other.id)
    , replicas(other.replicas)
    , checkpoint(other.checkpoint)
{
}

inline region_intent&
region_intent :: operator = (const region_intent& rhs)
{
    if (this != &rhs)
    {
        id = rhs.id;
        replicas = rhs.replicas;
        checkpoint = rhs.checkpoint;
    }

    return *this;
}

END_HYPERDEX_NAMESPACE

#endif // hyperdex_coordinator_region_intent_h_
