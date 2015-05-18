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

#ifndef hyperdex_coordinator_offline_server_h_
#define hyperdex_coordinator_offline_server_h_

// HyperDex
#include "namespace.h"

BEGIN_HYPERDEX_NAMESPACE

class offline_server
{
    public:
        offline_server();
        offline_server(const region_id& id, const server_id& sid);
        offline_server(const offline_server& other);

    public:
        region_id id;
        server_id sid;
};

inline size_t
pack_size(const offline_server& os)
{
    return pack_size(os.id) + pack_size(os.sid);
}

inline e::packer
operator << (e::packer pa, const offline_server& os)
{
    return pa << os.id << os.sid;
}

inline e::unpacker
operator >> (e::unpacker up, offline_server& os)
{
    return up >> os.id >> os.sid;
}

inline
offline_server :: offline_server()
    : id()
    , sid()
{
}

inline
offline_server :: offline_server(const region_id& _id, const server_id& _sid)
    : id(_id)
    , sid(_sid)
{
}

inline
offline_server :: offline_server(const offline_server& other)
    : id(other.id)
    , sid(other.sid)
{
}

END_HYPERDEX_NAMESPACE

#endif // hyperdex_coordinator_offline_server_h_
