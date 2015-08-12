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

#ifndef hyperdex_coordinator_server_barrier_h_
#define hyperdex_coordinator_server_barrier_h_

// STL
#include <list>
#include <utility>
#include <vector>

// HyperDex
#include "namespace.h"
#include "common/ids.h"
#include "common/serialization.h"

BEGIN_HYPERDEX_NAMESPACE

class server_barrier
{
    public:
        server_barrier();
        ~server_barrier() throw ();

    public:
        uint64_t min_version() const;
        void new_version(uint64_t version,
                         const std::vector<server_id>& servers);
        void pass(uint64_t version, const server_id& sid);

    private:
        void maybe_clear_prefix();

    private:
        friend size_t pack_size(const server_barrier& ri);
        friend e::packer operator << (e::packer pa, const server_barrier& ri);
        friend e::unpacker operator >> (e::unpacker up, server_barrier& ri);
        typedef std::pair<uint64_t, std::vector<server_id> > version_t;
        typedef std::list<version_t> version_list_t;
        version_list_t m_versions;
};

size_t
pack_size(const server_barrier& ri);
e::packer
operator << (e::packer pa, const server_barrier& ri);
e::unpacker
operator >> (e::unpacker up, server_barrier& ri);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_coordinator_server_barrier_h_
