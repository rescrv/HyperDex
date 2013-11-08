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

#ifndef hyperdex_coordinator_replica_sets_h_
#define hyperdex_coordinator_replica_sets_h_

// HyperDex
#include "namespace.h"
#include "common/ids.h"
#include "common/server.h"

BEGIN_HYPERDEX_NAMESPACE

class replica_set
{
    public:
        replica_set();
        replica_set(size_t start, size_t size,
                    std::vector<server_id>* storage);
        replica_set(const replica_set&);

    public:
        size_t size() const;

    public:
        server_id operator [] (size_t idx) const;
        replica_set& operator = (const replica_set&);

    private:
        size_t m_start;
        size_t m_size;
        std::vector<server_id>* m_storage;
};

void
compute_replica_sets(uint64_t R, uint64_t P,
                     const std::vector<server_id>& permutation,
                     const std::vector<server>& servers,
                     std::vector<server_id>* replica_storage,
                     std::vector<replica_set>* replica_sets);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_coordinator_replica_sets_h_
