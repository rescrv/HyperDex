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

#ifndef hyperdex_daemon_identifier_collector_h_
#define hyperdex_daemon_identifier_collector_h_

// e
#include <e/ao_hash_map.h>
#include <e/compat.h>
#include <e/seqno_collector.h>

// HyperDex
#include "namespace.h"
#include "common/ids.h"

BEGIN_HYPERDEX_NAMESPACE

class identifier_collector
{
    public:
        identifier_collector(e::garbage_collector* gc);
        ~identifier_collector() throw ();

    // concurrent methods
    // these methods return "true" if ri is a managed region_id, else "false"
    public:
        // force the lower bound for "ri" to be "lb" or more
        // equivalent to calling collect on 0 through lb - 1
        void bump(const region_id& ri, uint64_t lb);
        // mark the identifier as collected
        void collect(const region_id& ri, uint64_t id);
        // store a value in "*lb" such that ids "< *lb" have been collected
        uint64_t lower_bound(const region_id& ri);

    // external synchronization required; nothing can call other methods
    public:
        void adopt(region_id* ris, size_t ris_sz);

    private:
        identifier_collector(const identifier_collector&);
        identifier_collector& operator = (const identifier_collector&);
        static uint64_t id(region_id ri) { return ri.get(); }

    private:
        const static region_id defaultri;
        typedef e::ao_hash_map<region_id, e::compat::shared_ptr<e::seqno_collector>, id, defaultri> collector_map_t;
        e::garbage_collector* m_gc;
        collector_map_t m_collectors;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_identifier_collector_h_
