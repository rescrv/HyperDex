// Copyright (c) 2013-2014, Cornell University
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

// STL
#include <algorithm>

// e
#include <e/atomic.h>

// HyperDex
#include "daemon/identifier_collector.h"

using hyperdex::identifier_collector;
using hyperdex::region_id;

const region_id identifier_collector::defaultri;

identifier_collector :: identifier_collector(e::garbage_collector* gc)
    : m_gc(gc)
    , m_collectors()
{
}

identifier_collector :: ~identifier_collector() throw ()
{
}

void
identifier_collector :: bump(const region_id& ri, uint64_t lb)
{
    e::compat::shared_ptr<e::seqno_collector> sc;

    if (!m_collectors.get(ri, &sc))
    {
        abort();
    }

    sc->collect_up_to(lb);
}

void
identifier_collector :: collect(const region_id& ri, uint64_t seqno)
{
    e::compat::shared_ptr<e::seqno_collector> sc;

    if (!m_collectors.get(ri, &sc))
    {
        abort();
    }

    sc->collect(seqno);
}

uint64_t
identifier_collector :: lower_bound(const region_id& ri)
{
    e::compat::shared_ptr<e::seqno_collector> sc;

    if (!m_collectors.get(ri, &sc))
    {
        abort();
    }

    uint64_t lb;
    sc->lower_bound(&lb);
    return lb;
}

void
identifier_collector :: adopt(region_id* ris, size_t ris_sz)
{
    collector_map_t new_collectors;

    for (size_t i = 0; i < ris_sz; ++i)
    {
        e::compat::shared_ptr<e::seqno_collector> sc;

        if (!m_collectors.get(ris[i], &sc))
        {
            sc = e::compat::shared_ptr<e::seqno_collector>(new e::seqno_collector(m_gc));
            sc->collect(0);
        }

        assert(sc);
        new_collectors.put(ris[i], sc);
    }

    m_collectors.swap(&new_collectors);
}
