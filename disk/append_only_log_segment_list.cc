// Copyright (c) 2012, Robert Escriva
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

// e
#include <e/atomic.h>

// append only log
#include "append_only_log_segment_list.h"

using hyperdex::append_only_log;

append_only_log :: segment_list :: segment_list()
    : m_ref(0)
    , m_sz(0)
    , m_segments(NULL)
{
}

append_only_log :: segment_list :: ~segment_list() throw ()
{
}

e::intrusive_ptr<append_only_log::segment_list>
append_only_log :: segment_list :: add(uint64_t lower_bound, writable_segment* ws)
{
    e::intrusive_ptr<segment_list> sl(new segment_list());
    sl->m_sz = m_sz + 1;
    sl->m_segments = new std::pair<uint64_t, e::intrusive_ptr<segment> >[sl->m_sz];

    for (size_t i = 0; i < m_sz; ++i)
    {
        sl->m_segments[i] = m_segments[i];
    }

    sl->m_segments[m_sz] = std::make_pair(lower_bound, ws->get_segment());
    return sl;
}

uint64_t
append_only_log :: segment_list :: get_lower_bound(size_t i)
{
    assert(i < m_sz);
    return m_segments[i].first;
}

append_only_log::segment*
append_only_log :: segment_list :: get_segment(size_t i)
{
    assert(i < m_sz);
    return m_segments[i].second.get();
}

bool
append_only_log :: segment_list :: sync(size_t i)
{
    assert(i < m_sz);
    return m_segments[i].second->sync();
}

void
append_only_log :: segment_list :: inc()
{
    e::atomic::increment_64_nobarrier(&m_ref, 1);
}

void
append_only_log :: segment_list :: dec()
{
    e::atomic::increment_64_nobarrier(&m_ref, -1);

    if (m_ref == 0)
    {
        delete this;
    }
}
