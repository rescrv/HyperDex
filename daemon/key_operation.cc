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

#define __STDC_LIMIT_MACROS

// Google Log
#include <glog/logging.h>

// HyperDex
#include "daemon/key_operation.h"

using hyperdex::key_operation;

key_operation :: key_operation(uint64_t old_version,
                               uint64_t new_version,
                               bool _fresh,
                               bool _has_value,
                               const std::vector<e::slice>& _value,
                               std::auto_ptr<e::arena> memory)
    : m_ref(0)
    , m_prev_version(old_version)
    , m_this_version(new_version)
    , m_acked(false)
    , m_fresh(_fresh)
    , m_has_value(_has_value)
    , m_recv_config_version()
    , m_recv()
    , m_sent_config_version()
    , m_sent()
    , m_value(_value)
    , m_memory(memory)
    , m_type(UNKNOWN)
    , m_this_old_region()
    , m_this_new_region()
    , m_prev_region()
    , m_next_region()
{
}

key_operation :: ~key_operation() throw ()
{
}

void
key_operation :: set_continuous()
{
    assert(m_type == UNKNOWN);
    m_type = CONTINUOUS;
}

void
key_operation :: set_continuous_hashes(const region_id& pr,
                                       const region_id& tor,
                                       const region_id& tnr,
                                       const region_id& nr)
{
    assert(m_type == CONTINUOUS);
    assert(m_prev_region == region_id());
    assert(m_this_old_region == region_id());
    assert(m_this_new_region == region_id());
    assert(m_next_region == region_id());

    m_prev_region = pr;
    m_this_old_region = tor;
    m_this_new_region = tnr;
    m_next_region = nr;
}

void
key_operation :: set_discontinuous(const region_id& pr,
                                   const region_id& tor,
                                   const region_id& tnr,
                                   const region_id& nr)
{
    assert(m_type == UNKNOWN);
    assert(m_prev_region == region_id());
    assert(m_this_old_region == region_id());
    assert(m_this_new_region == region_id());
    assert(m_next_region == region_id());
    m_type = DISCONTINUOUS;

    m_prev_region = pr;
    m_this_old_region = tor;
    m_this_new_region = tnr;
    m_next_region = nr;
}

void
key_operation :: debug_dump()
{
    LOG(INFO) << "    unique op id: prev=" << m_prev_version << " this=" << m_this_version;
    LOG(INFO) << "    has value: " << (m_has_value ? "yes" : "no");
    LOG(INFO) << "    recv: version=" << m_recv_config_version << " from=" << m_recv;
    LOG(INFO) << "    sent: version=" << m_sent_config_version << " to=" << m_sent;
    LOG(INFO) << "    fresh: " << (m_fresh ? "yes" : "no");
    LOG(INFO) << "    acked: " << (m_acked ? "yes" : "no");
    LOG(INFO) << "    prev: " << m_prev_region;
    LOG(INFO) << "    this_old: " << m_this_old_region;
    LOG(INFO) << "    this_new: " << m_this_new_region;
    LOG(INFO) << "    next: " << m_next_region;
}
