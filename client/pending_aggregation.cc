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
#include "client/pending_aggregation.h"

using hyperdex::pending_aggregation;

pending_aggregation :: pending_aggregation(uint64_t id,
                                           hyperclient_returncode* status)
    : pending(id, status)
    , m_outstanding()
{
}

pending_aggregation :: ~pending_aggregation() throw ()
{
}

bool
pending_aggregation :: aggregation_done()
{
    return m_outstanding.empty();
}

void
pending_aggregation :: handle_sent_to(const server_id& si,
                                      const virtual_server_id& vsi)
{
    m_outstanding.push_back(std::make_pair(si, vsi));
}

void
pending_aggregation :: handle_failure(const server_id& si,
                                      const virtual_server_id& vsi)
{
    remove(si, vsi);
}

bool
pending_aggregation :: handle_message(client*,
                                      const server_id& si,
                                      const virtual_server_id& vsi,
                                      network_msgtype,
                                      std::auto_ptr<e::buffer>,
                                      e::unpacker,
                                      hyperclient_returncode*)
{
    remove(si, vsi);
    return true;
}

void
pending_aggregation :: remove(const server_id& si,
                              const virtual_server_id& vsi)
{
    for (size_t i = 0; i < m_outstanding.size(); ++i)
    {
        if (m_outstanding[i].first != si ||
            m_outstanding[i].second != vsi)
        {
            continue;
        }

        for (size_t j = i; j + 1 < m_outstanding.size(); ++j)
        {
            m_outstanding[j] = m_outstanding[j + 1];
        }

        m_outstanding.pop_back();
        return;
    }
}
