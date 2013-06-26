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

#ifndef hyperdex_coordinator_missing_acks_h_
#define hyperdex_coordinator_missing_acks_h_

// STL
#include <vector>

// HyperDex
#include "namespace.h"
#include "common/ids.h"

BEGIN_HYPERDEX_NAMESPACE

class missing_acks
{
    public:
        missing_acks(uint64_t version, const std::vector<server_id>& servers);
        missing_acks(const missing_acks&);
        ~missing_acks() throw ();

    public:
        void ack(const server_id& id);
        bool empty() const { return m_servers.empty(); }
        uint64_t version() const { return m_version; }

    private:
        uint64_t m_version;
        std::vector<server_id> m_servers;
};

inline
missing_acks :: missing_acks(uint64_t _version,
                             const std::vector<server_id>& servers)
    : m_version(_version)
    , m_servers(servers)
{
    std::sort(m_servers.begin(), m_servers.end());
    std::vector<server_id>::iterator last;
    last = std::unique(m_servers.begin(), m_servers.end());
    m_servers.resize(last - m_servers.begin());
}

inline
missing_acks :: missing_acks(const missing_acks& other)
    : m_version(other.m_version)
    , m_servers(other.m_servers)
{
}

inline
missing_acks :: ~missing_acks() throw ()
{
}

inline void
missing_acks :: ack(const server_id& id)
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i] == id)
        {
            for (size_t j = i; j + 1 < m_servers.size(); ++j)
            {
                m_servers[j] = m_servers[j + 1];
            }

            m_servers.pop_back();
            break;
        }
        else if (m_servers[i] > id)
        {
            break;
        }
    }
}

inline bool
operator < (const missing_acks& lhs, const missing_acks& rhs)
{
    return lhs.version() < rhs.version();
}

END_HYPERDEX_NAMESPACE

#endif // hyperdex_coordinator_missing_acks_h_
