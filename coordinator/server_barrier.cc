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
#include "coordinator/server_barrier.h"

using hyperdex::server_barrier;

server_barrier :: server_barrier()
    : m_versions()
{
    m_versions.push_back(std::make_pair(uint64_t(0), std::vector<server_id>()));
}

server_barrier :: ~server_barrier() throw ()
{
}

uint64_t
server_barrier :: min_version() const
{
    return m_versions.front().first;
}

void
server_barrier :: new_version(uint64_t version,
                              const std::vector<server_id>& servers)
{
    assert(!m_versions.empty());
    assert(version > 0);
    assert(m_versions.back().first < version);
    m_versions.push_back(std::make_pair(version, servers));
    maybe_clear_prefix();
}

void
server_barrier :: pass(uint64_t version, const server_id& sid)
{
    for (version_list_t::iterator it = m_versions.begin();
            it != m_versions.end(); ++it)
    {
        if (it->first < version)
        {
            continue;
        }
        else if (it->first > version)
        {
            break;
        }

        assert(it->first == version);

        for (size_t i = 0; i < it->second.size(); )
        {
            if (it->second[i] == sid)
            {
                it->second[i] = it->second.back();
                it->second.pop_back();
            }
            else
            {
                ++i;
            }
        }

        maybe_clear_prefix();
        return;
    }
}

void
server_barrier :: maybe_clear_prefix()
{
    version_list_t::iterator last_empty = m_versions.end();

    for (version_list_t::iterator it = m_versions.begin();
            it != m_versions.end(); ++it)
    {
        if (it->second.empty())
        {
            last_empty = it;
        }
    }

    if (last_empty != m_versions.end())
    {
        while (m_versions.begin() != last_empty)
        {
            m_versions.pop_front();
        }
    }
}

size_t
hyperdex :: pack_size(const server_barrier& ri)
{
    typedef server_barrier::version_list_t version_list_t;
    size_t sz = sizeof(uint32_t);

    for (version_list_t::const_iterator it = ri.m_versions.begin();
            it != ri.m_versions.end(); ++it)
    {
        sz += sizeof(uint64_t) + pack_size(it->second);
    }

    return sz;
}

e::packer
hyperdex :: operator << (e::packer pa, const server_barrier& ri)
{
    typedef server_barrier::version_list_t version_list_t;
    uint32_t x = ri.m_versions.size();
    pa = pa << x;

    for (version_list_t::const_iterator it = ri.m_versions.begin();
            it != ri.m_versions.end(); ++it)
    {
        pa = pa << it->first << it->second;
    }

    return pa;
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, server_barrier& ri)
{
    typedef server_barrier::version_t version_t;
    uint32_t x = 0;
    up = up >> x;

    for (size_t i = 0; i < x && !up.error(); ++i)
    {
        ri.m_versions.push_back(version_t());
        version_t& v(ri.m_versions.back());
        up = up >> v.first >> v.second;
    }

    return up;
}
