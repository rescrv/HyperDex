// Copyright (c) 2011, Cornell University
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
#include <hyperdex/shard.h>

hyperdex :: shard :: shard()
    : m_lock()
    , m_map()
{
}

hyperdex :: shard :: ~shard()
{
}

bool
hyperdex :: shard :: get(const std::vector<char>& key,
                         std::vector<std::vector<char> >* value)
{
    m_lock.rdlock();
    map_t::iterator i = m_map.find(key);

    if (i == m_map.end())
    {
        m_lock.unlock();
        return false;
    }

    *value = i->second;
    m_lock.unlock();
    return true;
}

bool
hyperdex :: shard :: put(const std::vector<char>& key,
                         const std::vector<std::vector<char> >& value)
{
    m_lock.wrlock();
    m_map[key] = value;
    m_lock.unlock();
    return true;
}

bool
hyperdex :: shard :: del(const std::vector<char>& key)
{
    m_lock.wrlock();
    bool ret = m_map.erase(key) > 0;
    m_lock.unlock();
    return ret;
}
