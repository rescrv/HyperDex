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
#include <hyperdex/city.h>
#include <hyperdex/region.h>

hyperdex :: region :: region(const char* file, uint16_t nc)
    : m_ref(0)
    , m_numcolumns(nc)
    , m_lock()
    , m_disk(file)
{
}

hyperdex :: result_t
hyperdex :: region :: get(const e::buffer& key,
                          std::vector<e::buffer>* value,
                          uint64_t* version)
{
    uint64_t key_hash = CityHash64(key);
    return m_disk.get(key, key_hash, value, version);
}

hyperdex :: result_t
hyperdex :: region :: put(const e::buffer& key,
                          const std::vector<e::buffer>& value,
                          uint64_t version)
{
    uint64_t key_hash = CityHash64(key);
    std::vector<uint64_t> value_hashes;

    for (size_t i = 0; i < value.size(); ++i)
    {
        value_hashes.push_back(CityHash64(value[i]));
    }

    po6::threads::mutex::hold hold(&m_lock);
    return m_disk.put(key, key_hash, value, value_hashes, version);
}

hyperdex :: result_t
hyperdex :: region :: del(const e::buffer& key)
{
    uint64_t key_hash = CityHash64(key);
    po6::threads::mutex::hold hold(&m_lock);
    return m_disk.del(key, key_hash);
}
