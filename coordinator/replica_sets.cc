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

// STL
#include <algorithm>

// HyperDex
#include "coordinator/replica_sets.h"

using hyperdex::replica_set;
using hyperdex::server;
using hyperdex::server_id;

namespace
{

void
_small_replica_sets(uint64_t, uint64_t,
                    const std::vector<server_id>& permutation,
                    const std::vector<server_id>& servers,
                    std::vector<server_id>* replica_storage,
                    std::vector<replica_set>* replica_sets)
{
    for (size_t i = 0; i < permutation.size(); ++i)
    {
        size_t repls = 0;
        size_t start = replica_storage->size();

        for (size_t j = 0; j < permutation.size(); ++j)
        {
            size_t idx = (i + j) % permutation.size();

            if (std::binary_search(servers.begin(),
                                   servers.end(),
                                   permutation[idx]))
            {
                replica_storage->push_back(permutation[idx]);
                ++repls;
            }
        }

        replica_sets->push_back(replica_set(start, repls, replica_storage));
    }
}

void
_permutation_replica_sets(uint64_t R, uint64_t P,
                          const std::vector<server_id>& permutation,
                          const std::vector<server_id>& servers,
                          std::vector<server_id>* replica_storage,
                          std::vector<replica_set>* replica_sets)
{
    for (size_t n = 0; n < permutation.size(); ++n)
    {
        for (size_t p = 1; p <= P; ++p)
        {
            if (n + p * (R - 1) >= permutation.size())
            {
                break;
            }

            size_t repls = 0;
            size_t start = replica_storage->size();

            for (size_t r = 0; r < R; ++r)
            {
                size_t idx = n + p * r;

                if (std::binary_search(servers.begin(),
                                       servers.end(),
                                       permutation[idx]))
                {
                    replica_storage->push_back(permutation[idx]);
                    ++repls;
                }
            }

            replica_sets->push_back(replica_set(start, repls, replica_storage));
        }
    }
}

} // namespace

replica_set :: replica_set()
    : m_start(0)
    , m_size(0)
    , m_storage(NULL)
{
}

replica_set :: replica_set(size_t start, size_t sz,
                           std::vector<server_id>* storage)
    : m_start(start)
    , m_size(sz)
    , m_storage(storage)
{
}

replica_set :: replica_set(const replica_set& other)
    : m_start(other.m_start)
    , m_size(other.m_size)
    , m_storage(other.m_storage)
{
}

size_t
replica_set :: size() const
{
    return m_size;
}

server_id
replica_set :: operator [] (size_t idx) const
{
    assert(idx < m_size);
    return (*m_storage)[m_start + idx];
}

replica_set&
replica_set :: operator = (const replica_set& rhs)
{
    m_start = rhs.m_start;
    m_size = rhs.m_size;
    m_storage = rhs.m_storage;
    return *this;
}

void
hyperdex :: compute_replica_sets(uint64_t R, uint64_t P,
                                 const std::vector<server_id>& permutation,
                                 const std::vector<server>& _servers,
                                 std::vector<server_id>* replica_storage,
                                 std::vector<replica_set>* replica_sets)
{
    assert(R > 0);
    replica_storage->clear();
    replica_sets->clear();

    std::vector<server_id> servers;

    for (size_t i = 0; i < _servers.size(); ++i)
    {
        if (_servers[i].state == server::AVAILABLE)
        {
            servers.push_back(_servers[i].id);
        }
    }

    std::sort(servers.begin(), servers.end());

    if (permutation.size() <= R)
    {
        _small_replica_sets(R, P, permutation, servers, replica_storage, replica_sets);
    }
    else
    {
        _permutation_replica_sets(R, P, permutation, servers, replica_storage, replica_sets);
    }
}
