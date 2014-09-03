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
#include <map>
#include <set>
#include <tr1/random>

// HyperDex
#include "coordinator/replica_sets.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wlarger-than="

using hyperdex::replica_set;
using hyperdex::server;
using hyperdex::server_id;

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

namespace
{

struct permute_generator
{
    permute_generator() : m_ung() { m_ung.seed(0xdeadbeef); }
    size_t operator () (const size_t& x) { return m_ung() % x; }
    private:
        std::tr1::mt19937 m_ung;
};

struct scatter_width_comparator
{
    scatter_width_comparator(std::map<server_id, uint64_t>* scatter_widths)
        : m_scatter_widths(scatter_widths) {}
    scatter_width_comparator(scatter_width_comparator& other)
        : m_scatter_widths(other.m_scatter_widths) {}
    scatter_width_comparator& operator = (scatter_width_comparator&);
    bool operator () (const server_id& lhs, const server_id& rhs)
    {
        return (*m_scatter_widths)[lhs] < (*m_scatter_widths)[rhs];
    }
    private:
        std::map<server_id, uint64_t>* m_scatter_widths;
};

std::pair<server_id, server_id>
collocated_pair(const server_id& a, const server_id& b)
{
    if (a < b)
    {
        return std::make_pair(a, b);
    }
    else
    {
        return std::make_pair(b, a);
    }
}

} // namespace

void
hyperdex :: compute_replica_sets(uint64_t S,
                                 const std::vector<server>& _servers,
                                 std::vector<server_id>* replica_storage,
                                 std::vector<replica_set>* replica_sets)
{
    replica_storage->clear();
    replica_sets->clear();

    // Find the set of available servers to pick from
    std::vector<server_id> servers;
    std::map<server_id, unsigned> colors;

    for (size_t i = 0; i < _servers.size(); ++i)
    {
        if (_servers[i].state == server::AVAILABLE)
        {
            servers.push_back(_servers[i].id);
            colors[_servers[i].id] = _servers[i].color;
        }
    }

    std::sort(servers.begin(), servers.end());
    std::vector<server_id> permutation(servers);
    size_t progress = replica_sets->size() + 1;
    std::map<server_id, uint64_t> scatter_widths;
    std::set<std::pair<server_id, server_id> > collocated;
    permute_generator pg;
    scatter_width_comparator swc(&scatter_widths);

    for (size_t i = 0; i < servers.size(); ++i)
    {
        collocated.insert(collocated_pair(servers[i], servers[i]));
    }

    // until we stop creating replica sets
    while (progress != replica_sets->size())
    {
        progress = replica_sets->size();

        // for each server
        for (size_t i = 0; i < servers.size(); ++i)
        {
            // scatter width sufficient
            if (scatter_widths[servers[i]] >= S)
            {
                continue;
            }

            // try to increase the scatter width
            // do a random permutation and stable sort according to sw
            std::random_shuffle(permutation.begin(), permutation.end(), pg);
            std::stable_sort(permutation.begin(), permutation.end(), swc);

            // now get the list of servers that are candidates for this one
            std::vector<server_id> candidates;

            for (size_t j = 0; j < permutation.size(); ++j)
            {
                if (collocated.find(collocated_pair(servers[i], permutation[j])) != collocated.end())
                {
                    continue;
                }

                bool no_conflicts = true;

                for (size_t k = 0; k < candidates.size(); ++k)
                {
                    if (collocated.find(collocated_pair(candidates[k], permutation[j])) != collocated.end())
                    {
                        no_conflicts = false;
                    }
                }

                if (no_conflicts)
                {
                    candidates.push_back(permutation[j]);
                }
            }

            if (candidates.size() < 3)
            {
                continue;
            }

            size_t start = replica_storage->size();
            replica_sets->push_back(replica_set(start, 3, replica_storage));
            size_t color0 = 0;
            size_t color1 = 0;

            for (size_t j = 0; j < candidates.size(); ++j)
            {
                if (colors[candidates[j]] == 0)
                {
                    ++color0;
                }
                if (colors[candidates[j]] == 1)
                {
                    ++color1;
                }
            }

            if (color0 >= 2 && color1 >= 1)
            {
                for (size_t j = 0; j < candidates.size() &&
                        start + 2 > replica_storage->size(); ++j)
                {
                    if (colors[candidates[j]] == 0)
                    {
                        replica_storage->push_back(candidates[j]);
                    }
                }

                for (size_t j = 0; j < candidates.size() &&
                        start + 3 > replica_storage->size(); ++j)
                {
                    if (colors[candidates[j]] == 1)
                    {
                        replica_storage->push_back(candidates[j]);
                    }
                }
            }
            else
            {
                for (size_t j = 0; j < candidates.size() &&
                        start + 3 > replica_storage->size(); ++j)
                {
                    replica_storage->push_back(candidates[j]);
                }
            }

            assert(replica_storage->size() == start + 3);
            collocated.insert(collocated_pair((*replica_storage)[start + 0], (*replica_storage)[start + 1]));
            collocated.insert(collocated_pair((*replica_storage)[start + 0], (*replica_storage)[start + 2]));
            collocated.insert(collocated_pair((*replica_storage)[start + 1], (*replica_storage)[start + 2]));
        }
    }
}

#pragma GCC diagnostic pop
