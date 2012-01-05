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

// HyperDisk
#include "hyperdisk/shard_vector.h"

using hyperspacehashing::mask::coordinate;

hyperdisk :: shard_vector :: shard_vector(const coordinate& coord, e::intrusive_ptr<shard> s)
    : m_ref(0)
    , m_generation(1)
    , m_shards(1, std::make_pair(coord, s))
    , m_offsets(1, 0)
{
}

size_t
hyperdisk :: shard_vector :: size() const
{
    assert(m_shards.size() == m_offsets.size());
    return m_shards.size();
}

uint64_t
hyperdisk :: shard_vector :: generation() const
{
    assert(m_shards.size() == m_offsets.size());
    return m_generation;
}

const coordinate&
hyperdisk :: shard_vector :: get_coordinate(size_t i)
{
    assert(i < m_shards.size());
    assert(m_shards.size() == m_offsets.size());
    return m_shards[i].first;
}

hyperdisk::shard*
hyperdisk :: shard_vector :: get_shard(size_t i)
{
    assert(i < m_shards.size());
    assert(m_shards.size() == m_offsets.size());
    return m_shards[i].second.get();
}

uint32_t
hyperdisk :: shard_vector :: get_offset(size_t i)
{
    assert(i < m_offsets.size());
    assert(m_shards.size() == m_offsets.size());
    return m_offsets[i];
}

void
hyperdisk :: shard_vector :: set_offset(size_t i, uint32_t offset)
{
    assert(i < m_offsets.size());
    assert(m_shards.size() == m_offsets.size());
    m_offsets[i] = offset;
}

e::intrusive_ptr<hyperdisk::shard_vector>
hyperdisk :: shard_vector :: replace(size_t shard_num, e::intrusive_ptr<shard> s)
{
    std::vector<std::pair<coordinate, e::intrusive_ptr<shard> > > newvec;
    newvec.reserve(m_shards.size());

    for (size_t i = 0; i < m_shards.size(); ++i)
    {
        if (i == shard_num)
        {
            newvec.push_back(std::make_pair(m_shards[i].first, s));
        }
        else
        {
            newvec.push_back(m_shards[i]);
        }
    }

    e::intrusive_ptr<hyperdisk::shard_vector> ret;
    ret = new shard_vector(m_generation + 1, &newvec);
    return ret;
}

e::intrusive_ptr<hyperdisk::shard_vector>
hyperdisk :: shard_vector :: replace(size_t shard_num,
                                     const coordinate& c1, e::intrusive_ptr<shard> s1,
                                     const coordinate& c2, e::intrusive_ptr<shard> s2,
                                     const coordinate& c3, e::intrusive_ptr<shard> s3,
                                     const coordinate& c4, e::intrusive_ptr<shard> s4)
{
    std::vector<std::pair<coordinate, e::intrusive_ptr<shard> > > newvec;
    newvec.reserve(m_shards.size() + 3);

    for (size_t i = 0; i < m_shards.size(); ++i)
    {
        if (i != shard_num)
        {
            newvec.push_back(m_shards[i]);
        }
    }

    newvec.push_back(std::make_pair(c1, s1));
    newvec.push_back(std::make_pair(c2, s2));
    newvec.push_back(std::make_pair(c3, s3));
    newvec.push_back(std::make_pair(c4, s4));

    e::intrusive_ptr<hyperdisk::shard_vector> ret;
    ret = new shard_vector(m_generation + 1, &newvec);
    return ret;
}

hyperdisk :: shard_vector :: shard_vector(uint64_t gen,
                                          std::vector<std::pair<coordinate, e::intrusive_ptr<shard> > >* newvec)
    : m_ref(0)
    , m_generation(gen)
    , m_shards()
    , m_offsets()
{
    m_shards.swap(*newvec);
    m_offsets.resize(m_shards.size());

    for (size_t i = 0; i < m_shards.size(); ++i)
    {
        m_offsets[i] = m_shards[i].second->m_data_offset;
    }
}

hyperdisk :: shard_vector :: ~shard_vector() throw ()
{
}
