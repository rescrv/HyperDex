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

#ifndef hyperdisk_shard_vector_h_
#define hyperdisk_shard_vector_h_

// STL
#include <utility>
#include <vector>

// e
#include <e/intrusive_ptr.h>

// HyperspaceHashing
#include "hyperspacehashing/hyperspacehashing/mask.h"

// HyperDisk
#include "hyperdisk/shard.h"

namespace hyperdisk
{

class shard_vector
{
    public:
        shard_vector(const hyperspacehashing::mask::coordinate& coord, e::intrusive_ptr<shard> s);
        shard_vector(uint64_t generation,
                     std::vector<std::pair<hyperspacehashing::mask::coordinate, e::intrusive_ptr<shard> > >* newvec);

    public:
        size_t size() const;
        uint64_t generation() const;

    public:
        const hyperspacehashing::mask::coordinate& get_coordinate(size_t i);
        shard* get_shard(size_t i);
        uint32_t get_offset(size_t i);
        void set_offset(size_t i, uint32_t offset);
        e::intrusive_ptr<shard_vector> replace(size_t shard_num, e::intrusive_ptr<shard> s);
        e::intrusive_ptr<shard_vector> replace(size_t shard_num,
                                               const hyperspacehashing::mask::coordinate& c1, e::intrusive_ptr<shard> s1,
                                               const hyperspacehashing::mask::coordinate& c2, e::intrusive_ptr<shard> s2,
                                               const hyperspacehashing::mask::coordinate& c3, e::intrusive_ptr<shard> s3,
                                               const hyperspacehashing::mask::coordinate& c4, e::intrusive_ptr<shard> s4);

    private:
        friend class e::intrusive_ptr<shard_vector>;

    private:
        ~shard_vector() throw ();

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
        uint64_t m_generation;
        std::vector<std::pair<hyperspacehashing::mask::coordinate, e::intrusive_ptr<shard> > > m_shards;
        std::vector<uint32_t> m_offsets;
};

} // namespace hyperdisk

#endif // hyperdisk_shard_vector_h_
