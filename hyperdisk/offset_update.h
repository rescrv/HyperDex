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

#ifndef hyperdisk_offset_update_h_
#define hyperdisk_offset_update_h_

// e
#include <e/tuple_compare.h>

namespace hyperdisk
{

class offset_update
{
    public:
        offset_update();
        offset_update(uint64_t shard_generation,
                      size_t shard_num,
                      uint32_t new_offset);
        ~offset_update() throw ();

    public:
        bool operator == (const offset_update& rhs) const;

    public:
        uint64_t shard_generation;
        size_t shard_num;
        uint32_t new_offset;
};

inline
offset_update :: offset_update()
    : shard_generation(0)
    , shard_num(0)
    , new_offset(0)
{
}

inline
offset_update :: offset_update(uint64_t sg, size_t sn, uint32_t no)
    : shard_generation(sg)
    , shard_num(sn)
    , new_offset(no)
{
}

inline
offset_update :: ~offset_update() throw ()
{
}

bool
offset_update :: operator == (const offset_update& rhs) const
{
    return e::tuple_compare(shard_generation, shard_num, new_offset,
                            rhs.shard_generation, rhs.shard_num, rhs.new_offset) == 0;
}

} // namespace hyperdisk

#endif // hyperdisk_offset_update_h_
