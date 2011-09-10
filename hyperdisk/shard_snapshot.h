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

#ifndef hyperdisk_shard_snapshot_h_
#define hyperdisk_shard_snapshot_h_

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>

// HyperDisk
#include "hyperdisk/returncode.h"
#include "shard_constants.h"

// Forward Declarations
namespace hyperdisk
{
class shard;
}

namespace hyperdisk
{

class shard_snapshot
{
    public:
        bool valid();
        void next();

    public:
        uint32_t primary_hash();
        uint32_t secondary_hash();
        hyperspacehashing::mask::coordinate coordinate();
        uint64_t version();
        e::buffer key();
        std::vector<e::buffer> value();

    private:
        friend class e::intrusive_ptr<shard_snapshot>;
        friend class shard;

    private:
        shard_snapshot(e::intrusive_ptr<shard> d);
        ~shard_snapshot() throw ();

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
        bool m_valid;
        e::intrusive_ptr<shard> m_shard;
        const uint32_t m_limit;
        uint32_t m_entry;
};

} // namespace hyperdisk

#endif // hyperdisk_shard_snapshot_h_
