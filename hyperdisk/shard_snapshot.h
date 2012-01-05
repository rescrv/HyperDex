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
#include <e/intrusive_ptr.h>
#include <e/slice.h>

// HyperDisk
#include "hyperdisk/hyperdisk/returncode.h"
#include "hyperdisk/shard_constants.h"

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
        shard_snapshot(uint32_t offset, shard* s);
        shard_snapshot(const shard_snapshot& other);
        ~shard_snapshot() throw ();

    public:
        bool valid();
        bool valid(const hyperspacehashing::mask::coordinate& coord);
        void next();

    public:
        hyperspacehashing::mask::coordinate coordinate() { return m_coord; }
        uint64_t version();
        const e::slice& key();
        const std::vector<e::slice>& value();

    public:
        shard_snapshot& operator = (const shard_snapshot& rhs);

    private:
        void parse();

    private:
        shard* m_shard;
        uint32_t m_limit;
        uint32_t m_entry;
        bool m_valid;
        bool m_parsed;
        hyperspacehashing::mask::coordinate m_coord;
        uint64_t m_version;
        e::slice m_key;
        std::vector<e::slice> m_value;
};

} // namespace hyperdisk

#endif // hyperdisk_shard_snapshot_h_
