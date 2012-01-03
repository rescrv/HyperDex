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

#ifndef hyperdisk_snapshot_h_
#define hyperdisk_snapshot_h_

// STL
#include <memory>

// e
#include <e/intrusive_ptr.h>
#include <e/locking_iterable_fifo.h>
#include <e/slice.h>

// HyperspaceHashing
#include <hyperspacehashing/mask.h>

// Forward Declarations
namespace hyperdisk
{
class log_entry;
class shard_snapshot;
class shard_vector;
}

namespace hyperdisk
{

// A snapshot will iterate all shards in a disk, frozen at a particular point in
// time.  That is, it will be linearizable with all updates to the disk.
class snapshot
{
    public:
        bool valid();
        void next();

    public:
        hyperspacehashing::mask::coordinate coordinate();
        uint64_t version();
        const e::slice& key();
        const std::vector<e::slice>& value();

    private:
        friend class e::intrusive_ptr<snapshot>;
        friend class disk;

    private:
        snapshot(const hyperspacehashing::mask::coordinate& coord,
                 e::intrusive_ptr<shard_vector> shards,
                 std::vector<hyperdisk::shard_snapshot>* snaps);
        snapshot(const snapshot&);
        ~snapshot() throw ();

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        snapshot& operator = (const snapshot&);

    private:
        size_t m_ref;
        hyperspacehashing::mask::coordinate m_coord;
        e::intrusive_ptr<shard_vector> m_shards;
        std::vector<hyperdisk::shard_snapshot> m_snaps;
};

// A rolling snapshot will replay the disks' log after iterating all shards.
class rolling_snapshot
{
    public:
        bool valid();
        void next();

    public:
        bool has_value();
        uint64_t version();
        const e::slice& key();
        const std::vector<e::slice>& value();

    private:
        friend class e::intrusive_ptr<rolling_snapshot>;
        friend class disk;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        rolling_snapshot(const e::locking_iterable_fifo<log_entry>::iterator& iter,
                         const e::intrusive_ptr<snapshot>& snap);
        rolling_snapshot(const rolling_snapshot&);
        ~rolling_snapshot() throw ();

    private:
        rolling_snapshot& operator = (const rolling_snapshot&);

    private:
        size_t m_ref;
        e::locking_iterable_fifo<log_entry>::iterator m_iter;
        e::intrusive_ptr<snapshot> m_snap;
};

} // namespace hyperdisk

#endif // hyperdisk_snapshot_h_
