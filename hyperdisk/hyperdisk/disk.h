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

#ifndef hyperdisk_disk_h_
#define hyperdisk_disk_h_

// STL
#include <memory>
#include <queue>
#include <vector>

// po6
#include <po6/pathname.h>
#include <po6/threads/mutex.h>

// e
#include <e/intrusive_ptr.h>
#include <e/locking_iterable_fifo.h>

// HyperDisk
#include <hyperdisk/returncode.h>
#include <hyperdisk/snapshot.h>

// Forward Declarations
namespace hyperdisk
{
class coordinate;
class log_entry;
class shard;
class shard_vector;
}

namespace hyperdisk
{

// A simple embeddable disk layer which offers linearizable GET/PUT/DEL
// operations, and snapshots which exhibit monotonic reads.
//
// All public methods are thread-safe, and synchronization is handled
// internally.

class disk
{
    public:
        // XXX Make this right with reference counting.
        disk(const po6::pathname& directory, uint16_t arity);
        ~disk() throw ();

    public:
        // May return SUCCESS or NOTFOUND.
        returncode get(const e::buffer& key, std::vector<e::buffer>* value,
                       uint64_t* version);
        // May return SUCCESS or WRONGARITY.
        returncode put(const e::buffer& key, const std::vector<e::buffer>& value,
                       uint64_t version);
        // May return SUCCESS.
        returncode del(const e::buffer& key);
        // Create a snapshot of the disk.  The snapshot will contain the result
        // after applying a prefix of the execution history of the disk.
        e::intrusive_ptr<snapshot> make_snapshot();
        // Create a snapshot of the disk.  This will return every result that
        // will be returned by make_snapshot(), but will then continue to return
        // any execution history past the point at which the snapshot was taken.
        e::intrusive_ptr<rolling_snapshot> make_rolling_snapshot();
        // Drop the disk.  This removes it from the filesystem.  All existing
        // snapshots will continue to exist, but no calls should be made to the
        // disk (except the destructor).
        returncode drop();

    public:
        // Move data from in-memory data structures to the shards.  This
        // operation incurs disk I/O in proportion to the amount of backed-up
        // data in the write-ahead log.  It does not immediately flush data to
        // the underlying hard disk, instead letting the OS do so at its own
        // convenience.
        returncode flush();
        // Preallocate shards to ease the hit we would take from the large
        // amount of disk I/O at once.
        returncode preallocate();
        // Move data either synchronously or asynchronously from operating
        // system buffers to the underlying FS.  May return SUCCESS or
        // SYNCFAILED.  errno will be set to the reason the sync failed.
        returncode async();
        returncode sync();

    private:
        friend class e::intrusive_ptr<disk>;

    private:
        // Reference counting for disks.
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }
        // The pathname (relative to m_base) of a (tmp) shard at coordinate.
        po6::pathname shard_filename(const coordinate& c);
        po6::pathname shard_tmp_filename(const coordinate& c);
        // Create a shard for the given coordinate.  This only creates/mmaps the
        // appropriate file.
        e::intrusive_ptr<shard> create_shard(const coordinate& c);
        e::intrusive_ptr<shard> create_tmp_shard(const coordinate& c);
        // Drop the shard for the given coordinate.  This ONLY unlinks the
        // appropriate file.
        returncode drop_shard(const coordinate& c);
        returncode drop_tmp_shard(const coordinate& c);
        // Turn the hashes associated with an object into a shard's coordinate.
        coordinate get_coordinate(const e::buffer& key);
        coordinate get_coordinate(const e::buffer& key, const std::vector<e::buffer>& value);
        // Deal with shards which cannot hold more data.  The m_shard_mutate
        // lock must be held prior to calling these functions.
        returncode deal_with_full_shard(size_t shard_num);
        returncode clean_shard(size_t shard_num);
        returncode split_shard(size_t shard_num);

    private:
        size_t m_ref;
        size_t m_arity;
        // Read about locking in the source.
        po6::threads::mutex m_shards_mutate;
        po6::threads::mutex m_shards_lock;
        e::intrusive_ptr<shard_vector> m_shards;
        e::locking_iterable_fifo<log_entry> m_log;
        po6::io::fd m_base;
        po6::pathname m_base_filename;
        po6::threads::mutex m_spare_shards_lock;
        std::queue<std::pair<po6::pathname, e::intrusive_ptr<shard> > > m_spare_shards;
        size_t m_spare_shard_counter;
};

} // namespace hyperdisk

#endif // hyperdisk_disk_h_
