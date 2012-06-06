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
#include <queue>
#include <string>
#include <tr1/memory>
#include <vector>

// po6
#include <po6/pathname.h>
#include <po6/threads/mutex.h>

// e
#include <e/intrusive_ptr.h>
#include <e/lockfree_hash_map.h>
#include <e/locking_iterable_fifo.h>

// HyperspaceHashing
#include <hyperspacehashing/mask.h>

// HyperDisk
#include <hyperdisk/reference.h>
#include <hyperdisk/returncode.h>
#include <hyperdisk/snapshot.h>

// Forward Declarations
namespace hyperdisk
{
class log_entry;
class offset_update;
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
        // Create a new blank disk.
        static e::intrusive_ptr<disk> create(const po6::pathname& directory,
                                             const hyperspacehashing::mask::hasher& hasher,
                                             uint16_t arity);
        // Re-open quiesced disk.                                             
        static e::intrusive_ptr<disk> open(const po6::pathname& directory,
                                           const hyperspacehashing::mask::hasher& hasher,
                                           uint16_t arity,
                                           const std::string& quiesce_state_id);

    public:
        // May return SUCCESS or NOTFOUND.
        returncode get(const e::slice& key, std::vector<e::slice>* value,
                       uint64_t* version, reference* backing);
        // May return SUCCESS or WRONGARITY.
        returncode put(std::tr1::shared_ptr<e::buffer> backing, const e::slice& key,
                       const std::vector<e::slice>& value, uint64_t version);
        // May return SUCCESS.
        returncode del(std::tr1::shared_ptr<e::buffer> backing, const e::slice& key);
        // Create a snapshot of the disk.  The snapshot will contain the result
        // after applying a prefix of the execution history of the disk.
        e::intrusive_ptr<snapshot> make_snapshot(const hyperspacehashing::search& terms);
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
        // convenience.  It will flush at most 'num' items to the underlying
        // disk, 'num' == -1 will flush all.  This will not split underlying 
        // shards which need to be split to make more space.  If this returns 
        // a *FULL error, then you must call either 'do_mandatory_io' or 
        // 'do_optimistic_io'. 
        returncode flush(ssize_t num, bool nonblocking);
        // Do only the amount of shard-splitting necessary to split shards which
        // are 100% used.
        returncode do_mandatory_io();
        // Possibly split one shard if our disk is getting full.
        returncode do_optimistic_io();
        // Preallocate shards to ease the hit we would take from the large
        // amount of disk I/O at once.
        returncode preallocate();
        // Move data either synchronously or asynchronously from operating
        // system buffers to the underlying FS.  May return SUCCESS or
        // SYNCFAILED.  errno will be set to the reason the sync failed.
        returncode async();
        returncode sync();

    public:
        // Quiesce.
        bool quiesce(const std::string& quiesce_state_id);

    private:
        friend class e::intrusive_ptr<disk>;
        class stored;
        static uint64_t hash(const std::string& s);
        typedef e::lockfree_hash_map<std::string, e::intrusive_ptr<stored>, hash>
                stored_map_t;

    private:
        disk(const po6::pathname& directory,
             const hyperspacehashing::mask::hasher& hasher,
             uint16_t arity,
             bool load_quiesced_state = false,
             const std::string& quiesce_state_id = "");
        disk();
        ~disk() throw ();

    private:
        // Reference counting for disks.
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }
        // The pathname (relative to m_base) of a (tmp) shard at coordinate.
        po6::pathname shard_filename(const hyperspacehashing::mask::coordinate& c);
        po6::pathname shard_tmp_filename(const hyperspacehashing::mask::coordinate& c);
        // Create a shard for the given coordinate.  This only creates/mmaps the
        // appropriate file.
        e::intrusive_ptr<shard> create_shard(const hyperspacehashing::mask::coordinate& c);
        e::intrusive_ptr<shard> create_tmp_shard(const hyperspacehashing::mask::coordinate& c);
        // Drop the shard for the given coordinate.  This ONLY unlinks the
        // appropriate file.
        returncode drop_shard(const hyperspacehashing::mask::coordinate& c);
        returncode drop_tmp_shard(const hyperspacehashing::mask::coordinate& c);
        // Deal with shards which cannot hold more data.  The m_shard_mutate
        // lock must be held prior to calling these functions.
        returncode deal_with_full_shard(size_t shard_num);
        returncode clean_shard(size_t shard_num);
        returncode split_shard(size_t shard_num);

    private:
        size_t m_ref;
        size_t m_arity;
        hyperspacehashing::mask::hasher m_hasher;
        // Read about locking in the source.
        po6::threads::mutex m_shards_mutate;
        po6::threads::mutex m_shards_lock;
        e::intrusive_ptr<shard_vector> m_shards;
        e::locking_iterable_fifo<log_entry> m_log;
        e::locking_iterable_fifo<offset_update> m_offsets;
        po6::io::fd m_base;
        po6::pathname m_base_filename;
        po6::threads::mutex m_spare_shards_lock;
        std::queue<std::pair<po6::pathname, e::intrusive_ptr<shard> > > m_spare_shards;
        size_t m_spare_shard_counter;
        size_t m_needs_io;
        unsigned int m_seed;

    private:
        // State dump and load.
        static const int STATE_FILE_VER;
        static const char* STATE_FILE_NAME;
        bool dump_state(const std::string& quiesce_state_id);
        bool load_state(const std::string& quiesce_state_id);
};

} // namespace hyperdisk

#endif // hyperdisk_disk_h_
