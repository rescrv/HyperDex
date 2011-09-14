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

#ifndef hyperdisk_shard_h_
#define hyperdisk_shard_h_

// po6
#include <po6/pathname.h>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>

// HyperDisk
#include "hyperdisk/returncode.h"

// Forward Declarations
namespace hyperspacehashing
{
namespace mask
{
class coordinate;
}
}
namespace hyperdisk
{
class shard_snapshot;
}

// The shard class abstracts a memory mapped file on disk and provides an
// append-only log which may be cheaply snapshotted to allow iteration.  The
// methods of this class require synchronization.  In particular:
//  - Performing a GET requires a READ lock.
//  - Performing a PUT or a DEL requires a WRITE lock.
//  - Cleaning-related methods should have an acquire barrier prior to entry in
//    order to return an accurate result and a failure to do so will lead to an
//    increased number of false negatives.  These are possible anyway so it is
//    not an issue.
//  - Async/Sync require no special locking (they just call msync).
//  - Making a snapshot requires a READ lock exclusive with PUT or DEL
//    operations.
//  - There is no guarantee about GET operations concurrent with PUT or
//    DEL operations.  The disk layer will patch over any erroneous
//    NOTFOUND errors.
//
// This is simply a memory-mapped file.  The file is indexed by both a hash
// table and an append-only log.
//
// The hash table's entries are 64-bits in size.  The high-order 32-bit
// number is the offset in the table at which the indexed object may be
// fount.  The low-order 32-bit number is the hash used to index the
// table.
//
// The append-only log's entries are 128-bits in size.  The first of the
// 64-bit numbers is a combination of both the primary and secondary
// hashes of the object.  The secondary hash is stored in the high-order
// 32-bit number.  The second of the 64-bit numbers is a combination of
// the offest at which the object is stored, and the offset at which the
// object was invalidated.  The offset at which the object is
// invalidated is stored in the high-order 32-bit number.
//
// Entries are set/read as 64-bit words and then bitshifting is applied
// to get high/low numbers.

namespace hyperdisk
{

class shard
{
    public:
        // Create will create a newly initialized shard at the given filename,
        // even if it already exists.  That is, it will overwrite the existing
        // shard (or other file) at "filename".
        static e::intrusive_ptr<shard> create(const po6::io::fd& dir,
                                              const po6::pathname& filename);
        // Open an existing shard.  This will fail if the file doesn't exist.
        // XXX No sanity checking is done on the shard.
        static e::intrusive_ptr<shard> open(const po6::io::fd& dir,
                                            const po6::pathname& filename);

    public:
        // May return SUCCESS or NOTFOUND.
        returncode get(uint32_t primary_hash, const e::buffer& key,
                       std::vector<e::buffer>* value, uint64_t* version);
        // May return SUCCESS, DATAFULL, HASHFULL, or SEARCHFULL.
        returncode put(uint32_t primary_hash, uint32_t secondary_hash,
                       const e::buffer& key,
                       const std::vector<e::buffer>& value,
                       uint64_t version);
        // May return SUCCESS, NOTFOUND, or DATAFULL.
        returncode del(uint32_t primary_hash, const e::buffer& key);
        // The space calc functions are only accurate when mutually exclusive
        // with GET operations.
        // How much stale space (as a percentage) may be reclaimed from this log
        // through cleaning.
        int stale_space() const;
        // How much space (as a percentage) is used by either current or stale
        // data.
        int used_space() const;
        // May return SUCCESS or SYNCFAILED.  errno will be set to the reason
        // the sync failed.
        returncode async();
        // May return SUCCESS or SYNCFAILED.  errno will be set to the reason
        // the sync failed.
        returncode sync();
        e::intrusive_ptr<shard_snapshot> make_snapshot();
        // Copy all non-stale data from this shard to the other shard,
        // completely erasing all the data in the other shard.  Only
        // entries which match the coordinate will be kept.
        void copy_to(const hyperspacehashing::mask::coordinate& c, e::intrusive_ptr<shard> s);
        // Perform a logical integrity check of the shard.
        bool fsck();
        bool fsck(std::ostream& out, std::ostream& err);

    private:
        friend class e::intrusive_ptr<shard>;
        friend class shard_snapshot;
        friend class shard_vector;

    private:
        shard(po6::io::fd* fd);
        shard(const shard&);
        ~shard() throw ();

    private:
        size_t data_size(const e::buffer& key, const std::vector<e::buffer>& value) const;
        uint64_t data_version(uint32_t offset) const;
        size_t data_key_size(uint32_t offset) const;
        size_t data_key_offset(uint32_t offset) const
        { return offset + sizeof(uint64_t) + sizeof(uint32_t); }
        void data_key(uint32_t offset, size_t keysize, e::buffer* key) const;
        void data_value(uint32_t offset, size_t keysize, std::vector<e::buffer>* value) const;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }
        // Find the hash entry which matches the hash/key pair.  The index in
        // the hash table (suitable for indexing m_hash_table) is stored in the
        // location pointed to by 'entry'.  The value read from the entry at
        // some point in the lookup process is stored in the location pointed to
        // by 'value'.  This will always find a free location in the table as
        // the shard_constants are set to guarantee it.
        void hash_lookup(uint32_t primary_hash, const e::buffer& key,
                         size_t* entry, uint64_t* value);
        // This variant assumes that all previously inserted entries with the
        // same primary hash are distinct.
        void hash_lookup(uint32_t primary_hash, size_t* entry);
        // This will invalidate any entry in the search log which references
        // the specified offset.
        void invalidate_search_log(uint32_t to_invalidate, uint32_t invalidate_with);

    private:
        shard& operator = (const shard&);

    private:
        size_t m_ref;
        uint64_t* m_hash_table;
        uint64_t* m_search_log;
        char* m_data;
        uint32_t m_data_offset;
        uint32_t m_search_offset;
};

} // namespace hyperdisk

#endif // hyperdisk_shard_h_
