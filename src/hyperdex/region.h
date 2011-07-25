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

#ifndef hyperdex_region_h_
#define hyperdex_region_h_

// STL
#include <map>
#include <set>
#include <utility>
#include <vector>

// po6
#include <po6/pathname.h>
#include <po6/threads/rwlock.h>

// e
#include <e/intrusive_ptr.h>

// HyperDisk
#include <hyperdisk/shard.h>

// HyperDex
#include <hyperdex/ids.h>
#include <hyperdex/log.h>
#include <hyperdex/result_t.h>

// XXX The use of the rwlock makes things take a performance hit when cleaning
// or splitting regions.  This shouldn't be necessary.  Instead, what we should
// do is ensure that disks allow many readers/one writer, and then just protect
// the writers (a single mutex around flush will do this just fine).

// XXX The whole "region" class and subclasses (e.g., disk) do not have a
// "shutdown" method.  This means that it's not possible to safely shutdown a
// hyperdex cluster.
namespace hyperdex
{

class region
{
    public:
        // A snapshot will iterate all shards, frozen at a particular point in
        // time.
        class snapshot
        {
            public:
                bool valid();
                void next();

            public:
                uint32_t secondary_point();
                op_t op();
                uint64_t version();
                e::buffer key();
                std::vector<e::buffer> value();

            private:
                friend class e::intrusive_ptr<snapshot>;
                friend class region;

            private:
                snapshot(std::vector<e::intrusive_ptr<hyperdisk::shard::snapshot> >* ss);
                snapshot(const snapshot&);

            private:
                snapshot& operator = (const snapshot&);

            private:
                std::vector<e::intrusive_ptr<hyperdisk::shard::snapshot> > m_snaps;
                size_t m_ref;
        };

        // A rolling snapshot will replay m_log after iterating all shard.
        class rolling_snapshot
        {
            public:
                bool valid();
                void next();

            public:
                op_t op();
                uint64_t version();
                e::buffer key();
                std::vector<e::buffer> value();

            private:
                friend class e::intrusive_ptr<rolling_snapshot>;
                friend class region;

            private:
                rolling_snapshot(const log::iterator& iter, e::intrusive_ptr<snapshot> snap);
                rolling_snapshot(const rolling_snapshot&);

            private:
                rolling_snapshot& operator = (const rolling_snapshot&);

            private:
                log::iterator m_iter;
                e::intrusive_ptr<snapshot> m_snap;
                size_t m_ref;
        };

    public:
        region(const regionid& ri, const po6::pathname& directory, uint16_t nc);

    public:
        result_t get(const e::buffer& key, std::vector<e::buffer>* value,
                     uint64_t* version);
        result_t put(const e::buffer& key, const std::vector<e::buffer>& value,
                     uint64_t version);
        result_t del(const e::buffer& key);
        bool flush();
        void async();
        void sync();
        void drop();
        e::intrusive_ptr<snapshot> make_snapshot();
        e::intrusive_ptr<rolling_snapshot> make_rolling_snapshot();

    private:
        friend class e::intrusive_ptr<region>;

    private:
        // Create/drop shard requires exclusive access to m_shards (m_rwlock as a
        // write lock).
        e::intrusive_ptr<hyperdisk::shard> create_shard(const regionid& ri);
        void drop_shard(const regionid& ri);
        void get_value_hashes(const std::vector<e::buffer>& value, std::vector<uint64_t>* value_hashes);
        uint64_t get_point_for(uint64_t key_hash);
        uint64_t get_point_for(uint64_t key_hash, const std::vector<uint64_t>& value_hashes);
        bool flush_one(op_t op, uint64_t point, const e::buffer& key,
                       uint64_t key_hash, const std::vector<e::buffer>& value,
                       const std::vector<uint64_t>& value_hashes, uint64_t version);
        e::intrusive_ptr<snapshot> inner_make_snapshot();
        void clean_shard(const regionid& ri);
        void split_shard(const regionid& ri);

    private:
        size_t m_ref;
        size_t m_numcolumns;
        uint64_t m_point_mask;
        log m_log;
        po6::threads::rwlock m_rwlock;
        typedef std::map<regionid, e::intrusive_ptr<hyperdisk::shard> > shard_collection;
        shard_collection m_shards;
        po6::pathname m_base;
        std::set<regionid> m_needs_more_space;
};

} // namespace hyperdex

#endif // hyperdex_region_h_
