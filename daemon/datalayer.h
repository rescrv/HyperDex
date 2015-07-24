// Copyright (c) 2012-2013, Cornell University
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

#ifndef hyperdex_daemon_datalayer_h_
#define hyperdex_daemon_datalayer_h_

// STL
#include <list>
#include <set>
#include <sstream>
#include <string>
#include <vector>

// LevelDB
#include <hyperleveldb/db.h>

// po6
#include <po6/net/hostname.h>
#include <po6/net/location.h>
#include <po6/path.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/ao_hash_map.h>

// HyperDex
#include "namespace.h"
#include "common/attribute_check.h"
#include "common/configuration.h"
#include "common/datatype_info.h"
#include "common/ids.h"
#include "common/schema.h"
#include "daemon/leveldb.h"
#include "daemon/reconfigure_returncode.h"
#include "daemon/region_timestamp.h"

BEGIN_HYPERDEX_NAMESPACE
class daemon;

class datalayer
{
    public:
        enum returncode
        {
            SUCCESS,
            NOT_FOUND,
            BAD_ENCODING,
            CORRUPTION,
            IO_ERROR,
            LEVELDB_ERROR
        };
        class reference;
        class iterator;
        class replay_iterator;
        class dummy_iterator;
        class region_iterator;
        class search_iterator;
        class index_iterator;
        class range_index_iterator;
        class intersect_iterator;
        typedef leveldb_snapshot_ptr snapshot;
        // must be pow2
        const static uint64_t REGION_PERIODIC = 65536;

    public:
        datalayer(daemon*);
        ~datalayer() throw ();

    public:
        bool initialize(const std::string& path,
                        bool* saved,
                        server_id* saved_us,
                        po6::net::location* saved_bind_to,
                        po6::net::hostname* saved_coordinator);
        bool save_state(const server_id& m_us,
                        const po6::net::location& bind_to,
                        const po6::net::hostname& coordinator);
        void teardown();
        // reconfiguration
        void pause();
        void unpause();
        void reconfigure(const configuration& old_config,
                         const configuration& new_config,
                         const server_id& us);
        void debug_dump();
        // stats
        bool get_property(const e::slice& property,
                          std::string* value);
        std::string get_timestamp();
        uint64_t approximate_size();

    public:
        // retrieve the current value of a key
        returncode get(const region_id& ri,
                       const e::slice& key,
                       std::vector<e::slice>* value,
                       uint64_t* version,
                       reference* ref);
        // put, overput, or delete a key where the existing value is known
        returncode del(const region_id& ri,
                       const e::slice& key,
                       const std::vector<e::slice>& old_value);
        returncode put(const region_id& ri,
                       const e::slice& key,
                       const std::vector<e::slice>& new_value,
                       uint64_t version);
        returncode overput(const region_id& ri,
                           const e::slice& key,
                           const std::vector<e::slice>& old_value,
                           const std::vector<e::slice>& new_value,
                           uint64_t version);
        // put or delete where the previous value is unknown
        returncode uncertain_del(const region_id& ri,
                                 const e::slice& key);
        returncode uncertain_put(const region_id& ri,
                                 const e::slice& key,
                                 const std::vector<e::slice>& new_value,
                                 uint64_t version);
        // leveldb provides no failure mechanism for this, neither do we
        snapshot make_snapshot();
        // create iterators from snapshots
        iterator* make_search_iterator(snapshot snap,
                                       const region_id& ri,
                                       const std::vector<attribute_check>& checks,
                                       std::ostringstream* ostr);
        // backups
        bool backup(const e::slice& name);
        // get the object pointed to by the iterator
        returncode get_from_iterator(const region_id& ri,
                                     const schema& sc,
                                     iterator* iter,
                                     e::slice* key,
                                     std::vector<e::slice>* value,
                                     uint64_t* version,
                                     reference* ref);
        // track version counters
        void bump_version(const region_id& ri, uint64_t version);
        uint64_t max_version(const region_id& ri);
        // checkpointing
        returncode create_checkpoint(const region_timestamp& rt);
        void set_checkpoint_gc(uint64_t checkpoint_gc);
        void largest_checkpoint_for(const region_id& ri, uint64_t* checkpoint);
        bool region_will_be_wiped(region_id rid);
        void request_wipe(const transfer_id& xid,
                          const region_id& ri);
        void inhibit_wiping();
        void permit_wiping();
        replay_iterator* replay_region_from_checkpoint(const region_id& ri,
                                                       uint64_t checkpoint, bool* wipe);
        // indexing
        void create_index_marker(const region_id& ri, const index_id& ii);
        bool has_index_marker(const region_id& ri, const index_id& ii);
        // used on startup
        bool only_key_is_hyperdex_key();
        bool upgrade_13x_to_14();

    private:
        class index_state;
        class checkpointer_thread;
        class indexer_thread;
        class wiper_thread;
        class wiper_indexer_mediator;
        datalayer(const datalayer&);
        datalayer& operator = (const datalayer&);

    private:
        bool write_version(const region_id& ri,
                           uint64_t version,
                           leveldb::WriteBatch* updates);
        void update_memory_version(const region_id& ri, uint64_t version);
        uint64_t disk_version(const region_id& ri);
        void find_indices(const region_id& rid,
                          std::vector<const index*>* indices);
        void find_indices(const region_id& rid, uint16_t attr,
                          std::vector<const index*>* indices);

        returncode handle_error(leveldb::Status st);
        void collect_lower_checkpoints(uint64_t checkpoint_gc);

        const static region_id defaultri;
        static uint64_t id(region_id ri) { return ri.get(); }

    private:
        daemon* m_daemon;
        leveldb_db_ptr m_db;
        std::vector<index_state> m_indices;
        e::ao_hash_map<region_id, uint64_t, id, defaultri> m_versions;
        const std::auto_ptr<checkpointer_thread> m_checkpointer;
        const std::auto_ptr<wiper_indexer_mediator> m_mediator;
        const std::auto_ptr<indexer_thread> m_indexer;
        const std::auto_ptr<wiper_thread> m_wiper;
};

class datalayer::reference
{
    public:
        reference();
        ~reference() throw ();

    public:
        void swap(reference* ref);

    private:
        friend class datalayer;

    private:
        std::string m_backing;
};

std::ostream&
operator << (std::ostream& lhs, datalayer::returncode rhs);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_datalayer_h_
