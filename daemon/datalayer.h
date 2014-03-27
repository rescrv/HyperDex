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
#include <tr1/memory>
#include <vector>

// LevelDB
#include <hyperleveldb/db.h>

// po6
#include <po6/net/hostname.h>
#include <po6/net/location.h>
#include <po6/pathname.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// HyperDex
#include "namespace.h"
#include "common/attribute_check.h"
#include "common/configuration.h"
#include "common/datatypes.h"
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
            LEVELDB_ERROR,
            INVALID_REGION
        };
        class reference;
        class iterator;
        class replay_iterator;
        class dummy_iterator;
        class region_iterator;
        class search_iterator;
        class index_iterator;
        class sorted_iterator;
        class unsorted_iterator;
        class intersect_iterator;
        typedef leveldb_snapshot_ptr snapshot;

    public:
        datalayer(daemon*);
        ~datalayer() throw ();

    public:
        bool initialize(const po6::pathname& path,
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
                       const region_id& reg_id,
                       uint64_t seq_id,
                       const e::slice& key,
                       const std::vector<e::slice>& old_value);
        returncode put(const region_id& ri,
                       const region_id& reg_id,
                       uint64_t seq_id,
                       const e::slice& key,
                       const std::vector<e::slice>& new_value,
                       uint64_t version);
        returncode overput(const region_id& ri,
                           const region_id& reg_id,
                           uint64_t seq_id,
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
        // state from retransmitted messages
        // XXX errors are absorbed here; short of crashing we can only log
        bool check_acked(const region_id& ri,
                         const region_id& reg_id,
                         uint64_t seq_id);
        void mark_acked(const region_id& ri,
                        const region_id& reg_id,
                        uint64_t seq_id);
        void max_seq_id(const region_id& reg_id,
                        uint64_t* seq_id);
        // Clear less than seq_id
        void clear_acked(const region_id& reg_id,
                         uint64_t seq_id);
        // leveldb provides no failure mechanism for this, neither do we
        snapshot make_snapshot();
        // create iterators from snapshots
        iterator* make_region_iterator(snapshot snap,
                                       const region_id& ri,
                                       returncode* error);
        iterator* make_search_iterator(snapshot snap,
                                       const region_id& ri,
                                       const std::vector<attribute_check>& checks,
                                       std::ostringstream* ostr);
        // backups
        bool backup(const e::slice& name);
        // get the object pointed to by the iterator
        returncode get_from_iterator(const region_id& ri,
                                     iterator* iter,
                                     e::slice* key,
                                     std::vector<e::slice>* value,
                                     uint64_t* version,
                                     reference* ref);
        // checkpointing
        returncode create_checkpoint(const region_timestamp& rt);
        void set_checkpoint_lower_gc(uint64_t checkpoint_gc);
        void largest_checkpoint_for(const region_id& ri, uint64_t* checkpoint);
        void request_wipe(const transfer_id& xid,
                          const region_id& ri);
        replay_iterator* replay_region_from_checkpoint(const region_id& ri,
                                                       uint64_t checkpoint, bool* wipe);
        // used on startup
        bool only_key_is_hyperdex_key();

    private:
        datalayer(const datalayer&);
        datalayer& operator = (const datalayer&);

    private:
        void checkpointer();
        void wiper();
        void wipe_checkpoints(const region_id& rid);
        bool wipe_some_indices(const region_id& rid);
        bool wipe_some_objects(const region_id& rid);
        bool wipe_some_common(uint8_t c, const region_id& rid);
        void shutdown();
        returncode handle_error(leveldb::Status st);
        void collect_lower_checkpoints(uint64_t checkpoint_gc);

    private:
        daemon* m_daemon;
        leveldb_db_ptr m_db;
        po6::threads::thread m_checkpointer;
        po6::threads::thread m_wiper;
        po6::threads::mutex m_protect;
        po6::threads::cond m_wakeup_checkpointer;
        po6::threads::cond m_wakeup_wiper;
        po6::threads::cond m_wakeup_reconfigurer;
        bool m_shutdown;
        bool m_need_pause;
        bool m_checkpointer_paused;
        bool m_wiper_paused;
        uint64_t m_checkpoint_gc;
        typedef std::list<std::pair<transfer_id, region_id> > wipe_list_t;
        wipe_list_t m_wiping;
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
