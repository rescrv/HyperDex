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
#include "common/counter_map.h"
#include "common/datatypes.h"
#include "common/ids.h"
#include "common/schema.h"
#include "daemon/leveldb.h"
#include "daemon/reconfigure_returncode.h"

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
        bool setup(const po6::pathname& path,
                   bool* saved,
                   server_id* saved_us,
                   po6::net::location* saved_bind_to,
                   po6::net::hostname* saved_coordinator);
        void teardown();
        // perform one-time initialization of the db (call after "setup").
        // requires that "saved" was false in "setup".
        bool initialize();
        // implicit sets the "dirty" bit
        bool save_state(const server_id& m_us,
                        const po6::net::location& bind_to,
                        const po6::net::hostname& coordinator);
        // clears the "dirty" bit
        bool clear_dirty();
        void pause();
        void unpause();
        void reconfigure(const configuration& old_config,
                         const configuration& new_config,
                         const server_id& us);
        // stats
        bool get_property(const e::slice& property,
                          std::string* value);
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
        // get a logged transfer
        returncode get_transfer(const region_id& ri,
                                uint64_t seq_no,
                                bool* has_value,
                                e::slice* key,
                                std::vector<e::slice>* value,
                                uint64_t* version,
                                reference* ref);
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
        // Request that a particular capture_id be wiped.  This is requested by
        // the state_transfer_manager.  The state_transfer_manger will get a
        // call back on report_wiped after it is done.
        void request_wipe(const capture_id& cid);
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
        // get the object pointed to by the iterator
        returncode get_from_iterator(const region_id& ri,
                                     iterator* iter,
                                     e::slice* key,
                                     std::vector<e::slice>* value,
                                     uint64_t* version,
                                     reference* ref);

    private:
        datalayer(const datalayer&);
        datalayer& operator = (const datalayer&);

    private:
        void cleaner();
        void shutdown();
        returncode handle_error(leveldb::Status st);

    private:
        daemon* m_daemon;
        leveldb_db_ptr m_db;
        counter_map m_counters;
        po6::threads::thread m_cleaner;
        po6::threads::mutex m_block_cleaner;
        po6::threads::cond m_wakeup_cleaner;
        po6::threads::cond m_wakeup_reconfigurer;
        bool m_need_cleaning;
        bool m_shutdown;
        bool m_need_pause;
        bool m_paused;
        std::set<capture_id> m_state_transfer_captures;
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
