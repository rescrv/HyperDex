// Copyright (c) 2012, Cornell University
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
#include "common/attribute_check.h"
#include "common/configuration.h"
#include "common/counter_map.h"
#include "common/ids.h"
#include "common/schema.h"
#include "daemon/leveldb.h"
#include "daemon/reconfigure_returncode.h"

namespace hyperdex
{
// Forward declarations
class daemon;

class datalayer
{
    public:
        enum returncode
        {
            SUCCESS,
            NOT_FOUND,
            BAD_ENCODING,
            BAD_SEARCH,
            CORRUPTION,
            IO_ERROR,
            LEVELDB_ERROR
        };
        class reference;
        class region_iterator;
        class snapshot;

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
        // create a snapshot for search
        returncode make_snapshot(const region_id& ri,
                                 const schema& sc,
                                 const std::vector<attribute_check>* checks,
                                 snapshot* snap,
                                 std::ostringstream* ostr);
        // leveldb provides no failure mechanism for this, neither do we
        leveldb_snapshot_ptr make_raw_snapshot();
        void make_region_iterator(region_iterator* riter,
                                  leveldb_snapshot_ptr snap,
                                  const region_id& ri);
        returncode get_transfer(const region_id& ri,
                                uint64_t seq_no,
                                bool* has_value,
                                e::slice* key,
                                std::vector<e::slice>* value,
                                uint64_t* version,
                                reference* ref);
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

    private:
        datalayer(const datalayer&);
        datalayer& operator = (const datalayer&);

    private:
        void cleaner();
        void shutdown();

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

class datalayer::region_iterator
{
    public:
        region_iterator();
        ~region_iterator() throw ();

    public:
        bool valid();
        void next();
        void unpack(e::slice* key, std::vector<e::slice>* val, uint64_t* ver, reference* ref);
        e::slice key();

    private:
        friend class datalayer;
        region_iterator(const region_iterator&);
        region_iterator& operator = (const region_iterator&);

    private:
        datalayer* m_dl;
        leveldb_snapshot_ptr m_snap;
        leveldb_iterator_ptr m_iter;
        region_id m_region;
};

class datalayer::snapshot
{
    public:
        snapshot();
        ~snapshot() throw ();

    public:
        bool valid();
        void next();
        void unpack(e::slice* key, std::vector<e::slice>* val, uint64_t* ver);
        void unpack(e::slice* key, std::vector<e::slice>* val, uint64_t* ver, reference* ref);

    private:
        friend class datalayer;
        snapshot(const snapshot&);
        snapshot& operator = (const snapshot&);

    private:
        datalayer* m_dl;
        leveldb_snapshot_ptr m_snap;
        const std::vector<attribute_check>* m_checks;
        region_id m_ri;
        std::list<std::vector<char> > m_backing;
        leveldb::Range m_range;
        bool (*m_parse)(const leveldb::Slice& in, e::slice* out);
        leveldb_iterator_ptr m_iter;
        returncode m_error;
        uint64_t m_version;
        e::slice m_key;
        std::vector<e::slice> m_value;
        std::ostringstream* m_ostr;
        uint64_t m_num_gets;
        reference m_ref;
};

std::ostream&
operator << (std::ostream& lhs, datalayer::returncode rhs);

} // namespace hyperdex

#endif // hyperdex_daemon_datalayer_h_
