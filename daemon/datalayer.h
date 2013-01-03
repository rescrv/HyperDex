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
#include <string>
#include <vector>

// LevelDB
#include <leveldb/db.h>

// po6
#include <po6/pathname.h>
#include <po6/net/hostname.h>
#include <po6/net/location.h>

// HyperDex
#include "common/attribute_check.h"
#include "common/configuration.h"
#include "common/region_id.h"
#include "common/schema.h"
#include "common/server_id.h"
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
            CORRUPTION,
            IO_ERROR,
            LEVELDB_ERROR
        };
        class reference;
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
        reconfigure_returncode prepare(const configuration& old_config,
                                       const configuration& new_config,
                                       const server_id& us);
        reconfigure_returncode reconfigure(const configuration& old_config,
                                           const configuration& new_config,
                                           const server_id& us);
        reconfigure_returncode cleanup(const configuration& old_config,
                                       const configuration& new_config,
                                       const server_id& us);

    public:
        returncode get(const region_id& ri,
                       const e::slice& key,
                       std::vector<e::slice>* value,
                       uint64_t* version,
                       reference* ref);
        returncode put(const region_id& ri,
                       uint64_t seq_id,
                       const e::slice& key,
                       const std::vector<e::slice>& value,
                       uint64_t version);
        returncode del(const region_id& ri,
                       uint64_t seq_id,
                       const e::slice& key);
        returncode make_snapshot(const region_id& ri,
                                 attribute_check* checks,
                                 size_t checks_sz,
                                 snapshot* snap);
        // XXX errors are absorbed here; short of crashing we can only log
        bool check_acked(const region_id& reg_id, uint64_t seq_id);
        void mark_acked(const region_id& reg_id, uint64_t seq_id);
        void max_seq_id(const region_id& reg_id, uint64_t* seq_id);

    private:
        class search_filter;
        typedef std::list<std::vector<char> > backing_t;

    private:
        datalayer(const datalayer&);
        datalayer& operator = (const datalayer&);

    private:
        void encode_key(const region_id& ri,
                        const e::slice& key,
                        std::vector<char>* kbacking,
                        leveldb::Slice* lkey);
        returncode decode_key(const e::slice& lkey,
                              region_id* ri,
                              e::slice* key);
        void encode_value(const std::vector<e::slice>& attrs,
                          uint64_t version,
                          std::vector<char>* backing,
                          leveldb::Slice* lvalue);
        returncode decode_value(const e::slice& value,
                                std::vector<e::slice>* attrs,
                                uint64_t* version);
        void generate_object_range(const region_id& ri,
                                   backing_t* backing,
                                   leveldb::Range* r);
        void generate_index(const region_id& ri,
                            uint16_t attr,
                            hyperdatatype type,
                            const e::slice& value,
                            const e::slice& key,
                            backing_t* backing,
                            std::vector<leveldb::Slice>* idxs);
        void generate_search_filters(const region_id& ri,
                                     attribute_check* check_ptr,
                                     attribute_check* check_end,
                                     backing_t* backing,
                                     std::vector<search_filter>* sf);
        returncode create_index_changes(const schema* sc,
                                        const region_id& ri,
                                        const e::slice& key,
                                        const leveldb::Slice& lkey,
                                        backing_t* backing,
                                        leveldb::WriteBatch* updates);
        returncode create_index_changes(const schema* sc,
                                        const region_id& ri,
                                        const e::slice& key,
                                        const leveldb::Slice& lkey,
                                        const std::vector<e::slice>& value,
                                        backing_t* backing,
                                        leveldb::WriteBatch* updates);

    private:
        daemon* m_daemon;
        leveldb::DB* m_db;
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

class datalayer::snapshot
{
    public:
        snapshot();
        ~snapshot() throw ();

    public:
        bool has_next();
        void next();
        void unpack(e::slice* key, std::vector<e::slice>* val, uint64_t* ver);
        void unpack(e::slice* key, std::vector<e::slice>* val, uint64_t* ver, reference* ref);

    private:
        friend class datalayer;
        snapshot(const snapshot&);
        snapshot& operator = (const snapshot&);

    private:
        datalayer* m_dl;
        const leveldb::Snapshot* m_snap;
        backing_t m_backing;
        std::vector<search_filter> m_sfs;
        const attribute_check* m_checks;
        size_t m_checks_sz;
        region_id m_ri;
        leveldb::Range m_obj_range;
        leveldb::Iterator* m_iter;
        returncode m_error;
        e::slice m_key;
        std::vector<e::slice> m_value;
        uint64_t m_version;
};

std::ostream&
operator << (std::ostream& lhs, datalayer::returncode rhs);

} // namespace hyperdex

#endif // hyperdex_daemon_datalayer_h_
