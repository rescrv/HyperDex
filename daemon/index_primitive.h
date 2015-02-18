// Copyright (c) 2013, Cornell University
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

#ifndef hyperdex_daemon_index_primitive_h_
#define hyperdex_daemon_index_primitive_h_

// HyperDex
#include "namespace.h"
#include "daemon/index_info.h"

BEGIN_HYPERDEX_NAMESPACE

class index_primitive : public index_info
{
    protected:
        index_primitive(const index_encoding* ie);
        virtual ~index_primitive() throw ();

    public:
        virtual void index_changes(const index* idx,
                                   const region_id& ri,
                                   const index_encoding* key_ie,
                                   const e::slice& key,
                                   const e::slice* old_value,
                                   const e::slice* new_value,
                                   leveldb::WriteBatch* updates) const;
        virtual datalayer::index_iterator* iterator_for_keys(leveldb_snapshot_ptr snap,
                                                             const region_id& ri) const;
        virtual datalayer::index_iterator* iterator_from_range(leveldb_snapshot_ptr snap,
                                                               const region_id& ri,
                                                               const index_id& ii,
                                                               const range& r,
                                                               const index_encoding* key_ie) const;

    private:
        class range_iterator;
        class key_iterator;

    private:
        datalayer::index_iterator* iterator_key(leveldb_snapshot_ptr snap,
                                                const region_id& ri,
                                                const range& r,
                                                const index_encoding* key_ie) const;
        datalayer::index_iterator* iterator_attr(leveldb_snapshot_ptr snap,
                                                 const region_id& ri,
                                                 const index_id& ii,
                                                 const range& r,
                                                 const index_encoding* key_ie) const;
        size_t index_entry_prefix_size(const region_id& ri, const index_id& ii) const;
        void index_entry(const region_id& ri,
                         const index_id& ii,
                         std::vector<char>* scratch,
                         e::slice* slice) const;
        void index_entry(const region_id& ri,
                         const index_id& ii,
                         const e::slice& value,
                         std::vector<char>* scratch,
                         e::slice* slice) const;
        void index_entry(const region_id& ri,
                         const index_id& ii,
                         const e::slice& internal_key,
                         const e::slice& value,
                         std::vector<char>* scratch,
                         e::slice* slice) const;
        void index_entry(const region_id& ri,
                         const index_id& ii,
                         const index_encoding* key_ie,
                         const e::slice& key,
                         const e::slice& value,
                         std::vector<char>* scratch,
                         e::slice* slice) const;

    private:
        index_primitive(const index_primitive&);
        index_primitive& operator = (const index_primitive&);

    private:
        const index_encoding *const m_ie;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_index_primitive_h_
