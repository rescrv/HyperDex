// Copyright (c) 2014, Cornell University
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

#ifndef hyperdex_daemon_index_document_h_
#define hyperdex_daemon_index_document_h_

// HyperDex
#include "namespace.h"
#include "common/datatype_document.h"
#include "daemon/index_info.h"

BEGIN_HYPERDEX_NAMESPACE

class index_document : public index_info
{
    public:
        index_document();
        virtual ~index_document() throw ();

    public:
        virtual hyperdatatype datatype() const;
        virtual void index_changes(const index* idx,
                                   const region_id& ri,
                                   const index_encoding* key_ie,
                                   const e::slice& key,
                                   const e::slice* old_document,
                                   const e::slice* new_document,
                                   leveldb::WriteBatch* updates) const;
        virtual datalayer::index_iterator* iterator_from_check(leveldb_snapshot_ptr snap,
                                                               const region_id& ri,
                                                               const index_id& ii,
                                                               const attribute_check& c,
                                                               const index_encoding* key_ie) const;
        virtual datalayer::index_iterator* iterator_for_keys(leveldb_snapshot_ptr snap,
                                             const region_id& ri) const;

    private:
        enum type_t { STRING, NUMBER, DOCUMENT };
        bool parse_path(const index* idx,
                        const e::slice& document,
                        type_t* t,
                        std::vector<char>* scratch,
                        e::slice* value) const;
        size_t index_entry_prefix_size(const region_id& ri, const index_id& ii) const;
        void index_entry(const region_id& ri,
                         const index_id& ii,
                         type_t t,
                         std::vector<char>* scratch,
                         e::slice* slice) const;
        void index_entry(const region_id& ri,
                         const index_id& ii,
                         type_t t,
                         const e::slice& value,
                         std::vector<char>* scratch,
                         e::slice* slice) const;
        void index_entry(const region_id& ri,
                         const index_id& ii,
                         type_t t,
                         const index_encoding* key_ie,
                         const e::slice& key,
                         const e::slice& value,
                         std::vector<char>* scratch,
                         e::slice* slice) const;

        datalayer::index_iterator* iterator_key(leveldb_snapshot_ptr snap,
                                        const region_id& ri,
                                        const range& r,
                                        const index_encoding* key_ie) const;

        const index_encoding* lookup_encoding(type_t t) const;

    private:
        const datatype_document m_di;
};

class index_encoding_document : public index_encoding
{
    public:
        index_encoding_document();
        virtual ~index_encoding_document() throw ();

    public:
        virtual bool encoding_fixed() const;
        virtual size_t encoded_size(const e::slice& decoded) const;
        virtual char* encode(const e::slice& decoded, char* encoded) const;
        virtual size_t decoded_size(const e::slice& encoded) const;
        virtual char* decode(const e::slice& encoded, char* decoded) const;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_index_document_h_
