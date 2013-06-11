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

#ifndef hyperdex_daemon_index_info_h_
#define hyperdex_daemon_index_info_h_

// LevelDB
#include <hyperleveldb/write_batch.h>

// e
#include <e/slice.h>

// HyperDex
#include "hyperdex.h"
#include "common/attribute_check.h"
#include "common/ids.h"
#include "common/range.h"
#include "daemon/datalayer.h"

namespace hyperdex
{

class index_info
{
    public:
        // return NULL for unindexable type
        static index_info* lookup(hyperdatatype datatype);

    public:
        index_info();
        virtual ~index_info() throw ();

    // override these if the type can be encoded into a string representation
    // such that ordering with memcmp reflects the ordering with comparable
    public:
        // is this encoding fixed in size?  are encoded_size() and
        // decoded_size() invariant of the passed slice?
        virtual bool encoding_fixed() = 0;
        // the number of bytes necessary to encode "decoded"
        virtual size_t encoded_size(const e::slice& decoded) = 0;
        // write the encoded form of "decoded" to "encoded" which points to at
        // least "encoded_size" bytes of memory
        virtual char* encode(const e::slice& decoded, char* encoded) = 0;
        // the number of bytes necessary to decode "encoded"
        virtual size_t decoded_size(const e::slice& encoded) = 0;
        // write the decoded form of "encoded" to "decoded" which points to at
        // least "decoded_size" bytes of memory
        virtual char* decode(const e::slice& encoded, char* decoded) = 0;

    // override these if the type can be in a localized index
    public:
        // apply to updates all the writes necessary to transform the index from
        // old_value to new_value
        virtual void index_changes(const region_id& ri,
                                   uint16_t attr,
                                   index_info* key_ii,
                                   const e::slice& key,
                                   const e::slice* old_value,
                                   const e::slice* new_value,
                                   leveldb::WriteBatch* updates) = 0;
        // return an iterator that retrieves at least the keys matching r
        // if not indexable (full scan), return NULL
        virtual datalayer::index_iterator* iterator_from_range(leveldb_snapshot_ptr snap,
                                                               const region_id& ri,
                                                               const range& r,
                                                               index_info* key_ii);
        // return an iterator that retrieves at least the keys that pass c
        // if not indexable (full scan), return NULL
        virtual datalayer::index_iterator* iterator_from_check(leveldb_snapshot_ptr snap,
                                                               const region_id& ri,
                                                               const attribute_check& c,
                                                               index_info* key_ii);
};

} // namespace hyperdex

#endif // hyperdex_daemon_index_info_h_
