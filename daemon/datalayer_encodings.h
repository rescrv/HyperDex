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

#ifndef hyperdex_daemon_datalayer_encodings_h_
#define hyperdex_daemon_datalayer_encodings_h_

// LevelDB
#include <hyperleveldb/slice.h>

// HyperDex
#include "namespace.h"
#include "common/ids.h"
#include "daemon/datalayer.h"

BEGIN_HYPERDEX_NAMESPACE

size_t
object_prefix_sz(region_id ri);

char*
encode_object_prefix(region_id ri, char* ptr);

void
encode_object_region(const region_id& ri,
                     std::vector<char>* scratch,
                     leveldb::Slice* out);

void
encode_key(const region_id& ri,
           const e::slice& internal_key,
           std::vector<char>* scratch,
           leveldb::Slice* out);

void
encode_key(const region_id& ri,
           hyperdatatype key_type,
           const e::slice& key,
           std::vector<char>* scratch,
           leveldb::Slice* out);

bool
decode_key(const leveldb::Slice& in,
           region_id* ri,
           e::slice* internal_key);

void
encode_value(const std::vector<e::slice>& attrs,
             uint64_t version,
             std::vector<char>* backing,
             leveldb::Slice* out);
datalayer::returncode
decode_value(const e::slice& in,
             std::vector<e::slice>* attrs,
             uint64_t* version);

// Encode the record of an operation for which we have sent an ACK
#define VERSION_BUF_SIZE (sizeof(uint8_t) + 2 * sizeof(uint64_t))
void
encode_version(const region_id& ri, /*region we wrote*/
               uint64_t version,
               char* out);
datalayer::returncode
decode_version(const e::slice& in,
               region_id* ri, /*region we saw an ack for*/
               uint64_t* version);

// checkpoints
#define CHECKPOINT_BUF_SIZE (sizeof(uint8_t) + 2 * sizeof(uint64_t))
void
encode_checkpoint(const region_id& ri,
                  uint64_t checkpoint,
                  char* out);
datalayer::returncode
decode_checkpoint(const e::slice& in,
                  region_id* ri,
                  uint64_t* checkpoint);

void
create_index_changes(const schema& sc,
                     const region_id& ri,
                     const std::vector<const index*>& indices,
                     const e::slice& key,
                     const std::vector<e::slice>* old_value,
                     const std::vector<e::slice>* new_value,
                     leveldb::WriteBatch* updates);

void
encode_bump(char* start, char* end);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_datalayer_encodings_h_
