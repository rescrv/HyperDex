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
#include "daemon/index_info.h"

namespace hyperdex
{

class index_primitive : public index_info
{
    protected:
        index_primitive();
        virtual ~index_primitive() throw ();

    public:
        virtual void index_changes(const region_id& ri,
                                   uint16_t attr,
                                   index_info* key_ii,
                                   const e::slice& key,
                                   const e::slice* old_value,
                                   const e::slice* new_value,
                                   leveldb::WriteBatch* updates);
        virtual datalayer::index_iterator* iterator_from_range(leveldb_snapshot_ptr snap,
                                                               const region_id& ri,
                                                               const range& r,
                                                               index_info* key_ii);

    public:
        void index_entry(const region_id& ri,
                         uint16_t attr,
                         std::vector<char>* scratch,
                         leveldb::Slice* slice);
        void index_entry(const region_id& ri,
                         uint16_t attr,
                         const e::slice& value,
                         std::vector<char>* scratch,
                         leveldb::Slice* slice);
        void index_entry(const region_id& ri,
                         uint16_t attr,
                         index_info* key_ii,
                         const e::slice& key,
                         const e::slice& value,
                         std::vector<char>* scratch,
                         leveldb::Slice* slice);
};

} // namespace hyperdex

#endif // hyperdex_daemon_index_primitive_h_
