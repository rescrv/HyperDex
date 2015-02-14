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

#ifndef hyperdex_daemon_index_container_h_
#define hyperdex_daemon_index_container_h_

// HyperDex
#include "namespace.h"
#include "daemon/index_info.h"

BEGIN_HYPERDEX_NAMESPACE

class index_container : public index_info
{
    protected:
        index_container();
        virtual ~index_container() throw ();

    public:
        virtual void index_changes(const index* idx,
                                   const region_id& ri,
                                   const index_encoding* key_ie,
                                   const e::slice& key,
                                   const e::slice* old_value,
                                   const e::slice* new_value,
                                   leveldb::WriteBatch* updates) const;
        virtual datalayer::index_iterator* iterator_from_check(leveldb_snapshot_ptr snap,
                                                               const region_id& ri,
                                                               const index_id& ii,
                                                               const attribute_check& c,
                                                               const index_encoding* key_ie) const;

    private:
        virtual void extract_elements(const e::slice& container,
                                      std::vector<e::slice>* elems) const = 0;
        virtual const datatype_info* element_datatype_info() const = 0;
        virtual const index_info* element_index_info() const = 0;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_index_container_h_
