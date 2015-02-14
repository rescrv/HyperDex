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

// STL
#include <algorithm>

// e
#include <e/endian.h>

// HyperDex
#include "daemon/datalayer.h"
#include "daemon/index_container.h"

using hyperdex::datalayer;
using hyperdex::index_container;

index_container :: index_container()
{
}

index_container :: ~index_container() throw ()
{
}

void
index_container :: index_changes(const index* idx,
                                 const region_id& ri,
                                 const index_encoding* key_ie,
                                 const e::slice& key,
                                 const e::slice* old_value,
                                 const e::slice* new_value,
                                 leveldb::WriteBatch* updates) const
{
    std::vector<e::slice> old_elems;
    std::vector<e::slice> new_elems;

    if (old_value)
    {
        this->extract_elements(*old_value, &old_elems);
    }

    if (new_value)
    {
        this->extract_elements(*new_value, &new_elems);
    }

    std::sort(old_elems.begin(), old_elems.end());
    std::sort(new_elems.begin(), new_elems.end());
    std::vector<e::slice>::iterator it;
    it = std::unique(old_elems.begin(), old_elems.end());
    old_elems.resize(it - old_elems.begin());
    it = std::unique(new_elems.begin(), new_elems.end());
    new_elems.resize(it - new_elems.begin());
    size_t old_idx = 0;
    size_t new_idx = 0;
    const index_info* ii = this->element_index_info();

    while (old_idx < old_elems.size() &&
           new_idx < new_elems.size())
    {
        if (old_elems[old_idx] == new_elems[new_idx])
        {
            ii->index_changes(idx, ri, key_ie, key,
                              &old_elems[old_idx], &new_elems[new_idx],
                              updates);
            ++old_idx;
            ++new_idx;
        }
        else if (old_elems[old_idx] < new_elems[new_idx])
        {
            ii->index_changes(idx, ri, key_ie, key,
                              &old_elems[old_idx], NULL, updates);
            ++old_idx;
        }
        else if (old_elems[old_idx] > new_elems[new_idx])
        {
            ii->index_changes(idx, ri, key_ie, key,
                              NULL, &new_elems[new_idx], updates);
            ++new_idx;
        }
    }

    while (old_idx < old_elems.size())
    {
        ii->index_changes(idx, ri, key_ie, key,
                          &old_elems[old_idx], NULL, updates);
        ++old_idx;
    }

    while (new_idx < new_elems.size())
    {
        ii->index_changes(idx, ri, key_ie, key,
                          NULL, &new_elems[new_idx], updates);
        ++new_idx;
    }
}

datalayer::index_iterator*
index_container :: iterator_from_check(leveldb_snapshot_ptr snap,
                                       const region_id& ri,
                                       const index_id& ii,
                                       const attribute_check& c,
                                       const index_encoding* key_ie) const
{
    if (c.predicate == HYPERPREDICATE_CONTAINS &&
        c.datatype == this->element_datatype_info()->datatype())
    {
        range r;
        r.attr = c.attr;
        r.type = c.datatype;
        r.start = c.value;
        r.end = c.value;
        r.has_start = true;
        r.has_end = true;
        r.invalid = false;
        return this->element_index_info()->iterator_from_range(snap, ri, ii, r, key_ie);
    }

    return NULL;
}
