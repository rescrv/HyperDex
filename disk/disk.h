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

#ifndef hyperdex_disk_disk_h_
#define hyperdex_disk_disk_h_

// po6
#include <po6/pathname.h>

// e
#include <e/intrusive_ptr.h>
#include <e/slice.h>

// HyperDex
#include "disk/append_only_log.h"
#include "disk/cuckoo_index.h"
#include "disk/disk_reference.h"
#include "disk/disk_returncode.h"
#include "disk/search_tree.h"

namespace hyperspacehashing
{
class search;
}

namespace hyperdex
{
class search_snapshot;
class transfer_iterator;

class disk
{
    public:
        disk(const po6::pathname& prefix, size_t attrs);
        ~disk() throw ();

    public:
        // open or create a disk
        disk_returncode open();
        disk_returncode close();
        // Completely remove all traces of the disk
        disk_returncode destroy();

    public:
        disk_returncode get(const e::slice& key, uint64_t key_hash,
                            std::vector<e::slice>* value,
                            uint64_t* version,
                            disk_reference* ref);
        disk_returncode put(const e::slice& key, uint64_t key_hash,
                            const std::vector<e::slice>& value,
                            const std::vector<uint64_t>& value_hashes,
                            uint64_t version);
        disk_returncode del(const e::slice& key, uint64_t key_hash);
        disk_returncode make_search_snapshot(const hyperspacehashing::search& terms,
                                             search_snapshot* snap);
        disk_returncode make_transfer_iterator(transfer_iterator* snap);

    private:
        disk_returncode parse_log_entry(e::buffer* data,
                                        e::slice* key,
                                        std::vector<e::slice>* value,
                                        uint64_t* version);

    private:
        size_t m_attrs;
        po6::pathname m_prefix;
        append_only_log m_log;
        cuckoo_index m_key_idx;
        search_tree m_search_idx;
};

} // namespace hyperdex

#endif // hyperdex_disk_disk_h_
