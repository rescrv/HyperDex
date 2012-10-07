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
#include <tr1/memory>

// po6
#include <po6/pathname.h>

// e
#include <e/lockfree_hash_map.h>
#include <e/slice.h>

// HyperDex
#include "disk/disk_returncode.h"
#include "datatypes/schema.h"
#include "daemon/reconfigure_returncode.h"
#include "hyperdex/hyperdex/ids.h"

namespace hyperspacehashing
{
class search;
}

namespace hyperdex
{
// Forward Declarations
class configuration;
class disk;
class disk_reference;
class instance;
class regionid;
class search_snapshot;
class transfer_iterator;

class datalayer
{
    public:
        datalayer(const po6::pathname& base);
        ~datalayer() throw ();

    public:
        reconfigure_returncode prepare(const hyperdex::configuration& old_config,
                                       const hyperdex::configuration& new_config,
                                       const hyperdex::instance& us);
        reconfigure_returncode reconfigure(const hyperdex::configuration& old_config,
                                           const hyperdex::configuration& new_config,
                                           const hyperdex::instance& us);
        reconfigure_returncode cleanup(const hyperdex::configuration& old_config,
                                       const hyperdex::configuration& new_config,
                                       const hyperdex::instance& us);
        void close();

    public:
        disk_returncode get(const hyperdex::regionid& ri, const e::slice& key,
                            std::vector<e::slice>* value, uint64_t* version,
                            disk_reference* ref);
        disk_returncode put(const hyperdex::regionid& ri, const e::slice& key,
                            const std::vector<e::slice>& value, uint64_t version);
        disk_returncode del(const hyperdex::regionid& ri, const e::slice& key);
        disk_returncode make_search_snapshot(const hyperdex::regionid& ri,
                                             const hyperspacehashing::search& terms,
                                             search_snapshot* snap);
        disk_returncode make_transfer_iterator(const hyperdex::regionid& ri,
                                               transfer_iterator* snap);

    private:
        static uint64_t regionid_hash(const hyperdex::regionid& r) { return r.hash(); }
        typedef std::tr1::shared_ptr<hyperdex::disk> disk_ptr;
        typedef e::lockfree_hash_map<hyperdex::regionid, disk_ptr, regionid_hash>
                disk_map_t;

    private:
        disk_returncode create_disk(const hyperdex::regionid& ri, schema* sc);
        disk_returncode drop_disk(const hyperdex::regionid& ri);

    private:
        po6::pathname m_base;
        disk_map_t m_disks;
};

} // namespace hyperdex

#endif // hyperdex_daemon_datalayer_h_

#if 0
-        // May return SUCCESS or DIDNOTHING.
-        hyperdisk::returncode flush(const hyperdex::regionid& ri, size_t n, bool nonblocking);
-        hyperdisk::returncode do_mandatory_io(const hyperdex::regionid& ri);
#endif
