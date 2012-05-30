// Copyright (c) 2011, Cornell University
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

#ifndef hyperdaemon_datalayer_h_
#define hyperdaemon_datalayer_h_

// STL
#include <list>
#include <map>
#include <set>
#include <tr1/memory>
#include <vector>

// po6
#include <po6/threads/rwlock.h>
#include <po6/threads/thread.h>

// e
#include <e/buffer.h>
#include <e/lockfree_hash_map.h>

// HyperDisk
#include "hyperdisk/hyperdisk/disk.h"
#include "hyperdisk/hyperdisk/returncode.h"

// HyperDex
#include "hyperdex/hyperdex/ids.h"

// Forward Declarations
namespace hyperdex
{
class configuration;
class coordinatorlink;
class instance;
class regionid;
}

namespace hyperdaemon
{

class datalayer
{
    public:
        datalayer(hyperdex::coordinatorlink* cl, const po6::pathname& base);
        ~datalayer() throw ();

    public:
        void prepare(const hyperdex::configuration& newconfig, const hyperdex::instance& us);
        void reconfigure(const hyperdex::configuration& newconfig, const hyperdex::instance& us);
        void cleanup(const hyperdex::configuration& newconfig, const hyperdex::instance& us);
        void shutdown();
        e::intrusive_ptr<hyperdisk::snapshot> make_snapshot(const hyperdex::regionid& ri,
                                                            const hyperspacehashing::search& terms);
        e::intrusive_ptr<hyperdisk::rolling_snapshot> make_rolling_snapshot(const hyperdex::regionid& ri);

    // Key-Value store operations.
    public:
        // May return SUCCESS, NOTFOUND or MISSINGDISK.
        hyperdisk::returncode get(const hyperdex::regionid& ri, const e::slice& key,
                                  std::vector<e::slice>* value, uint64_t* version,
                                  hyperdisk::reference* ref);
        // May return SUCCESS, WRONGARITY or MISSINGDISK.
        hyperdisk::returncode put(const hyperdex::regionid& ri,
                                  std::tr1::shared_ptr<e::buffer> backing,
                                  const e::slice& key,
                                  const std::vector<e::slice>& value,
                                  uint64_t version);
        // May return SUCCESS or MISSINGDISK.
        hyperdisk::returncode del(const hyperdex::regionid& ri,
                                  std::tr1::shared_ptr<e::buffer> backing,
                                  const e::slice& key);
        // May return SUCCESS or DIDNOTHING.
        hyperdisk::returncode flush(const hyperdex::regionid& ri, size_t n, bool nonblocking);
        hyperdisk::returncode do_mandatory_io(const hyperdex::regionid& ri);

    private:
        static uint64_t regionid_hash(const hyperdex::regionid& r) { return r.hash(); }
        typedef e::lockfree_hash_map<hyperdex::regionid, e::intrusive_ptr<hyperdisk::disk>, regionid_hash>
                disk_map_t;

    private:
        datalayer(const datalayer&);

    private:
        void optimistic_io_thread();
        void flush_thread();
        // Create a blank disk.
        void create_disk(const hyperdex::regionid& ri,
                         const hyperspacehashing::mask::hasher& hasher,
                         uint16_t num_columns);
        // Re-open a disk that was quiesced.
        void open_disk(const hyperdex::regionid& ri,
                       const hyperspacehashing::mask::hasher& hasher,
                       uint16_t num_columns,
                       const std::string& quiesce_state_id);
        void drop_disk(const hyperdex::regionid& ri);

    private:
        datalayer& operator = (const datalayer&);

    private:
        hyperdex::coordinatorlink* m_cl;
        volatile bool m_shutdown;
        po6::pathname m_base;
        po6::threads::thread m_optimistic_io_thread;
        std::vector<std::tr1::shared_ptr<po6::threads::thread> > m_flush_threads;
        disk_map_t m_disks;
        std::list<hyperdex::regionid> m_preallocate_rr;
        uint64_t m_last_preallocation;
        std::list<hyperdex::regionid> m_optimistic_rr;
        uint64_t m_last_dose_of_optimism;
        volatile bool m_flushed_recently;

    private:
        // Shutdown and restart.
        static const char* STATE_FILE_NAME;
        static const int STATE_FILE_VER;
        bool m_quiesce;
        std::string m_quiesce_state_id;
        bool dump_state(const hyperdex::configuration& config, const hyperdex::instance& us);
        bool load_state();
};

} // namespace hyperdaemon

#endif // hyperdex_datalayer_h_
