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
#include <map>
#include <set>
#include <tr1/memory>
#include <vector>

// po6
#include <po6/threads/rwlock.h>
#include <po6/threads/thread.h>

// e
#include <e/buffer.h>

// HyperDisk
#include <hyperdisk/disk.h>
#include <hyperdisk/returncode.h>

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
        e::intrusive_ptr<hyperdisk::snapshot> make_snapshot(const hyperdex::regionid& ri);
        e::intrusive_ptr<hyperdisk::rolling_snapshot> make_rolling_snapshot(const hyperdex::regionid& ri);
        // Push data towards the disk.
        void trickle(const hyperdex::regionid& ri);

    // Key-Value store operations.
    public:
        // May return SUCCESS, NOTFOUND or MISSINGDISK.
        hyperdisk::returncode get(const hyperdex::regionid& ri, const e::buffer& key,
                                  std::vector<e::buffer>* value, uint64_t* version);
        // May return SUCCESS, WRONGARITY or MISSINGDISK.
        hyperdisk::returncode put(const hyperdex::regionid& ri, const e::buffer& key,
                                  const std::vector<e::buffer>& value, uint64_t version);
        // May return SUCCESS or MISSINGDISK.
        hyperdisk::returncode del(const hyperdex::regionid& ri, const e::buffer& key);

    private:
        datalayer(const datalayer&);

    private:
        e::intrusive_ptr<hyperdisk::disk> get_region(const hyperdex::regionid& ri);
        void flush_loop();
        void create_disk(const hyperdex::regionid& ri, uint16_t num_columns);
        void drop_disk(const hyperdex::regionid& ri);

    private:
        datalayer& operator = (const datalayer&);

    private:
        hyperdex::coordinatorlink* m_cl;
        bool m_shutdown;
        po6::pathname m_base;
        po6::threads::thread m_flusher;
        po6::threads::rwlock m_lock;
        std::map<hyperdex::regionid, e::intrusive_ptr<hyperdisk::disk> > m_disks;
};

} // namespace hyperdaemon

#endif // hyperdex_datalayer_h_
