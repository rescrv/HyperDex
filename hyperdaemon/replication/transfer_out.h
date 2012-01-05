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

#ifndef hyperdaemon_replication_transfer_out_h_
#define hyperdaemon_replication_transfer_out_h_

// HyperDisk
#include "hyperdisk/hyperdisk/disk.h"
#include "hyperdisk/hyperdisk/snapshot.h"

namespace hyperdaemon
{
namespace replication
{

class transfer_out
{
    public:
        transfer_out(const hyperdex::entityid& from,
                     uint16_t xfer_id,
                     e::intrusive_ptr<hyperdisk::rolling_snapshot> s);

    public:
        po6::threads::mutex lock;
        e::intrusive_ptr<hyperdisk::rolling_snapshot> snap;
        uint64_t xfer_num;
        const hyperdex::entityid replicate_from;
        const hyperdex::entityid transfer_entity;
        bool failed;

    private:
        friend class e::intrusive_ptr<transfer_out>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
};


inline
transfer_out :: transfer_out(const hyperdex::entityid& from,
                             uint16_t xfer_id,
                             e::intrusive_ptr<hyperdisk::rolling_snapshot> s)
    : lock()
    , snap(s)
    , xfer_num(1)
    , replicate_from(from)
    , transfer_entity(std::numeric_limits<uint32_t>::max() - 1, xfer_id, 0, 0, 0)
    , failed(false)
    , m_ref(0)
{
}

} // namespace replication
} // namespace hyperdaemon

#endif // hyperdaemon_replication_transfer_out_h_
