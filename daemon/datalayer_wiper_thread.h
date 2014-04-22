// Copyright (c) 2014, Cornell University
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

#ifndef hyperdex_daemon_datalayer_wiper_thread_h_
#define hyperdex_daemon_datalayer_wiper_thread_h_

// HyperDex
#include "daemon/background_thread.h"
#include "daemon/datalayer.h"
#include "daemon/datalayer_wiper_indexer_mediator.h"

class hyperdex::datalayer::wiper_thread : public hyperdex::background_thread
{
    public:
        wiper_thread(daemon* d, wiper_indexer_mediator* m);
        ~wiper_thread() throw ();

    public:
        virtual const char* thread_name();
        virtual bool have_work();
        virtual void copy_work();
        virtual void do_work();

    public:
        void debug_dump();
        // These are different than pause/unpause, which should only ever be
        // called by the main thread.  They guarantee that after the wiper is
        // done with the current region, it will not make progress to the next
        // region.  Thus, between pairs of inhibit/permit,
        // "region_will_be_wiped" is guaranteed not to go from true->false.
        void inhibit_wiping();
        void permit_wiping();
        bool region_will_be_wiped(region_id rid);
        void request_wipe(transfer_id xid,
                          region_id ri);
        void kick();

    private:
        bool interrupted();
        void wipe(transfer_id xid, region_id rid);
        void wipe_checkpoints(region_id rid);
        void wipe_indices(region_id rid);
        void wipe_objects(region_id rid);
        void wipe_common(uint8_t c, region_id rid);

    private:
        daemon* m_daemon;
        wiper_indexer_mediator* m_mediator;
        typedef std::list<std::pair<transfer_id, region_id> > wipe_list_t;
        wipe_list_t m_wiping;
        bool m_have_current;
        transfer_id m_wipe_current_xid;
        region_id m_wipe_current_rid;
        uint64_t m_wiping_inhibit_permit_diff;
        uint64_t m_interrupted_count;
        bool m_interrupted;

    private:
        wiper_thread(const wiper_thread&);
        wiper_thread& operator = (const wiper_thread&);
};

#endif // hyperdex_daemon_datalayer_wiper_thread_h_
