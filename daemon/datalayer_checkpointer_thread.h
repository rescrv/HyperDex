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

#ifndef hyperdex_daemon_datalayer_checkpointer_h_
#define hyperdex_daemon_datalayer_checkpointer_h_

// HyperDex
#include "daemon/background_thread.h"
#include "daemon/datalayer.h"
#include "daemon/datalayer_encodings.h"

class hyperdex::datalayer::checkpointer_thread : public hyperdex::background_thread
{
    public:
        checkpointer_thread(daemon* d);
        ~checkpointer_thread() throw ();

    public:
        virtual const char* thread_name();
        virtual bool have_work();
        virtual void copy_work();
        virtual void do_work();

    public:
        void debug_dump();
        // these are different than pause/unpause, which should only ever be
        // called by the main thread.
        void inhibit_gc();
        void permit_gc();
        void set_checkpoint_gc(uint64_t checkpoint_gc);

    private:
        void collect_lower_checkpoints(uint64_t checkpoint_gc);

    private:
        daemon* m_daemon;
        uint64_t m_checkpoint_gc; // under lock
        uint64_t m_checkpoint_gced;
        uint64_t m_checkpoint_target; // do_work; no lock; copy of _gc
        uint64_t m_gc_inhibit_permit_diff;

    private:
        checkpointer_thread(const checkpointer_thread&);
        checkpointer_thread& operator = (const checkpointer_thread&);
};

#endif // hyperdex_daemon_datalayer_checkpointer_h_
