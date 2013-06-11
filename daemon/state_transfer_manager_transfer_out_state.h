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

#ifndef hyperdex_daemon_state_transfer_manager_transfer_out_state_h_
#define hyperdex_daemon_state_transfer_manager_transfer_out_state_h_

// STL
#include <list>
#include <tr1/memory>

// po6
#include <po6/threads/mutex.h>

// e
#include <e/intrusive_ptr.h>

// HyperDex
#include "daemon/datalayer.h"
#include "daemon/state_transfer_manager.h"

using hyperdex::state_transfer_manager;

class state_transfer_manager::transfer_out_state
{
    public:
        transfer_out_state(const transfer& xfer,
                           datalayer* data,
                           datalayer::snapshot snap);
        ~transfer_out_state() throw ();

    public:
        transfer xfer;
        po6::threads::mutex mtx;
        enum { SNAPSHOT_TRANSFER, LOG_TRANSFER } state;
        uint64_t next_seq_no;
        std::list<e::intrusive_ptr<pending> > window;
        size_t window_sz;
        // transfer from the snapshot
        e::intrusive_ptr<datalayer::iterator> iter;
        // transfer from the log of new operations
        uint64_t log_seq_no;

    private:
        friend class e::intrusive_ptr<transfer_out_state>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
};

#endif // hyperdex_daemon_state_transfer_manager_transfer_out_state_h_
