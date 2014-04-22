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
        transfer_out_state(const transfer& xfer);
        ~transfer_out_state() throw ();

    public:
        void debug_dump();

    public:
        transfer xfer;
        po6::threads::mutex mtx;
        uint64_t next_seq_no;
        std::list<e::intrusive_ptr<pending> > window;
        size_t window_sz;
        std::auto_ptr<datalayer::replay_iterator> iter;
        bool handshake_syn; // do we know the other end got a syn?
        bool handshake_ack; // do we know the other end got a ack?
        bool wipe;

    private:
        friend class e::intrusive_ptr<transfer_out_state>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;

    private:
        transfer_out_state(const transfer_out_state&);
        transfer_out_state& operator = (const transfer_out_state&);
};

#endif // hyperdex_daemon_state_transfer_manager_transfer_out_state_h_
