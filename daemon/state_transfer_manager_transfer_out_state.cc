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

#define __STDC_LIMIT_MACROS

// Google Log
#include <glog/logging.h>

// HyperDex
#include "daemon/datalayer_iterator.h"
#include "daemon/state_transfer_manager_pending.h"
#include "daemon/state_transfer_manager_transfer_out_state.h"

using hyperdex::state_transfer_manager;

state_transfer_manager :: transfer_out_state :: transfer_out_state(const transfer& _xfer)
    : xfer(_xfer)
    , mtx()
    , next_seq_no(1)
    , window()
    , window_sz(1)
    , iter()
    , handshake_syn(false)
    , handshake_ack(false)
    , wipe(false)
    , m_ref(0)
{
}

state_transfer_manager :: transfer_out_state :: ~transfer_out_state() throw ()
{
}

void
state_transfer_manager :: transfer_out_state :: debug_dump()
{
    po6::threads::mutex::hold hold(&mtx);
    LOG(INFO) << "  transfer=" << xfer;
    LOG(INFO) << "    next_seq_no=" << next_seq_no;
    LOG(INFO) << "    window_sz=" << window_sz;
    LOG(INFO) << "    handshake_syn=" << handshake_syn;
    LOG(INFO) << "    handshake_ack=" << handshake_ack;
    LOG(INFO) << "    wipe=" << wipe;
}
