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

// HyperDex
#include "daemon/datalayer_iterator.h"
#include "daemon/migration_out_state.h"
#include "daemon/migration_manager_pending.h"

using hyperdex::migration_manager;

migration_manager :: migration_out_state :: migration_out_state()
    : mtx()
    , id(1)
    , next_seq_no(1)
    , window()
    , window_sz(1)
    , iter()
    , mid()
    , sid()
    , rid()
    , m_ref(0)
{
}

migration_manager :: migration_out_state :: migration_out_state(migration_id _mid,
        space_id _sid, uint64_t _id, region_id _rid, std::auto_ptr<datalayer::iterator> _iter)
    : mtx()
    , id(_id)
    , next_seq_no(1)
    , window()
    , window_sz(1)
    , iter(_iter)
    , mid(_mid)
    , sid(_sid)
    , rid(_rid)
    , m_ref(0)
{
}

migration_manager :: migration_out_state :: ~migration_out_state() throw ()
{
}
