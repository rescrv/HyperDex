// Copyright (c) 2013, Cornell University
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
#include "daemon/replication_manager_key_region.h"
#include "daemon/replication_manager_key_state.h"
#include "daemon/replication_manager_key_state_reference.h"

using hyperdex::replication_manager;

replication_manager :: key_state_reference :: key_state_reference()
    : m_locked(false)
    , m_rm(NULL)
    , m_ks()
{
}

replication_manager :: key_state_reference :: key_state_reference(replication_manager* rm,
                                                                  e::intrusive_ptr<key_state> ks)
    : m_locked(false)
    , m_rm(NULL)
    , m_ks()
{
    lock(rm, ks);
}

replication_manager :: key_state_reference :: ~key_state_reference() throw ()
{
    if (m_locked)
    {
        unlock();
    }
}

void
replication_manager :: key_state_reference :: lock(replication_manager* rm,
                                                   e::intrusive_ptr<key_state> ks)
{
    assert(!m_locked);
    m_locked = true;
    m_rm = rm;
    m_ks = ks;
    m_ks->m_lock.lock();
}

void
replication_manager :: key_state_reference :: unlock()
{
    // so we need to prevent a deadlock with cycle
    // replication_manager::m_key_states_lock -> key_state::m_lock ->
    //
    // To do this, we mark garbage on key_state, release the lock, grab
    // replication_manager::m_key_states_lock and destroy the object.
    // Everyone else will spin without holding a lock on the key state.
    bool we_collect = m_ks->empty() && !m_ks->marked_garbage();

    if (we_collect)
    {
        m_ks->m_marked_garbage = true;
    }

    m_ks->m_lock.unlock();

    if (we_collect)
    {
        e::striped_lock<po6::threads::mutex>::hold hold(&m_rm->m_key_states_locks,
                m_rm->get_lock_num(m_ks->kr().region, m_ks->key()));
        m_rm->m_key_states.remove(m_ks->kr());
    }

    m_rm = NULL;
    m_ks = NULL;
    m_locked = false;
}
