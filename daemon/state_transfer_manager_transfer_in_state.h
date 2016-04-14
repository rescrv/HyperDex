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

#ifndef hyperdex_daemon_state_transfer_manager_transfer_in_state_h_
#define hyperdex_daemon_state_transfer_manager_transfer_in_state_h_

// e
#include <e/intrusive_ptr.h>

// HyperDex
#include "daemon/state_transfer_manager.h"

class hyperdex::state_transfer_manager::transfer_in_state
{
public:
	transfer_in_state(const transfer &xfer);
	~transfer_in_state() throw ();

public:
	void debug_dump();

public:
	transfer xfer;
	po6::threads::mutex mtx;
	uint64_t upper_bound_acked;
	std::list<e::intrusive_ptr<pending> > queued;
	bool handshake_complete;
	bool wipe;
	bool wiped;

private:
	friend class e::intrusive_ptr<transfer_in_state>;

private:
	void inc() { __sync_add_and_fetch(&m_ref, 1); }
	void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

private:
	size_t m_ref;
};

#endif // hyperdex_daemon_state_transfer_manager_transfer_in_state_h_
