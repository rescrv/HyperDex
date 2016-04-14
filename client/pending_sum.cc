// Copyright (c) 2012-2013, Cornell University
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
#include "client/pending_sum.h"
#include "common/datatype_info.h"

using hyperdex::pending_sum;

pending_sum :: pending_sum(uint64_t id,
                           uint16_t sum_idx,
                           datatype_info *sum_di,
                           hyperdex_client_returncode *status,
                           uint64_t *count)
	: pending_aggregation(id, status)
	, m_sum_idx(sum_idx)
	, m_sum_di(sum_di)
	, m_sum(count)
	, m_done(false)
{
	set_status(HYPERDEX_CLIENT_SUCCESS);
	set_error(e::error());
}

pending_sum :: ~pending_sum() throw ()
{
}

bool
pending_sum :: can_yield()
{
	return this->aggregation_done() && !m_done;
}

bool
pending_sum :: yield(hyperdex_client_returncode *status, e::error *err)
{
	*status = HYPERDEX_CLIENT_SUCCESS;
	*err = e::error();
	assert(this->can_yield());
	m_done = true;
	return true;
}

void
pending_sum :: handle_failure(const server_id &si,
                              const virtual_server_id &vsi)
{
	PENDING_ERROR(RECONFIGURE) << "reconfiguration affecting "
	                           << vsi << "/" << si;
	return pending_aggregation::handle_failure(si, vsi);
}

bool
pending_sum :: handle_message(client *cl,
                              const server_id &si,
                              const virtual_server_id &vsi,
                              network_msgtype mt,
                              std::auto_ptr<e::buffer> msg,
                              e::unpacker up,
                              hyperdex_client_returncode *status,
                              e::error *err)
{
	bool handled = pending_aggregation::handle_message(cl, si, vsi, mt, std::auto_ptr<e::buffer>(), up, status, err);
	assert(handled);
	*status = HYPERDEX_CLIENT_SUCCESS;
	*err = e::error();
	if (mt != RESP_SUM)
	{
		PENDING_ERROR(SERVERERROR) << "server " << vsi << " responded to SUM with " << mt;
		return true;
	}
	uint64_t local_sum;
	up = up >> local_sum;
	if (up.error())
	{
		PENDING_ERROR(SERVERERROR) << "communication error: server "
		                           << vsi << " sent corrupt message="
		                           << msg->as_slice().hex()
		                           << " in response to a SUM";
		return true;
	}
	*m_sum += local_sum;
	// Don't set the status or error so that errors will carry through.  It was
	// set to the success state in the constructor
	return true;
}
