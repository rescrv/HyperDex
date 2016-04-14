// Copyright (c) 2012-2016, Cornell University
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

#ifndef hyperdex_client_pending_sum_h_
#define hyperdex_client_pending_sum_h_

// HyperDex
#include "namespace.h"
#include "client/pending_aggregation.h"

BEGIN_HYPERDEX_NAMESPACE

class datatype_info;
class pending_sum : public pending_aggregation
{
public:
	pending_sum(uint64_t client_visible_id,
	            uint16_t sum_idx,
	            datatype_info *sum_di,
	            hyperdex_client_returncode *status,
	            uint64_t *count);
	virtual ~pending_sum() throw ();

	// return to client
public:
	virtual bool can_yield();
	virtual bool yield(hyperdex_client_returncode *status, e::error *error);

	// events
public:
	virtual void handle_failure(const server_id &si,
	                            const virtual_server_id &vsi);
	virtual bool handle_message(client *,
	                            const server_id &si,
	                            const virtual_server_id &vsi,
	                            network_msgtype mt,
	                            std::auto_ptr<e::buffer> msg,
	                            e::unpacker up,
	                            hyperdex_client_returncode *status,
	                            e::error *error);

	// noncopyable
private:
	pending_sum(const pending_sum &other);
	pending_sum &operator = (const pending_sum &rhs);

private:
	const uint16_t m_sum_idx;
	datatype_info *m_sum_di;
	uint64_t *m_sum;
	bool m_done;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_client_pending_sum_h_
