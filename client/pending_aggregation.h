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

#ifndef hyperdex_client_pending_aggregation_h_
#define hyperdex_client_pending_aggregation_h_

// STL
#include <utility>
#include <vector>

// HyperDex
#include "namespace.h"
#include "client/pending.h"

BEGIN_HYPERDEX_NAMESPACE

class pending_aggregation : public pending
{
    public:
        pending_aggregation(uint64_t client_visible_id,
                            hyperdex_client_returncode* status);
        virtual ~pending_aggregation() throw ();

    // handle aggregation across servers; must call handle_* messages from
    // subclasses for this to work
    public:
        bool aggregation_done();

    // events
    public:
        virtual void handle_sent_to(const server_id& si,
                                    const virtual_server_id& vsi);
        virtual void handle_failure(const server_id& si,
                                    const virtual_server_id& vsi);
        // pass NULL for msg; we don't need it
        virtual bool handle_message(client*,
                                    const server_id& si,
                                    const virtual_server_id& vsi,
                                    network_msgtype mt,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up,
                                    hyperdex_client_returncode* status,
                                    e::error* error);

    // refcount
    protected:
        friend class e::intrusive_ptr<pending_aggregation>;

    // noncopyable
    private:
        pending_aggregation(const pending_aggregation& other);
        pending_aggregation& operator = (const pending_aggregation& rhs);

    private:
        virtual void remove(const server_id& si, const virtual_server_id& vsi);

    private:
        std::vector<std::pair<server_id, virtual_server_id> > m_outstanding;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_client_pending_aggregation_h_
