// Copyright (c) 2011-2012, Cornell University
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

#ifndef hyperclient_pending_h_
#define hyperclient_pending_h_

// HyperDex
#include "hyperdex/hyperdex/ids.h"
#include "hyperdex/hyperdex/instance.h"
#include "hyperdex/hyperdex/network_constants.h"

// HyperClient
#include "hyperclient/hyperclient.h"

class hyperclient::pending
{
    public:
        pending(hyperclient_returncode* status);
        virtual ~pending() throw ();

    public:
        int64_t client_visible_id() const { return m_id; }
        int64_t server_visible_nonce() const { return m_nonce; }
        const hyperdex::entityid& entity() const { return m_ent; }
        const hyperdex::instance& instance() const { return m_inst; }

    public:
        void set_client_visible_id(uint64_t _id) { m_id = _id; }
        void set_server_visible_nonce(uint64_t _nonce) { m_nonce = _nonce; }
        void set_entity(hyperdex::entityid ent) { m_ent = ent; }
        void set_instance(hyperdex::instance inst) { m_inst = inst; }
        void set_status(hyperclient_returncode status) { *m_status = status; }

    public:
        virtual hyperdex::network_msgtype request_type() = 0;
        virtual int64_t handle_response(hyperclient* cl,
                                        const po6::net::location& sender,
                                        std::auto_ptr<e::buffer> msg,
                                        hyperdex::network_msgtype type,
                                        hyperclient_returncode* status) = 0;

    private:
        friend class e::intrusive_ptr<pending>;

    private:
        pending(const pending& other);

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        pending& operator = (const pending& rhs);

    private:
        size_t m_ref;
        int64_t m_id;
        int64_t m_nonce;
        hyperdex::entityid m_ent;
        hyperdex::instance m_inst;
        hyperclient_returncode* m_status;
};

#endif // hyperclient_pending_h_
