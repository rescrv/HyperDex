// Copyright (c) 2011-2013, Cornell University
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

#ifndef hyperdex_client_pending_h_
#define hyperdex_client_pending_h_

// STL
#include <memory>

// e
#include <e/error.h>
#include <e/intrusive_ptr.h>

// HyperDex
#include <hyperdex/client.h>
#include "namespace.h"
#include "common/configuration.h"
#include "common/ids.h"
#include "common/network_msgtype.h"

BEGIN_HYPERDEX_NAMESPACE
class client;

class pending
{
    public:
        pending(uint64_t client_visible_id,
                hyperdex_client_returncode* status);
        virtual ~pending() throw ();

    public:
        int64_t client_visible_id() const { return m_client_visible_id; }
        void set_status(hyperdex_client_returncode status) { *m_status = status; }
        e::error error() const { return m_error; }

    // return to client
    public:
        virtual bool can_yield() = 0;
        virtual bool yield(hyperdex_client_returncode* status, e::error* error) = 0;

    // events
    public:
        virtual void handle_sent_to(const server_id& si,
                                    const virtual_server_id& vsi) = 0;
        virtual void handle_failure(const server_id& si,
                                    const virtual_server_id& vsi) = 0;
        virtual bool handle_message(client* cl,
                                    const server_id& si,
                                    const virtual_server_id& vsi,
                                    network_msgtype mt,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up,
                                    hyperdex_client_returncode* status,
                                    e::error* error) = 0;

    // refcount
    protected:
        friend class e::intrusive_ptr<pending>;
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }
        size_t m_ref;

    protected:
        std::ostream& error(const char* file, size_t line);
        void set_error(const e::error& err);

    // noncopyable
    private:
        pending(const pending& other);
        pending& operator = (const pending& rhs);

    // operation state
    private:
        int64_t m_client_visible_id;
        hyperdex_client_returncode* m_status;
        e::error m_error;
};

#define PENDING_ERROR(CODE) \
    this->set_status(HYPERDEX_CLIENT_ ## CODE); \
    this->error(__FILE__, __LINE__)

END_HYPERDEX_NAMESPACE

#endif // hyperdex_client_pending_h_
