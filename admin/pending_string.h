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

#ifndef hyperdex_admin_pending_string_h_
#define hyperdex_admin_pending_string_h_

// HyperDex
#include "admin/pending.h"

BEGIN_HYPERDEX_NAMESPACE
class admin;

class pending_string : public pending
{
    public:
        pending_string(uint64_t admin_visible_id,
                       hyperdex_admin_returncode* status,
                       hyperdex_admin_returncode set_status,
                       const std::string& string,
                       const char** store);
        virtual ~pending_string() throw ();

    // return to admin
    public:
        virtual bool can_yield();
        virtual bool yield(hyperdex_admin_returncode* status);

    // events
    public:
        virtual void handle_sent_to(const server_id& si);
        virtual void handle_failure(const server_id& si);
        virtual bool handle_message(admin* cl,
                                    const server_id& si,
                                    network_msgtype mt,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up,
                                    hyperdex_admin_returncode* status);

    private:
        pending_string(const pending_string&);
        pending_string& operator = (const pending_string&);

    private:
        hyperdex_admin_returncode m_status;
        std::string m_string;
        const char** m_store;
        bool m_done;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_admin_pending_string_h_
