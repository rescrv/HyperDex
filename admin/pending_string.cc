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
#include "admin/pending_string.h"

using hyperdex::pending_string;

pending_string :: pending_string(uint64_t id,
                                 hyperdex_admin_returncode* status,
                                 hyperdex_admin_returncode _status,
                                 const std::string& string,
                                 const char** store)
    : pending(id, status)
    , m_status(_status)
    , m_string(string)
    , m_store(store)
    , m_done(false)
{
}

pending_string :: ~pending_string() throw ()
{
}

bool
pending_string :: can_yield()
{
    return !m_done;
}

bool
pending_string :: yield(hyperdex_admin_returncode* status)
{
    assert(this->can_yield());
    m_done = true;
    *status = HYPERDEX_ADMIN_SUCCESS;
    set_status(m_status);
    *m_store = m_string.c_str();
    return true;
}

void
pending_string :: handle_sent_to(const server_id&)
{
    abort();
}

void
pending_string :: handle_failure(const server_id&)
{
    abort();
}

bool
pending_string :: handle_message(admin*,
                                 const server_id&,
                                 network_msgtype,
                                 std::auto_ptr<e::buffer>,
                                 e::unpacker,
                                 hyperdex_admin_returncode*)
{
    abort();
}
