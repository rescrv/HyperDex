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
#include "common/network_returncode.h"
#include "admin/pending_raw_backup.h"

using hyperdex::pending_raw_backup;

pending_raw_backup :: pending_raw_backup(uint64_t id,
                                         hyperdex_admin_returncode* status,
                                         const char** path)
    : pending(id, status)
    , m_path()
    , m_path_c_str(path)
    , m_done(false)
{
}

pending_raw_backup :: ~pending_raw_backup() throw ()
{
}

bool
pending_raw_backup :: can_yield()
{
    return !m_done;
}

bool
pending_raw_backup :: yield(hyperdex_admin_returncode* status)
{
    *status = HYPERDEX_ADMIN_SUCCESS;
    m_done = true;
    return true;
}

void
pending_raw_backup :: handle_sent_to(const server_id&)
{
}

void
pending_raw_backup :: handle_failure(const server_id& si)
{
    YIELDING_ERROR(SERVERERROR) << "communication with " << si << "failed";
}

bool
pending_raw_backup :: handle_message(admin*,
                                     const server_id& si,
                                     network_msgtype mt,
                                     std::auto_ptr<e::buffer> msg,
                                     e::unpacker up,
                                     hyperdex_admin_returncode* status)
{
    *status = HYPERDEX_ADMIN_SUCCESS;

    if (mt != BACKUP)
    {
        YIELDING_ERROR(SERVERERROR) << "server " << si << " responded to BACKUP with " << mt;
        return true;
    }

    uint16_t rt;
    e::slice path;
    up = up >> rt >> path;

    if (up.error())
    {
        YIELDING_ERROR(SERVERERROR) << "communication error: server "
                                    << si << " sent corrupt message="
                                    << msg->as_slice().hex()
                                    << " in response to a BACKUP";
        return true;
    }

    network_returncode rc = static_cast<network_returncode>(rt);

    if (rc != NET_SUCCESS)
    {
        YIELDING_ERROR(SERVERERROR) << "backup of server " << si
                                    << " failed; see the server's log for details";
        return true;
    }

    m_path.assign(reinterpret_cast<const char*>(path.data()), path.size());
    *m_path_c_str = m_path.c_str();
    this->set_status(HYPERDEX_ADMIN_SUCCESS);
    this->set_error(e::error());
    return true;
}
