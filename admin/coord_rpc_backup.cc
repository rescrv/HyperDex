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

// POSIX
#include <sys/types.h>
#include <sys/stat.h>

// po6
#include <po6/io/fd.h>

// e
#include <e/strescape.h>

// HyperDex
#include "admin/admin.h"
#include "admin/coord_rpc_backup.h"

using hyperdex::coord_rpc_backup;

coord_rpc_backup :: coord_rpc_backup(uint64_t id,
                                     hyperdex_admin_returncode* status,
                                     const char* path)
    : coord_rpc(id, status)
    , m_path(path)
    , m_done(false)
{
}

coord_rpc_backup :: ~coord_rpc_backup() throw ()
{
}

bool
coord_rpc_backup :: can_yield()
{
    return !m_done;
}

bool
coord_rpc_backup :: yield(hyperdex_admin_returncode* status)
{
    assert(this->can_yield());
    m_done = true;
    *status = HYPERDEX_ADMIN_SUCCESS;
    return true;
}

bool
coord_rpc_backup :: handle_response(admin* adm,
                                    hyperdex_admin_returncode* status)
{
    *status = HYPERDEX_ADMIN_SUCCESS;
    hyperdex_admin_returncode resp_status;
    e::error err;
    adm->interpret_replicant_returncode(repl_status, &resp_status, &err);
    set_status(resp_status);
    set_error(err);

    if (resp_status != HYPERDEX_ADMIN_SUCCESS)
    {
        return true;
    }

    po6::io::fd fd(open(m_path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR));

    if (fd.get() < 0)
    {
        YIELDING_ERROR(LOCALERROR) << "could not open coordinator backup file \""
                                   << e::strescape(m_path) << "\": " << strerror(errno);
        return true;
    }

    if (fd.xwrite(repl_output, repl_output_sz) != static_cast<ssize_t>(repl_output_sz))
    {
        YIELDING_ERROR(LOCALERROR) << "could not write coordinator backup file \""
                                   << e::strescape(m_path) << "\": " << strerror(errno);
        return true;
    }

    return true;
}
