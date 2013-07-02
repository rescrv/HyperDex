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

// POSIX
#ifndef _MSC_VER
#include <poll.h>
#endif

// e
#include <e/endian.h>

// HyperDex
#include "common/configuration.h"
#include "common/coordinator_returncode.h"
#include "client/coordinator_link.h"
#include "client/hyperspace_builder_internal.h"

using hyperdex::configuration;
using hyperdex::coordinator_link;

coordinator_link :: coordinator_link(const po6::net::hostname& host)
    : m_config()
    , m_repl(host.address.c_str(), host.port)
    , m_need_wait(false)
    , m_wait_config_id(-1)
    , m_wait_config_status(REPLICANT_GARBAGE)
    , m_get_config_id(-1)
    , m_get_config_status(REPLICANT_GARBAGE)
    , m_get_config_output(NULL)
    , m_get_config_output_sz(0)
{
}

coordinator_link :: ~coordinator_link() throw ()
{
}

hyperclient_returncode
coordinator_link :: add_space(hyperspace* space)
{
    hyperdex::space s;

    if (!space_to_space(space, &s))
    {
        return HYPERCLIENT_BADSPACE;
    }

    std::auto_ptr<e::buffer> msg(e::buffer::create(pack_size(s)));
    msg->pack_at(0) << s;
    hyperclient_returncode status;
    const char* output;
    size_t output_sz;

    if (!make_rpc("add-space", reinterpret_cast<const char*>(msg->data()), msg->size(),
                  &status, &output, &output_sz))
    {
        return status;
    }

    status = HYPERCLIENT_SUCCESS;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case hyperdex::COORD_SUCCESS:
                status = HYPERCLIENT_SUCCESS;
                break;
            case hyperdex::COORD_MALFORMED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_DUPLICATE:
                status = HYPERCLIENT_DUPLICATE;
                break;
            case hyperdex::COORD_NOT_FOUND:
                status = HYPERCLIENT_NOTFOUND;
                break;
            case hyperdex::COORD_INITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_UNINITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_TRANSFER_IN_PROGRESS:
                status = HYPERCLIENT_INTERNAL;
                break;
            default:
                status = HYPERCLIENT_INTERNAL;
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return status;
}

hyperclient_returncode
coordinator_link :: rm_space(const char* space)
{
    hyperclient_returncode status;
    const char* output;
    size_t output_sz;

    if (!make_rpc("rm-space", space, strlen(space) + 1,
                  &status, &output, &output_sz))
    {
        return status;
    }

    status = HYPERCLIENT_SUCCESS;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case hyperdex::COORD_SUCCESS:
                status = HYPERCLIENT_SUCCESS;
                break;
            case hyperdex::COORD_MALFORMED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_DUPLICATE:
                status = HYPERCLIENT_DUPLICATE;
                break;
            case hyperdex::COORD_NOT_FOUND:
                status = HYPERCLIENT_NOTFOUND;
                break;
            case hyperdex::COORD_INITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_UNINITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_TRANSFER_IN_PROGRESS:
                status = HYPERCLIENT_INTERNAL;
                break;
            default:
                status = HYPERCLIENT_INTERNAL;
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return status;
}

hyperclient_returncode
coordinator_link :: initialize_cluster(uint64_t cluster, const char* path)
{
    replicant_returncode rstatus;
    const char* errmsg = NULL;
    size_t errmsg_sz = 0;

    int64_t noid = m_repl.new_object("hyperdex", path, &rstatus,
                                     &errmsg, &errmsg_sz);

    if (noid < 0)
    {
        switch (rstatus)
        {
            case REPLICANT_BAD_LIBRARY:
                return HYPERCLIENT_NOTFOUND;
            case REPLICANT_INTERRUPTED:
                return HYPERCLIENT_INTERRUPTED;
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_BACKOFF:
                return HYPERCLIENT_COORDFAIL;
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_TIMEOUT:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                return HYPERCLIENT_INTERNAL;
        }
    }

    replicant_returncode lstatus;
    int64_t lid = m_repl.loop(noid, -1, &lstatus);

    if (lid < 0)
    {
        m_repl.kill(noid);

        switch (lstatus)
        {
            case REPLICANT_INTERRUPTED:
                return HYPERCLIENT_INTERRUPTED;
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_BACKOFF:
                return HYPERCLIENT_COORDFAIL;
            case REPLICANT_TIMEOUT:
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                return HYPERCLIENT_INTERNAL;
        }
    }

    assert(lid == noid);

    switch (rstatus)
    {
        case REPLICANT_SUCCESS:
            break;
        case REPLICANT_INTERRUPTED:
            return HYPERCLIENT_INTERRUPTED;
        case REPLICANT_OBJ_EXIST:
            return HYPERCLIENT_DUPLICATE;
        case REPLICANT_FUNC_NOT_FOUND:
        case REPLICANT_OBJ_NOT_FOUND:
        case REPLICANT_COND_NOT_FOUND:
        case REPLICANT_COND_DESTROYED:
        case REPLICANT_SERVER_ERROR:
        case REPLICANT_NEED_BOOTSTRAP:
        case REPLICANT_MISBEHAVING_SERVER:
            return HYPERCLIENT_COORDFAIL;
        case REPLICANT_NAME_TOO_LONG:
        case REPLICANT_BAD_LIBRARY:
            return HYPERCLIENT_COORD_LOGGED;
        case REPLICANT_TIMEOUT:
        case REPLICANT_BACKOFF:
        case REPLICANT_INTERNAL_ERROR:
        case REPLICANT_NONE_PENDING:
        case REPLICANT_GARBAGE:
        default:
            return HYPERCLIENT_INTERNAL;
    }

    hyperclient_returncode status;
    char data[sizeof(uint64_t)];
    e::pack64be(cluster, data);
    const char* output;
    size_t output_sz;

    if (!make_rpc("initialize", data, sizeof(uint64_t),
                  &status, &output, &output_sz))
    {
        return status;
    }

    status = HYPERCLIENT_SUCCESS;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case hyperdex::COORD_SUCCESS:
                status = HYPERCLIENT_SUCCESS;
                break;
            case hyperdex::COORD_MALFORMED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_DUPLICATE:
                status = HYPERCLIENT_CLUSTER_JUMP;
                break;
            case hyperdex::COORD_NOT_FOUND:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_INITIALIZED:
                status = HYPERCLIENT_DUPLICATE;
                break;
            case hyperdex::COORD_UNINITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_TRANSFER_IN_PROGRESS:
                status = HYPERCLIENT_INTERNAL;
                break;
            default:
                status = HYPERCLIENT_INTERNAL;
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return status;
}

hyperclient_returncode
coordinator_link :: show_config(std::ostream& out)
{
    m_config = configuration();
    hyperclient_returncode status;

    if (!wait_for_config(&status))
    {
        return status;
    }

    m_config.dump(out);
    return HYPERCLIENT_SUCCESS;
}

hyperclient_returncode
coordinator_link :: initiate_transfer(uint64_t region_id, uint64_t server_id)
{
    hyperclient_returncode status;
    char data[2 * sizeof(uint64_t)];
    e::pack64be(region_id, data);
    e::pack64be(server_id, data + sizeof(uint64_t));
    const char* output;
    size_t output_sz;

    if (!make_rpc("xfer-begin", data, 2 * sizeof(uint64_t),
                  &status, &output, &output_sz))
    {
        return status;
    }

    status = HYPERCLIENT_SUCCESS;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case hyperdex::COORD_SUCCESS:
                status = HYPERCLIENT_SUCCESS;
                break;
            case hyperdex::COORD_MALFORMED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_DUPLICATE:
                status = HYPERCLIENT_DUPLICATE;
                break;
            case hyperdex::COORD_NOT_FOUND:
                status = HYPERCLIENT_NOTFOUND;
                break;
            case hyperdex::COORD_INITIALIZED:
                status = HYPERCLIENT_INTERNAL;
                break;
            case hyperdex::COORD_UNINITIALIZED:
                status = HYPERCLIENT_COORDFAIL;
                break;
            case hyperdex::COORD_TRANSFER_IN_PROGRESS:
                status = HYPERCLIENT_DUPLICATE;
                break;
            default:
                status = HYPERCLIENT_INTERNAL;
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return status;
}

const configuration&
coordinator_link :: config()
{
    return m_config;
}

// wait_for_config returns true iff there is a new config and returns false
// otherwise, specifying why the error occurred.
bool
coordinator_link :: wait_for_config(hyperclient_returncode* status)
{
    uint64_t version = m_config.version();

    if (!poll_for_config(status))
    {
        return false;
    }

    while (version >= m_config.version())
    {
#ifdef _MSC_VER
	//XXX: There can be at most 64 fds in the set
        pollfd pfd[64];
	    int fd_count = m_repl.poll_fd()->fd_count;

	    for(int i = 0; i < fd_count; ++i)
	    {
	        pfd[i].fd=m_repl.poll_fd()->fd_array[i];
            pfd[i].events = POLLIN;
            pfd[i].revents = 0;
        }

        if (WSAPoll(pfd, fd_count, -1) <= 0)
#else
        pollfd pfd;
        pfd.fd = m_repl.poll_fd();
        pfd.events = POLLIN;
        pfd.revents = 0;

        if (poll(&pfd, 1, -1) <= 0)
#endif
        {
            *status = HYPERCLIENT_POLLFAILED;
            return false;
        }

        if (!poll_for_config(status))
        {
            return false;
        }
    }

    return true;
}

bool
coordinator_link :: poll_for_config(hyperclient_returncode* status)
{
    if (m_wait_config_id < 0 && m_get_config_id < 0)
    {
        if (!initiate_wait_for_config(status))
        {
            return false;
        }
    }

    replicant_returncode lstatus = REPLICANT_GARBAGE;
    int64_t lid = m_repl.loop(0, &lstatus);

    if (lid < 0)
    {
        switch (lstatus)
        {
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
                *status = HYPERCLIENT_COORDFAIL;
                return false;
            case REPLICANT_TIMEOUT:
            case REPLICANT_BACKOFF:
                return true;
            case REPLICANT_INTERRUPTED:
                *status = HYPERCLIENT_INTERRUPTED;
                return false;
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                *status = HYPERCLIENT_INTERNAL;
                return false;
        }

        abort();
    }

    if (lid == m_wait_config_id)
    {
        m_wait_config_id = -1;

        switch (m_wait_config_status)
        {
            case REPLICANT_SUCCESS:
                break;
            case REPLICANT_INTERRUPTED:
                *status = HYPERCLIENT_INTERRUPTED;
                return false;
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
                *status = HYPERCLIENT_COORDFAIL;
                return false;
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_TIMEOUT:
            case REPLICANT_BACKOFF:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                *status = HYPERCLIENT_INTERNAL;
                return false;
        }

        return initiate_get_config(status);
    }
    else if (lid == m_get_config_id)
    {
        m_get_config_id = -1;

        switch (m_get_config_status)
        {
            case REPLICANT_SUCCESS:
                break;
            case REPLICANT_INTERRUPTED:
                *status = HYPERCLIENT_INTERRUPTED;
                return false;
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
                *status = HYPERCLIENT_COORDFAIL;
                return false;
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_TIMEOUT:
            case REPLICANT_BACKOFF:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                *status = HYPERCLIENT_INTERNAL;
                return false;
        }

        e::unpacker up(m_get_config_output, m_get_config_output_sz);
        configuration new_config;
        up = up >> new_config;
        replicant_destroy_output(m_get_config_output, m_get_config_output_sz);

        if (up.error())
        {
            *status = HYPERCLIENT_BADCONFIG;
            return false;
        }

        if (m_config.cluster() != 0 &&
            m_config.cluster() != new_config.cluster())
        {
            *status = HYPERCLIENT_CLUSTER_JUMP;
            return false;
        }

        m_config = new_config;
        return initiate_wait_for_config(status);
    }
    else
    {
        if (m_wait_config_id >= 0)
        {
            m_repl.kill(m_wait_config_id);
        }

        m_wait_config_id = -1;
        return initiate_wait_for_config(status);
    }
}

#ifdef _MSC_VER
fd_set*
#else
int
#endif
coordinator_link :: poll_fd()
{
    return m_repl.poll_fd();
}

bool
coordinator_link :: make_rpc(const char* func,
                             const char* data, size_t data_sz,
                             hyperclient_returncode* status,
                             const char** output, size_t* output_sz)
{
    replicant_returncode sstatus;
    int64_t sid = m_repl.send("hyperdex", func, data, data_sz,
                              &sstatus, output, output_sz);

    if (sid < 0)
    {
        switch (sstatus)
        {
            case REPLICANT_INTERRUPTED:
                *status = HYPERCLIENT_INTERRUPTED;
                return false;
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
                *status = HYPERCLIENT_COORDFAIL;
                return false;
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_TIMEOUT:
            case REPLICANT_BACKOFF:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                *status = HYPERCLIENT_INTERNAL;
                return false;
        }
    }

    replicant_returncode lstatus;
    int64_t lid = m_repl.loop(sid, -1, &lstatus);

    if (lid < 0)
    {
        m_repl.kill(sid);

        switch (lstatus)
        {
            case REPLICANT_INTERRUPTED:
                *status = HYPERCLIENT_INTERRUPTED;
                return false;
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_TIMEOUT:
            case REPLICANT_BACKOFF:
                *status = HYPERCLIENT_COORDFAIL;
                return false;
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                *status = HYPERCLIENT_INTERNAL;
                return false;
        }
    }

    switch (sstatus)
    {
        case REPLICANT_SUCCESS:
            return true;
        case REPLICANT_INTERRUPTED:
            *status = HYPERCLIENT_INTERRUPTED;
            return false;
        case REPLICANT_FUNC_NOT_FOUND:
        case REPLICANT_OBJ_NOT_FOUND:
        case REPLICANT_COND_NOT_FOUND:
        case REPLICANT_COND_DESTROYED:
        case REPLICANT_SERVER_ERROR:
        case REPLICANT_NEED_BOOTSTRAP:
        case REPLICANT_MISBEHAVING_SERVER:
            *status = HYPERCLIENT_COORDFAIL;
            return false;
        case REPLICANT_NAME_TOO_LONG:
        case REPLICANT_OBJ_EXIST:
        case REPLICANT_BAD_LIBRARY:
        case REPLICANT_TIMEOUT:
        case REPLICANT_BACKOFF:
        case REPLICANT_INTERNAL_ERROR:
        case REPLICANT_NONE_PENDING:
        case REPLICANT_GARBAGE:
        default:
            *status = HYPERCLIENT_INTERNAL;
            return false;
    }
}

bool
coordinator_link :: initiate_wait_for_config(hyperclient_returncode* status)
{
    m_wait_config_id = m_repl.wait("hyperdex", "config", m_config.version(), &m_wait_config_status);

    if (m_wait_config_id < 0)
    {
        switch (m_wait_config_status)
        {
            case REPLICANT_INTERRUPTED:
                *status = HYPERCLIENT_INTERRUPTED;
                return false;
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_BACKOFF:
                *status = HYPERCLIENT_COORDFAIL;
                return false;
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_TIMEOUT:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                *status = HYPERCLIENT_INTERNAL;
                return false;
        }

        abort();
    }

    return true;
}

bool
coordinator_link :: initiate_get_config(hyperclient_returncode* status)
{
    m_get_config_id = m_repl.send("hyperdex", "get-config", "", 0,
                                  &m_get_config_status,
                                  &m_get_config_output,
                                  &m_get_config_output_sz);

    if (m_get_config_id < 0)
    {
        switch (m_get_config_status)
        {
            case REPLICANT_INTERRUPTED:
                *status = HYPERCLIENT_INTERRUPTED;
                return false;
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_BACKOFF:
                *status = HYPERCLIENT_COORDFAIL;
                return false;
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_TIMEOUT:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                *status = HYPERCLIENT_INTERNAL;
                return false;
        }

        abort();
    }

    return true;
}
