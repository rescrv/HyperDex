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

// STL
#include <algorithm>

// HyperDex
#include "admin/admin.h"
#include "admin/backup_state_machine.h"

using hyperdex::backup_state_machine;

backup_state_machine :: backup_state_machine(const char* name,
                                             uint64_t id,
                                             hyperdex_admin_returncode* status,
                                             const char** backups)
    : multi_yieldable(id, status)
    , m_name(name)
    , m_state(INITIALIZED)
    , m_nested_id()
    , m_nested_rc()
    , m_configuration_version(0)
    , m_servers()
    , m_backup(NULL)
    , m_backups()
    , m_backups_str()
    , m_backups_c_str(backups)
{
}

backup_state_machine :: ~backup_state_machine() throw ()
{
}

bool
backup_state_machine :: can_yield()
{
    return m_state == DONE || m_state == ERROR;
}

bool
backup_state_machine :: yield(hyperdex_admin_returncode* status)
{
    assert(this->can_yield());
    *status = HYPERDEX_ADMIN_SUCCESS;
    m_state = YIELDED;
    m_backups_str = m_backups.str();
    *m_backups_c_str = m_backups_str.c_str();
    return true;
}

bool
backup_state_machine :: initialize(admin* adm, hyperdex_admin_returncode* status)
{
    *status = HYPERDEX_ADMIN_SUCCESS;
    m_nested_id = adm->read_only(1, &m_nested_rc);

    if (!check_nested(adm))
    {
        *status = m_nested_rc;
        return false;
    }

    adm->m_multi_ops[m_nested_id] = this;
    m_state = SET_READ_ONLY;
    return true;
}

bool
backup_state_machine :: callback(admin* adm, int64_t id, hyperdex_admin_returncode* status)
{
    *status = HYPERDEX_ADMIN_SUCCESS;

    switch (m_state)
    {
        case INITIALIZED:
            callback_unexpected(adm, id);
            return true;
        case SET_READ_ONLY:
            callback_set_read_only(adm, id);
            return true;
        case WAIT_TO_QUIESCE:
            callback_wait_to_quiesce(adm, id);
            return true;
        case DAEMON_BACKUP:
            callback_daemon_backup(adm, id);
            return true;
        case COORD_BACKUP:
            callback_coord_backup(adm, id);
            return true;
        case WAIT_TO_QUIESCE_AGAIN:
            callback_wait_to_quiesce_again(adm, id);
            return true;
        case SET_READ_WRITE:
            callback_set_read_write(adm, id);
            return true;
        case DONE:
            callback_unexpected(adm, id);
            return true;
        case BACKOUT:
            callback_backout(adm, id);
            return true;
        case ERROR:
            callback_unexpected(adm, id);
            return true;
        case YIELDED:
            callback_unexpected(adm, id);
            return true;
        default:
            YIELDING_ERROR(INTERNAL) << "wandered into undefined state";
            return true;
    }
}

void
backup_state_machine :: backout(admin* adm)
{
    m_nested_id = adm->read_only(0, &m_nested_rc);

    if (m_nested_id < 0)
    {
        m_state = ERROR;
    }
    else
    {
        adm->m_multi_ops[m_nested_id] = this;
    }
}

bool
backup_state_machine :: common_callback(admin* adm, int64_t id)
{
    bool ret = true;

    if (m_state == ERROR || m_state == YIELDED)
    {
        ret = false;
    }
    else if (m_nested_id != id)
    {
        YIELDING_ERROR(INTERNAL) << "callback had id=" << id
                                 << " when it should have id=" << m_nested_id;
        m_state = m_state > SET_READ_ONLY ? BACKOUT : ERROR;
        ret = false;
    }
    else if (m_nested_rc != HYPERDEX_ADMIN_SUCCESS)
    {
        this->set_status(m_nested_rc);
        this->set_error(adm->m_last_error);
        m_state = m_state > SET_READ_ONLY ? BACKOUT : ERROR;
        ret = false;
    }

    if (!ret && m_state == BACKOUT)
    {
        backout(adm);
    }

    return ret;
}

bool
backup_state_machine :: check_nested(admin* adm)
{
    if (m_nested_id >= 0)
    {
        return true;
    }

    this->set_status(m_nested_rc);
    this->set_error(adm->m_last_error);
    m_state = m_state >= SET_READ_ONLY ? BACKOUT : ERROR;

    if (m_state == BACKOUT)
    {
        backout(adm);
    }

    return false;
}

void
backup_state_machine :: callback_unexpected(admin*, int64_t)
{
    YIELDING_ERROR(INTERNAL) << "wandered into undefined state";
    m_state = ERROR;
}

void
backup_state_machine :: callback_set_read_only(admin* adm, int64_t id)
{
    if (!common_callback(adm, id))
    {
        return;
    }

    m_nested_id = adm->wait_until_stable(&m_nested_rc);

    if (!check_nested(adm))
    {
        return;
    }

    adm->m_multi_ops[m_nested_id] = this;
    m_state = WAIT_TO_QUIESCE;
}

void
backup_state_machine :: callback_wait_to_quiesce(admin* adm, int64_t id)
{
    if (!common_callback(adm, id))
    {
        return;
    }

    // at this point we know that:
    // 1. the cluster is in read-only mode
    // 2. every write initiated before setting it to read-only mode is complete
    m_configuration_version = adm->m_config.version();

    // now figure out the servers to take a backup on
    adm->m_config.get_all_addresses(&m_servers);
    std::sort(m_servers.rbegin(), m_servers.rend());
    return callback_daemon_backup(adm, id);
}

void
backup_state_machine :: callback_daemon_backup(admin* adm, int64_t id)
{
    if (!common_callback(adm, id))
    {
        return;
    }

    if (m_backup)
    {
        m_backups << m_backup << "\n";
        m_backup = NULL;
    }

    if (m_servers.empty())
    {
        std::string path = m_name + ".coordinator.bin";
        m_nested_id = adm->coord_backup(path.c_str(), &m_nested_rc);

        if (!check_nested(adm))
        {
            return;
        }

        adm->m_multi_ops[m_nested_id] = this;
        m_state = COORD_BACKUP;
        return;
    }

    server_id sid = m_servers.back().first;
    po6::net::location loc = m_servers.back().second;
    m_servers.pop_back();
    m_nested_id = adm->raw_backup(sid, m_name.c_str(), &m_nested_rc, &m_backup);
    m_backups << sid.get() << " " << loc.address << " ";

    if (!check_nested(adm))
    {
        return;
    }

    adm->m_multi_ops[m_nested_id] = this;
    m_state = DAEMON_BACKUP;
}

void
backup_state_machine :: callback_coord_backup(admin* adm, int64_t id)
{
    if (!common_callback(adm, id))
    {
        return;
    }

    m_nested_id = adm->wait_until_stable(&m_nested_rc);

    if (!check_nested(adm))
    {
        return;
    }

    adm->m_multi_ops[m_nested_id] = this;
    m_state = WAIT_TO_QUIESCE_AGAIN;
}

void
backup_state_machine :: callback_wait_to_quiesce_again(admin* adm, int64_t id)
{
    if (!common_callback(adm, id))
    {
        return;
    }

    if (m_configuration_version != adm->m_config.version())
    {
        YIELDING_ERROR(INTERNAL) << "configuration changed while taking backup";
        backout(adm);
    }

    m_nested_id = adm->read_only(0, &m_nested_rc);

    if (!check_nested(adm))
    {
        return;
    }

    adm->m_multi_ops[m_nested_id] = this;
    m_state = SET_READ_WRITE;
}

void
backup_state_machine :: callback_set_read_write(admin* adm, int64_t id)
{
    if (!common_callback(adm, id))
    {
        return;
    }

    this->set_status(HYPERDEX_ADMIN_SUCCESS);
    this->set_error(e::error());
    m_state = DONE;
}

void
backup_state_machine :: callback_backout(admin*, int64_t)
{
    m_state = ERROR;
}
