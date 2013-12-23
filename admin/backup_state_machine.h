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

#ifndef hyperdex_admin_backup_h_
#define hyperdex_admin_backup_h_

// STL
#include <memory>
#include <sstream>

// e
#include <e/error.h>
#include <e/intrusive_ptr.h>

// HyperDex
#include "include/hyperdex/admin.h"
#include "namespace.h"
#include "common/configuration.h"
#include "common/ids.h"
#include "common/network_msgtype.h"
#include "admin/multi_yieldable.h"

BEGIN_HYPERDEX_NAMESPACE
class admin;

class backup_state_machine : public multi_yieldable
{
    public:
        backup_state_machine(const char* name,
                             uint64_t admin_visible_id,
                             hyperdex_admin_returncode* status,
                             const char** backups);
        ~backup_state_machine() throw ();

    public:
        bool can_yield();
        bool yield(hyperdex_admin_returncode* status);
        bool initialize(admin* adm, hyperdex_admin_returncode* status);
        bool callback(admin* adm, int64_t id, hyperdex_admin_returncode* status);

    private:
        friend class e::intrusive_ptr<backup_state_machine>;
        backup_state_machine(const backup_state_machine&);
        backup_state_machine& operator = (const backup_state_machine&);

    private:
        void backout(admin* adm);
        bool common_callback(admin* adm, int64_t id);
        bool check_nested(admin* adm);
        void callback_unexpected(admin* adm, int64_t id);
        void callback_set_read_only(admin* adm, int64_t id);
        void callback_wait_to_quiesce(admin* adm, int64_t id);
        void callback_daemon_backup(admin* adm, int64_t id);
        void callback_coord_backup(admin* adm, int64_t id);
        void callback_wait_to_quiesce_again(admin* adm, int64_t id);
        void callback_set_read_write(admin* adm, int64_t id);
        void callback_backout(admin* adm, int64_t id);

    private:
        std::string m_name;
        enum { INITIALIZED,
               SET_READ_ONLY,
               WAIT_TO_QUIESCE,
               DAEMON_BACKUP,
               COORD_BACKUP,
               WAIT_TO_QUIESCE_AGAIN,
               SET_READ_WRITE,
               DONE,
               BACKOUT,
               ERROR,
               YIELDED } m_state;
        int64_t m_nested_id;
        hyperdex_admin_returncode m_nested_rc;
        uint64_t m_configuration_version;
        std::vector<std::pair<server_id, po6::net::location> > m_servers;
        const char* m_backup;
        std::ostringstream m_backups;
        std::string m_backups_str;
        const char** m_backups_c_str;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_admin_backup_h_
