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

#ifndef hyperdex_admin_hpp_
#define hyperdex_admin_hpp_

// C++
#include <iostream>

// HyperDex
#include <hyperdex/admin.h>

namespace hyperdex
{

class Admin
{
    public:
        Admin(const char* coordinator, uint16_t port)
            : m_adm(hyperdex_admin_create(coordinator, port))
        {
            if (!m_adm)
            {
                throw std::bad_alloc();
            }
        }
        ~Admin() throw ()
        {
            hyperdex_admin_destroy(m_adm);
        }

    public:
        int64_t dump_config(enum hyperdex_admin_returncode* status,
                            const char** config)
            { return hyperdex_admin_dump_config(m_adm, status, config); }
        int64_t read_only(int ro, enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_read_only(m_adm, ro, status); }
        int64_t wait_until_stable(enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_wait_until_stable(m_adm, status); }
        int64_t fault_tolerance(const char* space, uint64_t ft,
                                enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_fault_tolerance(m_adm, space, ft, status); }
        int validate_space(const char* description,
                           enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_validate_space(m_adm, description, status); }
        int64_t add_space(const char* description,
                          enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_add_space(m_adm, description, status); }
        int64_t rm_space(const char* name,
                         enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_rm_space(m_adm, name, status); }
        int64_t mv_space(const char* source, const char* target,
                         enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_mv_space(m_adm, source, target, status); }
        int64_t list_spaces(enum hyperdex_admin_returncode* status,
                            const char** spaces)
            { return hyperdex_admin_list_spaces(m_adm, status, spaces); }
        int64_t add_index(const char* space, const char* attr,
                          enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_add_index(m_adm, space, attr, status); }
        int64_t rm_index(uint64_t idxid, enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_rm_index(m_adm, idxid, status); }
        int64_t server_register(uint64_t token, const char* address,
                                enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_server_register(m_adm, token, address, status); }
        int64_t server_online(uint64_t token, enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_server_online(m_adm, token, status); }
        int64_t server_offline(uint64_t token, enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_server_offline(m_adm, token, status); }
        int64_t server_forget(uint64_t token, enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_server_forget(m_adm, token, status); }
        int64_t server_kill(uint64_t token, enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_server_kill(m_adm, token, status); }
        int64_t backup(const char* name, enum hyperdex_admin_returncode* status, const char** backups)
            { return hyperdex_admin_backup(m_adm, name, status, backups); }
        int64_t enable_perf_counters(enum hyperdex_admin_returncode* status,
                                     struct hyperdex_admin_perf_counter* pc)
            { return hyperdex_admin_enable_perf_counters(m_adm, status, pc); }
        void disable_perf_counters()
            { return hyperdex_admin_disable_perf_counters(m_adm); }

    public:
        int64_t loop(int timeout, enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_loop(m_adm, timeout, status); }
        std::string error_message()
            { return hyperdex_admin_error_message(m_adm); }
        std::string error_location()
            { return hyperdex_admin_error_location(m_adm); }

    private:
        Admin(const Admin&);
        Admin& operator = (const Admin&);

    private:
        hyperdex_admin* m_adm;
};

} // namespace hyperdex

std::ostream&
operator << (std::ostream& lhs, hyperdex_admin_returncode rhs);

#endif // hyperdex_admin_hpp_
