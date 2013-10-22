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
        int validate_space(const char* description,
                           enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_validate_space(m_adm, description, status); }
        int64_t add_space(const char* description,
                          enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_add_space(m_adm, description, status); }
        int64_t rm_space(const char* name,
                         enum hyperdex_admin_returncode* status)
            { return hyperdex_admin_rm_space(m_adm, name, status); }
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
