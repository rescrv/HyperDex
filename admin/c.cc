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

#define __STDC_LIMIT_MACROS

// HyperDex
#include "include/hyperdex/admin.h"
#include "visibility.h"
#include "common/macros.h"
#include "admin/admin.h"

#define C_WRAP_EXCEPT(X) \
    try \
    { \
        X \
    } \
    catch (po6::error& e) \
    { \
        errno = e; \
        *status = HYPERDEX_ADMIN_EXCEPTION; \
        return -1; \
    } \
    catch (std::bad_alloc& ba) \
    { \
        errno = ENOMEM; \
        *status = HYPERDEX_ADMIN_NOMEM; \
        return -1; \
    } \
    catch (...) \
    { \
        *status = HYPERDEX_ADMIN_EXCEPTION; \
        return -1; \
    }

extern "C"
{

HYPERDEX_API struct hyperdex_admin*
hyperdex_admin_create(const char* coordinator, uint16_t port)
{
    try
    {
        return reinterpret_cast<struct hyperdex_admin*>(new hyperdex::admin(coordinator, port));
    }
    catch (po6::error& e)
    {
        errno = e;
        return NULL;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        return NULL;
    }
}

HYPERDEX_API void
hyperdex_admin_destroy(struct hyperdex_admin* admin)
{
    delete reinterpret_cast<hyperdex::admin*>(admin);
}

HYPERDEX_API int64_t
hyperdex_admin_dump_config(struct hyperdex_admin* _adm,
                           hyperdex_admin_returncode* status,
                           const char** config)
{
    C_WRAP_EXCEPT(
    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);
    return adm->dump_config(status, config);
    );
}

HYPERDEX_API int
hyperdex_admin_validate_space(struct hyperdex_admin* _adm,
                              const char* description,
                              hyperdex_admin_returncode* status,
                              const char** error_msg)
{
    C_WRAP_EXCEPT(
    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);
    return adm->validate_space(description, status, error_msg);
    );
}

HYPERDEX_API int64_t
hyperdex_admin_add_space(struct hyperdex_admin* _adm,
                         const char* description,
                         hyperdex_admin_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);
    return adm->add_space(description, status);
    );
}

HYPERDEX_API int64_t
hyperdex_admin_rm_space(struct hyperdex_admin* _adm,
                        const char* name,
                        hyperdex_admin_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);
    return adm->rm_space(name, status);
    );
}

HYPERDEX_API int64_t
hyperdex_admin_enable_perf_counters(struct hyperdex_admin* _adm,
                                    enum hyperdex_admin_returncode* status,
                                    struct hyperdex_admin_perf_counter* pc)
{
    C_WRAP_EXCEPT(
    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);
    return adm->enable_perf_counters(status, pc);
    );
}

HYPERDEX_API void
hyperdex_admin_disable_perf_counters(struct hyperdex_admin* _adm)
{
    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);
    return adm->disable_perf_counters();
}

HYPERDEX_API int64_t
hyperdex_admin_loop(struct hyperdex_admin* _adm, int timeout,
                    enum hyperdex_admin_returncode* status)
{
    C_WRAP_EXCEPT(
    hyperdex::admin* adm = reinterpret_cast<hyperdex::admin*>(_adm);
    return adm->loop(timeout, status);
    );
}

} // extern "C"
