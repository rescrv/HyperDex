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

// C
#include <stdint.h>

// po6
#include <po6/net/hostname.h>

// BusyBee
#include <busybee_constants.h>
#include <busybee_single.h>

// HyperDex
#include <hyperdex/admin.h>
#include "visibility.h"
#include "common/ids.h"
#include "common/network_msgtype.h"
#include "common/network_returncode.h"
#include "common/serialization.h"

extern "C"
{

using namespace hyperdex;

HYPERDEX_API int
hyperdex_admin_raw_backup(const char* host, uint16_t port,
                          const char* name,
                          enum hyperdex_admin_returncode* status)
{
    try
    {
        busybee_single bbs(po6::net::location(host, port));
        const uint8_t type = static_cast<uint8_t>(BACKUP);
        const uint8_t flags = 0;
        const uint64_t version = 0;
        virtual_server_id to(UINT64_MAX);
        const uint64_t nonce = 0xdeadbeefcafebabe;
        size_t name_sz = strlen(name) + 1;
        e::slice name_s(name, name_sz);
        size_t sz = BUSYBEE_HEADER_SIZE
                  + sizeof(uint8_t) /*mt*/
                  + sizeof(uint8_t) /*flags*/
                  + sizeof(uint64_t) /*version*/
                  + sizeof(uint64_t) /*vidt*/
                  + sizeof(uint64_t) /*nonce*/
                  + pack_size(name_s);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        e::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
        pa = pa << type << flags << version << to << nonce << name_s;
        bbs.set_timeout(-1);

        switch (bbs.send(msg))
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
                *status = HYPERDEX_ADMIN_TIMEOUT;
                return -1;
            case BUSYBEE_INTERRUPTED:
                *status = HYPERDEX_ADMIN_INTERRUPTED;
                return -1;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_DISRUPTED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_EXTERNAL:
                *status = HYPERDEX_ADMIN_SERVERERROR;
                return -1;
            default:
                abort();
        }

        switch (bbs.recv(&msg))
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
                *status = HYPERDEX_ADMIN_TIMEOUT;
                return -1;
            case BUSYBEE_INTERRUPTED:
                *status = HYPERDEX_ADMIN_INTERRUPTED;
                return -1;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_DISRUPTED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_EXTERNAL:
                *status = HYPERDEX_ADMIN_SERVERERROR;
                return -1;
            default:
                abort();
        }

        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE
                                          + sizeof(uint8_t) /*mt*/
                                          + sizeof(uint64_t) /*vidt*/
                                          + sizeof(uint64_t) /*nonce*/);
        uint16_t rt;

        if ((up >> rt).error())
        {
            *status = HYPERDEX_ADMIN_SERVERERROR;
            return -1;
        }

        network_returncode rc = static_cast<network_returncode>(rt);

        if (rc == NET_SUCCESS)
        {
            *status = HYPERDEX_ADMIN_SUCCESS;
            return 0;
        }
        else
        {
            *status = HYPERDEX_ADMIN_SERVERERROR;
            return -1;
        }
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        *status = HYPERDEX_ADMIN_NOMEM;
        return -1;
    }
    catch (...)
    {
        *status = HYPERDEX_ADMIN_EXCEPTION;
        return -1;
    }
}

} // extern "C"
