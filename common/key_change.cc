// Copyright (c) 2014, Cornell University
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

// e
#include <e/guard.h>

// HyperDex
#include <hyperdex/client.h>
#include "common/datatype_info.h"
#include "common/serialization.h"
#include "common/key_change.h"

using hyperdex::key_change;

key_change :: key_change()
    : key()
    , erase(false)
    , fail_if_not_found(false)
    , fail_if_found(false)
    , checks()
    , funcs()
    , auth()
{
}

key_change :: key_change(const key_change& other)
    : key(other.key)
    , erase(other.erase)
    , fail_if_not_found(other.fail_if_not_found)
    , fail_if_found(other.fail_if_found)
    , checks(other.checks)
    , funcs(other.funcs)
    , auth()
{
    if (other.auth.get())
    {
        auth.reset(new auth_wallet(*other.auth));
    }
}

key_change :: ~key_change() throw ()
{
}

bool
key_change :: validate(const schema& sc) const
{
    return datatype_info::lookup(sc.attrs[0].type)->validate(key) &&
           validate_attribute_checks(sc, checks) == checks.size() &&
           validate_funcs(sc, funcs) == funcs.size() &&
           (!erase || funcs.empty());
}

hyperdex::network_returncode
key_change :: check(const schema& sc,
                    bool has_old_value,
                    const std::vector<e::slice>* old_value) const
{
    const key_change* kc = this;

    if (!has_old_value && kc->erase)
    {
        return NET_NOTFOUND;
    }
    else if (!has_old_value && kc->fail_if_not_found)
    {
        return NET_NOTFOUND;
    }
    else if (has_old_value && kc->fail_if_found)
    {
        return NET_CMPFAIL;
    }
    else if (has_old_value && passes_attribute_checks(sc, kc->checks, key, *old_value) < kc->checks.size())
    {
        return NET_CMPFAIL;
    }

    return NET_SUCCESS;
}

key_change&
key_change :: operator = (const key_change& rhs)
{
    if (this != &rhs)
    {
        key               = rhs.key;
        erase             = rhs.erase;
        fail_if_not_found = rhs.fail_if_not_found;
        fail_if_found     = rhs.fail_if_found;
        checks            = rhs.checks;
        funcs             = rhs.funcs;

        if (rhs.auth.get())
        {
            auth.reset(new auth_wallet(*rhs.auth));
        }
    }

    return *this;
}

#define FLAG_WRITE 128
#define FLAG_AUTH 64
#define FLAG_FINF 1
#define FLAG_FIF 2

e::packer
hyperdex :: operator << (e::packer pa, const key_change& td)
{
    uint8_t flags = (td.erase ? 0 : FLAG_WRITE)
                  | (td.fail_if_not_found ? FLAG_FINF : 0)
                  | (td.fail_if_found ? FLAG_FIF : 0)
                  | (td.auth.get() ? FLAG_AUTH : 0);
    pa = pa << td.key << flags << td.checks << td.funcs;

    if (td.auth.get())
    {
        pa = pa << *td.auth;
    }

    return pa;
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, key_change& td)
{
    uint8_t flags;
    up = up >> td.key >> flags >> td.checks >> td.funcs;
    td.erase = !(flags & FLAG_WRITE);
    td.fail_if_not_found = flags & FLAG_FINF;
    td.fail_if_found = flags & FLAG_FIF;

    if ((flags & FLAG_AUTH))
    {
        td.auth.reset(new auth_wallet());
        up = up >> *td.auth;
    }

    return up;
}

size_t
hyperdex :: pack_size(const key_change& td)
{
    return pack_size(td.key)
         + sizeof(uint8_t)
         + pack_size(td.checks)
         + pack_size(td.funcs)
         + (td.auth.get() ? pack_size(*td.auth) : 0);
}
