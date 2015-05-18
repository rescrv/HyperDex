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
#include "common/auth_wallet.h"
#include "common/serialization.h"

using hyperdex::auth_wallet;

auth_wallet :: auth_wallet()
    : m_macaroons()
{
}

auth_wallet :: auth_wallet(const char** macaroons, size_t macaroons_sz)
    : m_macaroons()
{
    m_macaroons.reserve(macaroons_sz);

    for (size_t i = 0; i < macaroons_sz; ++i)
    {
        m_macaroons.push_back(std::string(macaroons[i], strlen(macaroons[i])));
    }
}

auth_wallet :: auth_wallet(const auth_wallet& other)
    : m_macaroons(other.m_macaroons)
{
}

bool
auth_wallet :: get_macaroons(std::vector<macaroon*>* macaroons)
{
    for (size_t i = 0; i < m_macaroons.size(); ++i)
    {
        macaroon_returncode err;
        macaroon* M = macaroon_deserialize(m_macaroons[i].c_str(), &err);

        if (!M)
        {
            return false;
        }

        macaroons->push_back(M);
    }

    return true;
}

static void
build_macaroons(const std::vector<std::string>& in,
                std::vector<e::slice>* out)
{
    out->clear();
    out->reserve(in.size());

    for (size_t i = 0; i < in.size(); ++i)
    {
        out->push_back(e::slice(in[i]));
    }
}

e::packer
hyperdex :: operator << (e::packer lhs, const auth_wallet& rhs)
{
    std::vector<e::slice> macaroons;
    build_macaroons(rhs.m_macaroons, &macaroons);
    return lhs << macaroons;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, auth_wallet& rhs)
{
    std::vector<e::slice> macaroons;
    lhs = lhs >> macaroons;
    rhs.m_macaroons.resize(macaroons.size());

    for (size_t i = 0; i < macaroons.size(); ++i)
    {
        rhs.m_macaroons[i].assign(macaroons[i].cdata(), macaroons[i].size());
    }

    return lhs;
}

size_t
hyperdex :: pack_size(const auth_wallet& aw)
{
    std::vector<e::slice> macaroons;
    build_macaroons(aw.m_macaroons, &macaroons);
    return pack_size(macaroons);
}
