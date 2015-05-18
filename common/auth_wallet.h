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

#ifndef hyperdex_common_auth_wallet_h_
#define hyperdex_common_auth_wallet_h_

// e
#include <e/serialization.h>

// macaroons
#include <macaroons.h>

// HyperDex
#include "namespace.h"
#include "common/schema.h"

BEGIN_HYPERDEX_NAMESPACE

class auth_wallet
{
    public:
        auth_wallet();
        auth_wallet(const char** macaroons, size_t macaroons_sz);
        auth_wallet(const auth_wallet&);

    public:
        bool get_macaroons(std::vector<macaroon*>* macaroons);

    private:
        friend e::packer operator << (e::packer lhs, const auth_wallet& rhs);
        friend e::unpacker operator >> (e::unpacker lhs, auth_wallet& rhs);
        friend size_t pack_size(const auth_wallet& aw);

        friend std::ostream& operator << (std::ostream& lhs, const auth_wallet& rhs); // XXX

    private:
        auth_wallet& operator = (const auth_wallet&);

    private:
        std::vector<std::string> m_macaroons;
};

e::packer
operator << (e::packer lhs, const auth_wallet& rhs);
e::unpacker
operator >> (e::unpacker lhs, auth_wallet& rhs);
size_t
pack_size(const auth_wallet& aw);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_auth_wallet_h_
