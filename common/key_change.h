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

#ifndef hyperdex_common_key_change_h_
#define hyperdex_common_key_change_h_

// HyperDex
#include "namespace.h"
#include "common/attribute_check.h"
#include "common/auth_wallet.h"
#include "common/funcall.h"
#include "common/network_returncode.h"

BEGIN_HYPERDEX_NAMESPACE

// A (pending) change to an entry in the database
class key_change
{
    public:
        key_change();
        key_change(const key_change& other);
        ~key_change() throw ();

    public:
        bool validate(const schema& sc) const;

        // Check if a key change can be peformed
        network_returncode check(const schema& sc,
                                 bool has_old_value,
                                 const std::vector<e::slice>* old_value) const;

    public:
        key_change& operator = (const key_change&);

    public:
        e::slice key;

        // Will this entry be removed?
        bool erase;

        // Keychange should fail if there is no previous value
        bool fail_if_not_found;

        // Keychange should fail if there is a previous value
        bool fail_if_found;

        // Checks to be performed before key change
        std::vector<attribute_check> checks;

        // The actual keychange is stored here
        std::vector<funcall> funcs;

        // Authorization info is stored here
        std::auto_ptr<auth_wallet> auth;
};

e::packer
operator << (e::packer, const key_change& td);
e::unpacker
operator >> (e::unpacker, key_change& td);
size_t
pack_size(const key_change& td);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_key_change_h_
