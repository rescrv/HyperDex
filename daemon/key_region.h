// Copyright (c) 2011-2014, Cornell University
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

#ifndef hyperdex_daemon_key_region_h_
#define hyperdex_daemon_key_region_h_

// e
#include <e/compat.h>

// HyperDex
#include "common/ids.h"
#include "cityhash/city.h"

BEGIN_HYPERDEX_NAMESPACE

class key_region
{
    public:
        key_region();
        key_region(const region_id& r, const e::slice& k);
        key_region(const key_region& other);

    public:
        bool operator < (const key_region& rhs) const;
        bool operator == (const key_region& rhs) const;
        key_region& operator = (const key_region& rhs);

    public:
        region_id region;
        std::string key;

    private:
        friend class e::compat::hash<key_region>;
};

END_HYPERDEX_NAMESPACE

BEGIN_E_COMPAT_NAMESPACE

template <>
struct hash<hyperdex::key_region>
{
    size_t operator()(const hyperdex::key_region& kr) const
    {
        return CityHash64WithSeed(reinterpret_cast<const char*>(kr.key.data()),
                                  kr.key.size(),
                                  e::compat::hash<uint64_t>()(kr.region.get()));
    }
};

END_E_COMPAT_NAMESPACE

#endif // hyperdex_daemon_key_region_h_
