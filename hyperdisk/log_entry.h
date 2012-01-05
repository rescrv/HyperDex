// Copyright (c) 2011, Cornell University
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

#ifndef hyperdisk_log_entry_h_
#define hyperdisk_log_entry_h_

// STL
#include <tr1/memory>
#include <vector>

// e
#include <e/buffer.h>

// HyperspaceHashing
#include "hyperspacehashing/hyperspacehashing/mask.h"

namespace hyperdisk
{

// A ``log_entry`` represents one change to the hyperdisk.  The mask fields of
// the coord member must be 0 or UINT64_MAX.  The former indicates their
// absence while the latter indicates their presence.  Thus,
// coordinate(UINT64_MAX, X, UINT64_MAX, Y) is a PUT while
// coordinate(UINT64_MAX, X, 0, 0) is a DEL.  coordinate(0, 0, 0, 0) indicates a
// dummy log entry.

class log_entry
{
    public:
        log_entry();
        log_entry(const hyperspacehashing::mask::coordinate& coord,
                  std::tr1::shared_ptr<e::buffer> backing,
                  const e::slice& key,
                  const std::vector<e::slice>& value,
                  uint64_t version);
        log_entry(const hyperspacehashing::mask::coordinate& coord,
                  std::tr1::shared_ptr<e::buffer> backing,
                  const e::slice& key);

    public:
        hyperspacehashing::mask::coordinate coord;
        bool is_put;
        std::tr1::shared_ptr<e::buffer> backing;
        e::slice key;
        std::vector<e::slice> value;
        uint64_t version;
};

inline
log_entry :: log_entry()
    : coord()
    , is_put()
    , backing()
    , key()
    , value()
    , version()
{
}

inline
log_entry :: log_entry(const hyperspacehashing::mask::coordinate& c,
                       std::tr1::shared_ptr<e::buffer> b,
                       const e::slice& k,
                       const std::vector<e::slice>& va,
                       uint64_t ve)
    : coord(c)
    , is_put(true)
    , backing(b)
    , key(k)
    , value(va)
    , version(ve)
{
}

inline
log_entry :: log_entry(const hyperspacehashing::mask::coordinate& c,
                       std::tr1::shared_ptr<e::buffer> b,
                       const e::slice& k)
    : coord(c)
    , is_put(false)
    , backing(b)
    , key(k)
    , value()
    , version()
{
}

} // namespace hyperdisk

#endif // hyperdisk_log_entry_h_
