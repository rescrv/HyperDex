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

// C
#include <cassert>

// STL
#include <set>

// Google Log
#include <glog/logging.h>

// HyperDex
#include <hyperdex/datalayer.h>

hyperdex :: datalayer :: datalayer()
{
}

std::set<hyperdex::regionid>
hyperdex :: datalayer :: regions()
{
    // XXX
    return std::set<hyperdex::regionid>();
}

void
hyperdex :: datalayer :: create(const regionid& ri,
                                uint16_t numcolumns)
{
    LOG(INFO) << "If I were implemented I would create " << ri << " with "
              << numcolumns << " columns";
}

void
hyperdex :: datalayer :: drop(const regionid& ri)
{
    LOG(INFO) << "If I were implemented I would drop " << ri;
}

bool
hyperdex :: datalayer :: get(const regionid& ri,
                             const e::buffer& key,
                             std::vector<e::buffer>* value)
{
    // XXX
    return false;
}

bool
hyperdex :: datalayer :: put(const regionid& ri,
                             const e::buffer& key,
                             const std::vector<e::buffer>& value)
{
    // XXX
    return false;
}

bool
hyperdex :: datalayer :: del(const regionid& ri,
                             const e::buffer& key)
{
    // XXX
    return false;
}
