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

// Libev
#include <ev++.h>

// HyperDex
#include <hyperdex/client.h>
#include <hyperdex/instance.h>
#include <hyperdex/masterlink.h>
#include <hyperdex/physical.h>

struct hyperdex :: client :: priv
{
    priv(po6::net::location coordinator)
        : dl()
        , phys(dl, po6::net::ipaddr::ANY())
        , ml(coordinator, "client\n", std::tr1::function<void (const hyperdex::configuration&)>())
        , mapping_lock()
        , mapping()
    {
    }

    ev::dynamic_loop dl;
    hyperdex::physical phys;
    hyperdex::masterlink ml;
    po6::threads::rwlock mapping_lock;
    std::map<entityid, instance> mapping;
};

hyperdex :: client :: client(po6::net::location coordinator)
    : p(new priv(coordinator))
{
}

hyperdex :: result_t
hyperdex :: client :: get(const std::string& space,
                          const e::buffer& key,
                          std::vector<e::buffer>* value)
{
    // XXX
    return INVALID;
}

hyperdex :: result_t
hyperdex :: client :: put(const std::string& space,
                          const e::buffer& key,
                          const std::vector<e::buffer>& value)
{
    // XXX
    return INVALID;
}

hyperdex :: result_t
hyperdex :: client :: del(const std::string& space,
                          const e::buffer& key)
{
    // XXX
    return INVALID;
}
