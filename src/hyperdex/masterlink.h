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

#ifndef hyperdex_masterlink_h_
#define hyperdex_masterlink_h_

#define __STDC_LIMIT_MACROS

// po6
#include <po6/net/location.h>

// HyperDex
#include <hyperdex/datalayer.h>
#include <hyperdex/logical.h>

namespace hyperdex
{

class masterlink
{
    public:
        masterlink(const po6::net::location& loc,
                   hyperdex::datalayer* data,
                   hyperdex::logical* comm);
        ~masterlink();

    public:
        void shutdown();

    private:
        masterlink(const masterlink&);

    private:
        void connect();
        void io(ev::io& i, int revents);
        void read();
        void run();
        void time(ev::timer& t, int revents);
        void wake(ev::async& a, int revents);

    private:
        masterlink& operator = (const masterlink&);

    private:
        bool m_continue;
        po6::net::location m_loc;
        hyperdex::datalayer* m_data;
        hyperdex::logical* m_comm;
        po6::net::socket m_sock;
        po6::threads::thread m_thread;
        ev::dynamic_loop m_dl;
        ev::async m_wake;
        ev::io m_io;
        ev::timer m_timer;
        e::buffer m_partial;
        hyperdex::configuration m_config;
        bool m_config_valid;
};

} // namespace hyperdex

#endif // hyperdex_masterlink_h_
