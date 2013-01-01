// Copyright (c) 2012, Cornell University
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
//     * Neither the name of Replicant nor the names of its contributors may be
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

// Popt
#include <popt.h>

// C++
#include <iostream>

static const char* _connect_host = "127.0.0.1";
static unsigned long _connect_port = 1982;

extern "C"
{

static struct poptOption connect_popts[] = {
    {"host", 'h', POPT_ARG_STRING, &_connect_host, 'h',
     "connect to the coordinator on an IP address or hostname (default: 127.0.0.1)",
     "addr"},
    {"port", 'p', POPT_ARG_LONG, &_connect_port, 'p',
     "connect to the coordinator on an alternative port (default: 1982)",
     "port"},
    POPT_TABLEEND
};

} // extern "C"

#define CONNECT_TABLE {NULL, 0, POPT_ARG_INCLUDE_TABLE, connect_popts, 0, "Connect to a cluster:", NULL},

static bool
check_host()
{
    return true;
}

static bool
check_port()
{
    if (_connect_port >= (1 << 16))
    {
        std::cerr << "port number to connect to is out of range" << std::endl;
        return false;
    }

    return true;
}
