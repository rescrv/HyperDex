// Copyright (c) 2013, Cornell University
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
#include <cstdlib>

// HyperDex
#include <hyperdex/admin.hpp>
#include "tools/common.h"

int
main(int argc, const char* argv[])
{
    hyperdex::connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS] <server-id>");
    ap.add("Connect to a cluster:", conn.parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (!conn.validate())
    {
        std::cerr << "invalid host:port specification\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 1)
    {
        std::cerr << "please specify the server id" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    char* end = NULL;
    uint64_t token = strtoull(ap.args()[0], &end, 0);

    if (*end != '\0' || ap.args()[0] == end)
    {
        std::cerr << "server id must be a number" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    try
    {
        hyperdex::Admin h(conn.host(), conn.port());
        hyperdex_admin_returncode rrc;
        int64_t rid = h.server_online(token, &rrc);

        if (rid < 0)
        {
            std::cerr << "could not bring server online: " << rrc << std::endl;
            return EXIT_FAILURE;
        }

        hyperdex_admin_returncode lrc;
        int64_t lid = h.loop(-1, &lrc);

        if (lid < 0)
        {
            std::cerr << "could not bring server online: " << lrc << std::endl;
            return EXIT_FAILURE;
        }

        assert(rid == lid);

        if (rrc != HYPERDEX_ADMIN_SUCCESS)
        {
            std::cerr << "could not bring server online: " << rrc << std::endl;
            return EXIT_FAILURE;
        }

        return EXIT_SUCCESS;
    }
    catch (std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
