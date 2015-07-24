// Copyright (c) 2012, Robert Escriva
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

// C
#include <cstdio>
#include <stdint.h>

// STL
#include <vector>

// po6
#include <po6/io/fd.h>

// HyperDex
#include "tools/common.h"

int
main(int argc, const char* argv[])
{
    std::string libpath;

    if (!hyperdex::locate_coordinator_lib(argv[0], &libpath))
    {
        std::cerr << "cannot locate the HyperDex coordinator library" << std::endl;
        return EXIT_FAILURE;
    }

    // setup the environment
    if (setenv("REPLICANT_WRAP", "hyperdex-coordinator", 1) < 0)
    {
        std::cerr << "could not setup the environment: " << strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    // generate a random token
    uint64_t token;
    po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

    if (sysrand.get() < 0 ||
        sysrand.read(&token, sizeof(token)) != sizeof(token))
    {
        std::cerr << "could not generate random token for cluster" << std::endl;
        return EXIT_FAILURE;
    }

    char token_buf[21];
    snprintf(token_buf, 21, "%lu", (unsigned long) token);

    // exec replicant daemon
    std::vector<const char*> args;
    args.push_back("replicant");
    args.push_back("daemon");

    for (int i = 1; i < argc; ++i)
    {
        args.push_back(argv[i]);
    }

    args.push_back("--object");
    args.push_back("hyperdex");
    args.push_back("--library");
    args.push_back(libpath.c_str());
    args.push_back("--init-string");
    args.push_back(token_buf);
    args.push_back(NULL);

    if (execvp("replicant", const_cast<char*const*>(&args[0])) < 0)
    {
        perror("could not exec replicant");
        return EXIT_FAILURE;
    }

    abort();
}
