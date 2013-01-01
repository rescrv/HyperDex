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

// C
#include <cstdlib>
#include <cstring>

// POSIX
#include <errno.h>
#include <unistd.h>

// Popt
#include <popt.h>

// C++
#include <iostream>

// e
#include <e/guard.h>

extern "C"
{

static struct poptOption popts[] = {
    {"help", '?', POPT_ARG_NONE, NULL, '?', "Show this help message", NULL},
    {"usage", 0, POPT_ARG_NONE, NULL, 'u', "Display brief usage message", NULL},
    POPT_TABLEEND
};

} // extern "C"

struct prog
{
    prog(const char* n, const char* r, const char* p) : name(n), rname(r), path(p) {}
    const char* name;
    const char* rname;
    const char* path;
};

#define PROG(N) prog(N, "hyperdex " N, HYPERDEX_EXEC_DIR "/hyperdex-" N)

static prog progs[] = {
    PROG("add-space"),
    PROG("rm-space"),
    /*PROG("daemon"),*/
    prog(NULL, NULL, NULL)
};

static int
help(poptContext poptcon)
{
    poptPrintHelp(poptcon, stderr, 0);
    std::cerr << "\n"
              << "Available commands:\n"
              << "    add-space         Create a new space\n"
              << "    rm-space          Remove an existing space\n"
              << "    daemon            Start a new HyperDex daemon\n"
              << std::flush;
    return EXIT_FAILURE;
}

int
main(int argc, const char* argv[])
{
    poptContext poptcon;
    poptcon = poptGetContext(NULL, argc, argv, popts, POPT_CONTEXT_POSIXMEHARDER);
    e::guard g = e::makeguard(poptFreeContext, poptcon); g.use_variable();
    poptSetOtherOptionHelp(poptcon, "[COMMAND] [ARGS]");
    int rc;

    while ((rc = poptGetNextOpt(poptcon)) != -1)
    {
        switch (rc)
        {
            case '?':
                return help(poptcon);
            case 'u':
                poptPrintUsage(poptcon, stderr, 0);
                return EXIT_FAILURE;
            case POPT_ERROR_NOARG:
            case POPT_ERROR_BADOPT:
            case POPT_ERROR_BADNUMBER:
            case POPT_ERROR_OVERFLOW:
                std::cerr << poptStrerror(rc) << " " << poptBadOption(poptcon, 0) << std::endl;
                return EXIT_FAILURE;
            case POPT_ERROR_OPTSTOODEEP:
            case POPT_ERROR_BADQUOTE:
            case POPT_ERROR_ERRNO:
            default:
                std::cerr << "logic error in argument parsing" << std::endl;
                return EXIT_FAILURE;
        }
    }

    const char** args = poptGetArgs(poptcon);

    if (!args || args[0] == NULL)
    {
        return help(poptcon);
    }

    for (prog* p = progs; p->name; ++p)
    {
        if (strcmp(p->name, args[0]) == 0)
        {
            args[0] = p->rname;

            if (execv(p->path, const_cast<char*const*>(args)) < 0)
            {
                std::cerr << "failed to exec " << p->name << ": " << strerror(errno) << std::endl;
                return EXIT_FAILURE;
            }

            std::cerr << "failed to exec " << p->name << " for unknown reasons" << std::endl;
            return EXIT_FAILURE;
        }
    }

    std::cerr << "unknown command " << args[0] << "\n" << std::endl;
    return help(poptcon);
}
