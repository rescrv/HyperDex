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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

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
#include <iomanip>

// e
#include <e/guard.h>

extern "C"
{

static const char* _path = NULL;

static struct poptOption help_popts[] = {
    {"help", '?', POPT_ARG_NONE, NULL, '?', "Show this help message", NULL},
    {"usage", 0, POPT_ARG_NONE, NULL, 'u', "Display brief usage message", NULL},
    {"version", 0, POPT_ARG_NONE, NULL, 'v', "Print the version of HyperDex and exit", NULL},
    POPT_TABLEEND
};

static struct poptOption global_popts[] = {
    {"exec-path", 0, POPT_ARG_STRING, &_path, 'p', "Path to where the HyperDex programs are installed", "PATH"},
    POPT_TABLEEND
};

static struct poptOption popts[] = {
    {NULL, 0, POPT_ARG_INCLUDE_TABLE, help_popts, 0, "Help options:", NULL},
    {NULL, 0, POPT_ARG_INCLUDE_TABLE, global_popts, 0, "Global options:", NULL},
    POPT_TABLEEND
};

} // extern "C"

struct subcommand
{
    subcommand(const char* n, const char* d) : name(n), description(d) {}
    const char* name;
    const char* description;
};

static subcommand subcommands[] = {
    subcommand("coordinator",           "Start a new HyperDex coordinator"),
    subcommand("daemon",                "Start a new HyperDex daemon"),
    subcommand("add-space",             "Create a new space"),
    subcommand("rm-space",              "Remove an existing space"),
    subcommand("initialize-cluster",    "One time initialization of a HyperDex coordinator"),
    subcommand("initiate-transfer",     "Manually start a data transfer to repair a failure"),
    subcommand("show-config",           "Output a human-readable version of the cluster configuration"),
    subcommand(NULL, NULL)
};

static int
help(poptContext poptcon)
{
    poptPrintHelp(poptcon, stdout, 0);
    size_t max_command_sz = 0;

    for (subcommand* s = subcommands; s->name; ++s)
    {
        size_t command_sz = strlen(s->name);
        max_command_sz = std::max(max_command_sz, command_sz);
    }

    size_t pad = ((max_command_sz + 3ULL) & ~3ULL) + 4;

    std::cout << std::setfill(' ') << "\nAvailable commands:\n";

    for (subcommand* s = subcommands; s->name; ++s)
    {
        std::cout << "    " << std::left << std::setw(pad) << s->name << s->description << "\n";
    }

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
                poptPrintUsage(poptcon, stdout, 0);
                return EXIT_FAILURE;
            case 'p':
                break;
            case 'v':
                std::cout << "HyperDex version " VERSION << std::endl;
                return EXIT_SUCCESS;
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

    std::string path;
    char* hyperdex_exec_path = getenv("HYPERDEX_EXEC_PATH");

    if (_path)
    {
        path = std::string(_path);
    }
    else if (hyperdex_exec_path)
    {
        path = std::string(hyperdex_exec_path);
    }
    else
    {
        path = std::string(HYPERDEX_EXEC_DIR);
    }

    char* old_path = getenv("PATH");

    if (old_path)
    {
        path += ":" + std::string(old_path);
    }

    if (setenv("PATH", path.c_str(), 1) < 0)
    {
        perror("setting path");
    }

    for (subcommand* s = subcommands; s->name; ++s)
    {
        if (strcmp(s->name, args[0]) == 0)
        {
            std::string name("hyperdex-");
            name += s->name;
            args[0] = name.c_str();

            if (execvp(args[0], const_cast<char*const*>(args)) < 0)
            {
                std::cerr << "failed to exec " << s->name << ": " << strerror(errno) << std::endl;
                std::cerr << "HYPERDEX_EXEC_PATH=" << path << std::endl;
                return EXIT_FAILURE;
            }
        }
    }

    std::cerr << "hyperdex: '" << args[0] << "' is not a HyperDex command.  See 'hyperdex --help'\n" << std::endl;
    return help(poptcon);
}
