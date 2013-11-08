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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// STL
#include <vector>

// e
#include <e/subcommand.h>

int
main(int argc, const char* argv[])
{
    std::vector<e::subcommand> cmds;
    cmds.push_back(e::subcommand("coordinator",           "Start a new HyperDex coordinator"));
    cmds.push_back(e::subcommand("daemon",                "Start a new HyperDex daemon"));
    cmds.push_back(e::subcommand("add-space",             "Create a new HyperDex space"));
    cmds.push_back(e::subcommand("rm-space",              "Remove an existing HyperDex space"));
    cmds.push_back(e::subcommand("validate-space",        "Validate a HyperDex space description"));
    cmds.push_back(e::subcommand("server-register",       "Manually register a new HyperDex server"));
    cmds.push_back(e::subcommand("show-config",           "Output a human-readable version of the cluster configuration"));
    cmds.push_back(e::subcommand("perf-counters",         "Collect performance counters from a cluster"));
    return dispatch_to_subcommands(argc, argv,
                                   "hyperdex", "HyperDex",
                                   PACKAGE_VERSION,
                                   "hyperdex-",
                                   "HYPERDEX_EXEC_PATH", HYPERDEX_EXEC_DIR,
                                   &cmds.front(), cmds.size());
}
