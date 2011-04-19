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

// C++
#include <iostream>

// Google Log
#include <glog/logging.h>

// HyperDex
#include <hyperdex/hyperdexd.h>

hyperdex :: hyperdexd :: hyperdexd()
    : m_continue(true)
{
}

hyperdex :: hyperdexd :: ~hyperdexd()
{
}

void
hyperdex :: hyperdexd :: run()
{
    // Actually run HyperDex from within here.

    // Wait for a signal to interrupt us.
    while (m_continue)
    {
        pause();
    }

    // Cleanup the background threads.
}

void
hyperdex :: hyperdexd :: HUP()
{
    LOG(ERROR) << "SIGHUP received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: INT()
{
    LOG(ERROR) << "SIGINT received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: QUIT()
{
    LOG(ERROR) << "SIGQUIT received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: TERM()
{
    LOG(ERROR) << "SIGTERM received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: USR1()
{
    LOG(INFO) << "SIGUSR1 received.  This does nothing.";
}

void
hyperdex :: hyperdexd :: USR2()
{
    LOG(INFO) << "SIGUSR2 received.  This does nothing.";
}
