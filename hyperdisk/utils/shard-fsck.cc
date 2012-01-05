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

// po6
#include <po6/error.h>
#include <po6/io/fd.h>

// e
#include <e/intrusive_ptr.h>

// HyperDisk
#include "hyperdisk/shard.h"

int
main(int argc, char* argv[])
{
    for (int i = 1; i < argc; ++i)
    {
        std::cout << "Processing " << argv[i] << std::endl;

        try
        {
            po6::io::fd cwd(AT_FDCWD);
            e::intrusive_ptr<hyperdisk::shard> shard;
            shard = hyperdisk::shard::open(cwd, argv[i]);
            shard->fsck(std::cerr);
        }
        catch (po6::error& e)
        {
            std::cerr << "error:  [" << e << "] " << e.what();
            return EXIT_FAILURE;
        }
        catch (std::runtime_error& e)
        {
            std::cerr << "error:  " << e.what();
            return EXIT_FAILURE;
        }
    }

    return EXIT_SUCCESS;
}
