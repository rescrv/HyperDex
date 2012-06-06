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

#ifndef util_atomicfile_h_
#define util_atomicfile_h_

// POSIX
#include <errno.h>
#include <limits.h>
#include <stdio.h>

// C++
#include <iostream>

// STL
#include <string>

// po6
#include <po6/error.h>
#include <po6/io/fd.h>
#include <po6/pathname.h>

namespace util
{

class atomicfile
{
    public:
        // Rewrite given file atomically, on failure errno will be set.
        static bool rewrite(const char* dir, const char* file, const char* contents);
};

inline bool 
atomicfile::rewrite(const char* dir, const char* file, const char* contents)
{
    // Using C file I/O as C++ flush and sync are not portable.

    // Write contents to a temp file in the same dir as target file.
    po6::pathname tmp_name = po6::join(dir, "tmp");
    po6::io::fd fd;
    if (!mkstemp(&fd, &tmp_name))
    {
        return false;
    }
    
    FILE *f = fdopen(fd.get(), "w");
    if (!f)
    {
        ::close(fd.get());
        unlink(tmp_name.get());
        return false;
    }
    
    if (EOF == fputs(contents, f))
    {
        fclose(f);
        unlink(tmp_name.get());
        return false;
    }
    
    // Make sure the file is on the disk.
    if (fflush(f))
    {
        fclose(f);
        unlink(tmp_name.get());
        return false;
    }

    if (fsync(fd.get()))
    {
        fclose(f);
        unlink(tmp_name.get());
        return false;
    }
    
    if (fclose(f))
    {
        unlink(tmp_name.get());
        return false;
    }
    
    // Rename the temp file into target.
    po6::pathname file_name = po6::join(dir, file);
    if (rename(tmp_name.get(), file_name.get()))
    {
        unlink(tmp_name.get());
        return false;
    }

    return true;
}

} // namespace util

#endif // util_atomicfile_h_
