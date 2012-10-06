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

#ifndef hyperdex_disk_disk_returncode_h_
#define hyperdex_disk_disk_returncode_h_

// C++
#include <iostream>

namespace hyperdex
{

// disk_returncode occupies [8192, 8320)
enum disk_returncode
{
    DISK_SUCCESS     = 8192,
    DISK_NOT_FOUND   = 8193,

    DISK_CORRUPT     = 8194,
    DISK_FATAL_ERROR = 8195,



    DISK_WRONGARITY  = 8220,
    DISK_DATAFULL    = 8221,
    DISK_SEARCHFULL  = 8196,
    DISK_SYNCFAILED  = 8197,
    DISK_DROPFAILED  = 8198,
    DISK_MISSINGDISK = 8199,
    DISK_SPLITFAILED = 8200,
    DISK_DIDNOTHING  = 8201
};

std::ostream&
operator << (std::ostream& lhs, disk_returncode rhs);

} // namespace hyperdex

#endif // hyperdex_disk_disk_returncode_h_
