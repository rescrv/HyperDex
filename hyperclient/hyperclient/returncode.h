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

#ifndef hyperclient_returncode_h_
#define hyperclient_returncode_h_

namespace hyperclient
{

enum returncode
{
    SUCCESS     = 0,
    NOTFOUND    = 1,
    WRONGARITY  = 2,
    NOTASPACE   = 8,
    BADSEARCH   = 9,
    BADDIMENSION= 10,
    COORDFAIL   = 16,
    SERVERERROR = 17,
    CONNECTFAIL = 18,
    DISCONNECT  = 19,
    RECONFIGURE = 20,
    LOGICERROR  = 21,
    TIMEOUT     = 22
};

#define str(x) #x
#define xstr(x) str(x)
#define stringify(x) case (x): lhs << xstr(x); break

inline std::ostream&
operator << (std::ostream& lhs, const returncode& rhs)
{
    switch(rhs)
    {
        stringify(SUCCESS);
        stringify(NOTFOUND);
        stringify(WRONGARITY);
        stringify(NOTASPACE);
        stringify(BADSEARCH);
        stringify(BADDIMENSION);
        stringify(COORDFAIL);
        stringify(SERVERERROR);
        stringify(CONNECTFAIL);
        stringify(DISCONNECT);
        stringify(RECONFIGURE);
        stringify(LOGICERROR);
        stringify(TIMEOUT);
        default:
            lhs << "unknown returncode";
            break;
    }

    return lhs;
}

#undef stringify
#undef xstr
#undef str

} // namespace hyperclient

#endif // hyperclient_returncode_h_
