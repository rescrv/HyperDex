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

#ifndef hyperclient_completedop_h_
#define hyperclient_completedop_h_

// HyperClient
#include "hyperclient/hyperclient.h"

// Note that there is no room for completedop to carry message contents.  If a
// message is successful, it should not be stored in a completedop.  completedop
// is used only for operations which just convey a status.

class hyperclient::completedop
{
    public:
        completedop(e::intrusive_ptr<pending> _op,
                    hyperclient_returncode _why,
                    int _error);
        completedop(const completedop& other);
        ~completedop() throw ();

    public:
        completedop& operator = (const completedop& rhs)
        { op = rhs.op; why = rhs.why; return *this; }

    public:
        e::intrusive_ptr<pending> op;
        hyperclient_returncode why;
        int error;
};

inline
hyperclient :: completedop :: completedop(e::intrusive_ptr<pending> _op,
                                          hyperclient_returncode _why,
                                          int _error)
    : op(_op)
    , why(_why)
    , error(_error)
{
}

inline
hyperclient :: completedop :: completedop(const completedop& other)
    : op(other.op)
    , why(other.why)
    , error(other.error)
{
}

inline
hyperclient :: completedop :: ~completedop() throw ()
{
}

#endif // hyperclient_completedop_h_
