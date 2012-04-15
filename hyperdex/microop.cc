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

#include <iostream>

// HyperDex
#include "hyperdex/hyperdex/microop.h"

hyperdex :: microop :: microop()
    : attr(-1)
    , type()
    , action(OP_FAIL)
    , arg1()
    , arg2()
{
}

e::buffer::packer
hyperdex :: operator << (e::buffer::packer lhs, const microop& rhs)
{
    uint16_t type = static_cast<uint16_t>(rhs.type);
    uint8_t action = static_cast<uint8_t>(rhs.action);
    lhs = lhs << rhs.attr << type << action
              << rhs.arg1 << rhs.arg2;
    return lhs;
}

e::buffer::unpacker
hyperdex :: operator >> (e::buffer::unpacker lhs, microop& rhs)
{
    uint16_t type;
    uint8_t action;
    lhs = lhs >> rhs.attr >> type >> action
              >> rhs.arg1 >> rhs.arg2;
    rhs.type = static_cast<hyperdatatype>(type);
    rhs.action = static_cast<hyperdex::microop_type>(action);
    return lhs;
}
