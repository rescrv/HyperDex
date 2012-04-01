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

// STL
#include <algorithm>

// HyperDaemon
#include "hyperdaemon/datatypes.h"

size_t
hyperdaemon :: sizeof_microop(const hyperdex::microop& op)
{
    using namespace hyperdex;

    switch (op.action)
    {
        case OP_FAIL:
            return 0;
        case OP_STRING_SET:
        case OP_STRING_APPEND:
        case OP_STRING_PREPEND:
            return sizeof(uint32_t) + op.argv1_string.size();
        case OP_INT64_SET:
        case OP_INT64_ADD:
        case OP_INT64_SUB:
        case OP_INT64_MUL:
        case OP_INT64_DIV:
        case OP_INT64_MOD:
        case OP_INT64_AND:
        case OP_INT64_OR:
        case OP_INT64_XOR:
            return sizeof(int64_t);
        default:
            return 0;
    }
}

static uint8_t*
apply_string(const e::slice& old_value,
             const hyperdex::microop* ops,
             size_t num_ops,
             uint8_t* writeto,
             hyperdex::network_returncode* error)
{
    using namespace hyperdex;

    e::slice base = old_value;
    bool set_base = false;
    const hyperdex::microop* prepend = NULL;
    const hyperdex::microop* append = NULL;

    for (size_t i = 0; i < num_ops; ++i)
    {
        switch (ops[i].action)
        {
            case OP_STRING_SET:

                if (set_base || prepend || append)
                {
                    *error = NET_BADMICROS;
                    return NULL;
                }

                base = ops[i].argv1_string;
                set_base = true;
                break;
            case OP_STRING_PREPEND:

                if (set_base || prepend)
                {
                    *error = NET_BADMICROS;
                    return NULL;
                }

                prepend = ops + i;
                break;
            case OP_STRING_APPEND:

                if (set_base || append)
                {
                    *error = NET_BADMICROS;
                    return NULL;
                }

                append = ops + i;
                break;
            case OP_INT64_SET:
            case OP_INT64_ADD:
            case OP_INT64_SUB:
            case OP_INT64_MUL:
            case OP_INT64_DIV:
            case OP_INT64_MOD:
            case OP_INT64_AND:
            case OP_INT64_OR:
            case OP_INT64_XOR:
            case OP_FAIL:
            default:
                *error = NET_BADMICROS;
                return NULL;
        }
    }

    if (prepend)
    {
        memmove(writeto, prepend->argv1_string.data(), prepend->argv1_string.size());
        writeto += prepend->argv1_string.size();
    }

    memmove(writeto, base.data(), base.size());
    writeto += base.size();

    if (append)
    {
        memmove(writeto, append->argv1_string.data(), append->argv1_string.size());
        writeto += append->argv1_string.size();
    }

    return writeto;
}

static uint8_t*
apply_int64(const e::slice& old_value,
            const hyperdex::microop* ops,
            size_t num_ops,
            uint8_t* writeto,
            hyperdex::network_returncode* error)
{
    using namespace hyperdex;

    int64_t number = 0;
    memmove(&number, old_value.data(), std::min(old_value.size(), sizeof(number)));
    number = le64toh(number);

    for (size_t i = 0; i < num_ops; ++i)
    {
        const hyperdex::microop* op = ops + i;

        switch (op->action)
        {
            case OP_INT64_SET:
                number = op->argv1_int64;
                break;
            case OP_INT64_ADD:
                number += op->argv1_int64;
                break;
            case OP_INT64_SUB:
                number -= op->argv1_int64;
                break;
            case OP_INT64_MUL:
                number *= op->argv1_int64;
                break;
            case OP_INT64_DIV:
                number /= op->argv1_int64;
                break;
            case OP_INT64_MOD:
                number %= op->argv1_int64;
                break;
            case OP_INT64_AND:
                number &= op->argv1_int64;
                break;
            case OP_INT64_OR:
                number |= op->argv1_int64;
                break;
            case OP_INT64_XOR:
                number ^= op->argv1_int64;
                break;
            case OP_STRING_SET:
            case OP_STRING_APPEND:
            case OP_STRING_PREPEND:
            case OP_FAIL:
            default:
                *error = NET_BADMICROS;
                return NULL;
        }
    }

    number = htole64(number);
    memmove(writeto, &number, sizeof(number));
    return writeto + sizeof(number);
}

uint8_t*
hyperdaemon :: apply_microops(hyperdex::datatype type,
                              const e::slice& old_value,
                              const hyperdex::microop* ops,
                              size_t num_ops,
                              uint8_t* writeto,
                              hyperdex::network_returncode* error)
{
    using namespace hyperdex;

    switch (type)
    {
        case DATATYPE_STRING:
            return apply_string(old_value, ops, num_ops, writeto, error);
        case DATATYPE_INT64:
            return apply_int64(old_value, ops, num_ops, writeto, error);
        default:
            *error = NET_BADMICROS;
            return NULL;
    }
}
