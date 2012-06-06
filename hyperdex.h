/* Copyright (c) 2012, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of HyperDex nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef hyperdex_h_
#define hyperdex_h_

/* This file includes enums and constants that are used throughout HyperDex. */

/* Datatype occupies [8960, 9088) */
enum hyperdatatype
{
    HYPERDATATYPE_STRING    = 8960,
    HYPERDATATYPE_INT64     = 8961,

    // List types
    HYPERDATATYPE_LIST_GENERIC = 8976,
    HYPERDATATYPE_LIST_STRING  = 8977,
    HYPERDATATYPE_LIST_INT64   = 8978,

    // Set types
    HYPERDATATYPE_SET_GENERIC = 8992,
    HYPERDATATYPE_SET_STRING  = 8993,
    HYPERDATATYPE_SET_INT64   = 8994,

    // Map types
    HYPERDATATYPE_MAP_GENERIC        = 9008,
    HYPERDATATYPE_MAP_STRING_KEYONLY = 9024,
    HYPERDATATYPE_MAP_STRING_STRING  = 9025,
    HYPERDATATYPE_MAP_STRING_INT64   = 9026,
    HYPERDATATYPE_MAP_INT64_KEYONLY  = 9040,
    HYPERDATATYPE_MAP_INT64_STRING   = 9041,
    HYPERDATATYPE_MAP_INT64_INT64    = 9042,

    // Returned if the server acts up
    HYPERDATATYPE_GARBAGE   = 9087
};

#ifdef __cplusplus

// C++
#include <iostream>

#define str(x) #x
#define xstr(x) str(x)
#define stringify(x) case (x): lhs << xstr(x); break

inline std::ostream&
operator << (std::ostream& lhs, hyperdatatype rhs)
{
    switch (rhs)
    {
        stringify(HYPERDATATYPE_STRING);
        stringify(HYPERDATATYPE_INT64);
        stringify(HYPERDATATYPE_LIST_GENERIC);
        stringify(HYPERDATATYPE_LIST_STRING);
        stringify(HYPERDATATYPE_LIST_INT64);
        stringify(HYPERDATATYPE_SET_GENERIC);
        stringify(HYPERDATATYPE_SET_STRING);
        stringify(HYPERDATATYPE_SET_INT64);
        stringify(HYPERDATATYPE_MAP_GENERIC);
        stringify(HYPERDATATYPE_MAP_STRING_KEYONLY);
        stringify(HYPERDATATYPE_MAP_STRING_STRING);
        stringify(HYPERDATATYPE_MAP_STRING_INT64);
        stringify(HYPERDATATYPE_MAP_INT64_KEYONLY);
        stringify(HYPERDATATYPE_MAP_INT64_STRING);
        stringify(HYPERDATATYPE_MAP_INT64_INT64);
        stringify(HYPERDATATYPE_GARBAGE);
        default:
            lhs << "DATATYPE UNKNOWN";
            break;
    }

    return lhs;
}

#undef stringify
#undef xstr
#undef str

#endif /* __cplusplus */

#endif /* hyperdex_h_ */
