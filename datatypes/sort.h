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

#ifndef datatypes_sort_h_
#define datatypes_sort_h_

// STL
#include <algorithm>
#ifdef _MSC_VER
#include <functional>
#else
#include <tr1/functional>
#endif

using hyperdex::funcall;

inline bool
compare_by_arg1(int (*compare_elem)(const e::slice& lhs, const e::slice& rhs),
                const funcall& lhs, const funcall& rhs)
{
    return compare_elem(lhs.arg1, rhs.arg1) < 0;
}

inline void
sort_funcalls_by_arg1(funcall* begin, funcall* end,
                      int (*compare_elem)(const e::slice& lhs, const e::slice& rhs))
{
    std::sort(begin, end,
              std::tr1::bind(compare_by_arg1, compare_elem,
                             std::tr1::placeholders::_1,
                             std::tr1::placeholders::_2));
}

inline bool
compare_by_arg2(int (*compare_elem)(const e::slice& lhs, const e::slice& rhs),
                const funcall& lhs, const funcall& rhs)
{
    return compare_elem(lhs.arg2, rhs.arg2) < 0;
}

inline void
sort_funcalls_by_arg2(funcall* begin, funcall* end,
                      int (*compare_elem)(const e::slice& lhs, const e::slice& rhs))
{
    std::sort(begin, end,
              std::tr1::bind(compare_by_arg2, compare_elem,
                             std::tr1::placeholders::_1,
                             std::tr1::placeholders::_2));
}

#endif // datatypes_sort_h_
