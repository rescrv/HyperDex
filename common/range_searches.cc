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

#define __STDC_LIMIT_MACROS

// HyperDex
#include "common/range_searches.h"
#include "datatypes/compare.h"

using hyperdex::attribute_check;
using hyperdex::range;

range :: range()
    : attr(UINT16_MAX)
    , type(HYPERDATATYPE_GARBAGE)
    , start()
    , end()
    , has_start(false)
    , has_end(false)
    , invalid(true)
{
}

range :: range(const range& other)
    : attr(other.attr)
    , type(other.type)
    , start(other.start)
    , end(other.end)
    , has_start(other.has_start)
    , has_end(other.has_end)
    , invalid(other.invalid)
{
}

range :: ~range() throw ()
{
}

range&
range :: operator = (const range& rhs)
{
    attr = rhs.attr;
    type = rhs.type;
    start = rhs.start;
    end = rhs.end;
    has_start = rhs.has_start;
    has_end = rhs.has_end;
    invalid = rhs.invalid;
    return *this;
}

static void
range_search(const attribute_check* ptr,
             const attribute_check* end,
             range* r)
{
    assert(ptr < end);
    const attribute_check* lower = NULL;
    const attribute_check* upper = NULL;
    *r = range();
    r->attr = ptr->attr;
    r->type = ptr->datatype;
    r->has_start = false;
    r->has_end = false;
    r->invalid = true;

    while (ptr < end)
    {
        switch (ptr->predicate)
        {
            case HYPERPREDICATE_EQUALS:
                if ((lower && compare_as_type(ptr->value, lower->value, ptr->datatype) < 0) ||
                    (upper && compare_as_type(ptr->value, upper->value, ptr->datatype) > 0))
                {
                    return;
                }
                lower = ptr;
                upper = ptr;
                break;
            case HYPERPREDICATE_LESS_EQUAL:
                if (lower && compare_as_type(ptr->value, lower->value, ptr->datatype) < 0)
                {
                    return;
                }
                if (!upper || compare_as_type(ptr->value, upper->value, ptr->datatype) < 0)
                {
                    upper = ptr;
                }
                break;
            case HYPERPREDICATE_GREATER_EQUAL:
                if (upper && compare_as_type(ptr->value, upper->value, ptr->datatype) > 0)
                {
                    return;
                }
                if (!lower || compare_as_type(ptr->value, lower->value, ptr->datatype) > 0)
                {
                    lower = ptr;
                }
                break;
            case HYPERPREDICATE_CONTAINS_LESS_THAN:
            case HYPERPREDICATE_REGEX:
                break;
            case HYPERPREDICATE_FAIL:
            default:
                return;
        }

        ++ptr;
    }

    // ensure we have found a valid range
    if (lower && upper &&
        compare_as_type(lower->value, upper->value, ptr->datatype) > 0)
    {
        return;
    }

    r->invalid = false;

    if (lower)
    {
        r->has_start = true;
        r->start = lower->value;
    }

    if (upper)
    {
        r->has_end = true;
        r->end = upper->value;
    }
}

bool
hyperdex :: range_searches(const std::vector<attribute_check>& checks,
                           std::vector<range>* ranges)
{
    const attribute_check* check_ptr = &checks.front();
    const attribute_check* check_end = check_ptr + checks.size();
    ranges->clear();

    while (check_ptr < check_end)
    {
        const attribute_check* tmp = check_ptr;

        while (tmp < check_end && check_ptr->attr == tmp->attr)
        {
            if (check_ptr->datatype != tmp->datatype)
            {
                return false;
            }

            ++tmp;
        }

        // assert that the input of checks is sorted
        assert(tmp == check_end || check_ptr->attr < tmp->attr);
        ranges->push_back(range());
        range_search(check_ptr, tmp, &ranges->back());
        check_ptr = tmp;
    }

    return true;
}
