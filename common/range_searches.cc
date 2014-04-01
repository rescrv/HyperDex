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

// STL
#include <algorithm>

// HyperDex
#include "common/datatype_info.h"
#include "common/range_searches.h"

using hyperdex::attribute_check;
using hyperdex::datatype_info;
using hyperdex::range;

// the caller of this function is safe to assume that only checks with
//  * HYPERPREDICATE_EQUAL
//  * HYPERPREDICATE_LESS_THAN
//  * HYPERPREDICATE_LESS_EQUAL
//  * HYPERPREDICATE_GREATER_EQUAL
//  * HYPERPREDICATE_GREATER_THAN
// will be used to construct the returned ranges.  If you break this assumption,
// you will need to look at the way indices are picked in the data layer and the
// way ranges are picked for hyperspace hashing.
static bool
range_search(const attribute_check& check, range* r)
{
    switch (check.predicate)
    {
        case HYPERPREDICATE_EQUALS:
            r->attr = check.attr;
            r->type = check.datatype;
            r->start = check.value;
            r->end = check.value;
            r->has_start = true;
            r->has_end = true;
            r->invalid = false;
            return true;
        case HYPERPREDICATE_LESS_THAN:
        case HYPERPREDICATE_LESS_EQUAL:
            r->attr = check.attr;
            r->type = check.datatype;
            r->end = check.value;
            r->has_start = false;
            r->has_end = true;
            r->invalid = false;
            return true;
        case HYPERPREDICATE_GREATER_EQUAL:
        case HYPERPREDICATE_GREATER_THAN:
            r->attr = check.attr;
            r->type = check.datatype;
            r->start = check.value;
            r->has_start = true;
            r->has_end = false;
            r->invalid = false;
            return true;
        case HYPERPREDICATE_FAIL:
        case HYPERPREDICATE_CONTAINS_LESS_THAN:
        case HYPERPREDICATE_REGEX:
        case HYPERPREDICATE_LENGTH_EQUALS:
        case HYPERPREDICATE_LENGTH_LESS_EQUAL:
        case HYPERPREDICATE_LENGTH_GREATER_EQUAL:
        case HYPERPREDICATE_CONTAINS:
        default:
            return false;
    }
}

static bool
compress_ranges(const range* range_ptr, const range* range_end, range* r)
{
    r->attr = range_ptr->attr;
    r->type = range_ptr->type;
    r->start = e::slice();
    r->end = e::slice();
    r->has_start = false;
    r->has_end = false;
    r->invalid = false;
    datatype_info* di = datatype_info::lookup(range_ptr->type);

    if (!di)
    {
        return false;
    }

    for (; range_ptr < range_end; ++range_ptr)
    {
        // bump/set the start if necessary
        if (range_ptr->has_start && r->has_start)
        {
            if (di->compare(range_ptr->start, r->start) > 0)
            {
                r->start = range_ptr->start;
            }
        }
        else if (range_ptr->has_start)
        {
            r->start = range_ptr->start;
            r->has_start = true;
        }

        // cut/set the end if necessary
        if (range_ptr->has_end && r->has_end)
        {
            if (di->compare(range_ptr->end, r->end) < 0)
            {
                r->end = range_ptr->end;
            }
        }
        else if (range_ptr->has_end)
        {
            r->end = range_ptr->end;
            r->has_end = true;
        }
    }

    if (r->has_start && r->has_end && di->compare(r->start, r->end) > 0)
    {
        r->invalid = true;
    }

    return true;
}

void
hyperdex :: range_searches(const schema& sc,
                           const std::vector<attribute_check>& checks,
                           std::vector<range>* ranges)
{
    std::vector<range> raw_ranges;
    raw_ranges.reserve(checks.size());

    for (size_t i = 0; i < checks.size(); ++i)
    {
        if (checks[i].attr >= sc.attrs_sz ||
            datatype_info::lookup(sc.attrs[checks[i].attr].type)->document())
        {
            continue;
        }

        range r;

        if (range_search(checks[i], &r))
        {
            raw_ranges.push_back(r);
        }
    }

    std::sort(raw_ranges.begin(), raw_ranges.end());
    const range* range_ptr = &raw_ranges.front();
    const range* range_end = range_ptr + raw_ranges.size();
    ranges->clear();

    while (range_ptr < range_end)
    {
        const range* tmp = range_ptr;

        while (tmp < range_end && range_ptr->attr == tmp->attr)
        {
            assert(range_ptr->attr == tmp->attr);
            assert(range_ptr->type == tmp->type);
            ++tmp;
        }

        assert(tmp == range_end || range_ptr->attr < tmp->attr);
        range r;

        if (compress_ranges(range_ptr, tmp, &r))
        {
            ranges->push_back(r);
        }

        range_ptr = tmp;
    }

    assert(ranges->size() <= checks.size());
}
