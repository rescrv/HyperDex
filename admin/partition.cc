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

// C
#include <cmath>

// HyperDex
#include "admin/partition.h"

static void
generate_points(uint64_t intervals,
                std::vector<uint64_t>* lbs,
                std::vector<uint64_t>* ubs)
{
    uint64_t interval = (0x8000000000000000ULL / intervals) * 2;

    for (uint64_t i = 0; i < intervals; ++i)
    {
        lbs->push_back(i * interval);
    }

    for (size_t i = 1; i < lbs->size(); ++i)
    {
        ubs->push_back((*lbs)[i] - 1);
    }

    ubs->push_back(UINT64_MAX);
    assert(lbs->size() == ubs->size());
}

void
recursively_generate(size_t idx, const std::vector<uint64_t>& dimensions,
                     uint64_t bigger, const std::vector<uint64_t>& bigger_lbs, const std::vector<uint64_t>& bigger_ubs,
                     uint64_t smaller, const std::vector<uint64_t>& smaller_lbs, const std::vector<uint64_t>& smaller_ubs,
                     std::vector<uint64_t>* lower_coord, std::vector<uint64_t>* upper_coord,
                     std::vector<hyperdex::region>* regions)
{
    assert(dimensions.size() == lower_coord->size());
    assert(lower_coord->size() == upper_coord->size());

    if (idx >= dimensions.size())
    {
        regions->push_back(hyperdex::region());
        regions->back().lower_coord = *lower_coord;
        regions->back().upper_coord = *upper_coord;
    }
    else
    {
        const std::vector<uint64_t>* lbs;
        const std::vector<uint64_t>* ubs;

        if (dimensions[idx] == bigger)
        {
            lbs = &bigger_lbs;
            ubs = &bigger_ubs;
        }
        else
        {
            assert(dimensions[idx] == smaller);
            lbs = &smaller_lbs;
            ubs = &smaller_ubs;
        }

        for (size_t i = 0; i < lbs->size(); ++i)
        {
            (*lower_coord)[idx] = (*lbs)[i];
            (*upper_coord)[idx] = (*ubs)[i];
            recursively_generate(idx + 1, dimensions,
                                 bigger, bigger_lbs, bigger_ubs,
                                 smaller, smaller_lbs, smaller_ubs,
                                 lower_coord, upper_coord, regions);
        }
    }
}

void
hyperdex :: partition(uint16_t num_attrs, uint32_t num_servers, std::vector<region>* regions)
{
    assert(num_attrs > 0);
    double attrs_per_dimension(num_servers);
    attrs_per_dimension = pow(attrs_per_dimension, 1/double(num_attrs));
    std::vector<uint64_t> dimensions(num_attrs, uint64_t(attrs_per_dimension));
    uint64_t partitions = dimensions.size() * dimensions[0];

    for (size_t i = 0; partitions < num_servers && i < num_attrs; ++i)
    {
        partitions = partitions / dimensions[i];
        ++dimensions[i];
        partitions = partitions * dimensions[i];
    }

    uint64_t bigger = dimensions[0];
    std::vector<uint64_t> bigger_lbs;
    std::vector<uint64_t> bigger_ubs;
    generate_points(bigger, &bigger_lbs, &bigger_ubs);
    uint64_t smaller = dimensions[dimensions.size() - 1];
    std::vector<uint64_t> smaller_lbs;
    std::vector<uint64_t> smaller_ubs;
    generate_points(smaller, &smaller_lbs, &smaller_ubs);

    regions->clear();
    std::vector<uint64_t> lower_coord(num_attrs, 0);
    std::vector<uint64_t> upper_coord(num_attrs, 0);

    recursively_generate(0, dimensions,
                         bigger, bigger_lbs, bigger_ubs,
                         smaller, smaller_lbs, smaller_ubs,
                         &lower_coord, &upper_coord, regions);
}
