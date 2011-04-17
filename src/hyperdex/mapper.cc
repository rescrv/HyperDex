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

// STL
#include <algorithm>

// HyperDex
#include "hyperdex/mapper.h"
#include "hyperdex/city.h"

hyperdex :: mapper :: mapper(const std::vector<size_t>& dimensions)
    : m_dimensions(dimensions)
    , m_offsets(dimensions)
{
    size_t i = 1;
    m_offsets.push_back(i);

    // This little loop will store the number of cells in the submatrix formed
    // by dimenisions i-n in m_offsets[i].  m_offsets[0] will indicate the
    // number of cells in the hyperspace.
    for (std::vector<size_t>::reverse_iterator p = m_offsets.rbegin();
            p != m_offsets.rend(); ++p)
    {
        i *= *p;
        *p = i;
    }
}

hyperdex :: mapper :: ~mapper()
{
}

void
hyperdex :: mapper :: map(const hyperdex::query& query,
                          size_t modulus,
                          std::vector<size_t>* result)
                      const
{
    // Find a coordinate/mask which matches the query.
    std::vector<size_t> coord(query.size(), 0);
    std::vector<bool> mask(query.size(), false);

    for (size_t i = 0; i < query.size(); ++i)
    {
        if (query.is_specified(i))
        {
            const std::string& s(query.value(i));
            mask[i] = true;
            coord[i] = CityHash64(s.c_str(), s.size()) % m_dimensions[i];
        }
    }

    std::vector<size_t> positions;
    map(coord, mask, 0, modulus, &positions);

    for (size_t i = 0; i < positions.size(); ++i)
    {
        positions[i] %= modulus;
    }

    uniquify(&positions);
    result->swap(positions);
}

void
hyperdex :: mapper :: uniquify(std::vector<size_t>* vec)
                      const
{
    std::sort(vec->begin(), vec->end());
    std::vector<size_t>::iterator end = std::unique(vec->begin(), vec->end());
    std::vector<size_t> unique(vec->begin(), end);
    vec->swap(unique);
}

void
hyperdex :: mapper :: map(const std::vector<size_t>& coord,
                          const std::vector<bool>& mask,
                          size_t dimension,
                          size_t modulus,
                          std::vector<size_t>* result)
                      const
{
    // Recursive base case
    if (dimension == m_dimensions.size() - 1)
    {
        // If the query specifies a subset of this dimension.
        if (mask[dimension])
        {
            result->push_back(coord[dimension] % modulus);
        }
        // Else the query specifies this entire dimension.
        else
        {
            for (size_t i = 0; i < m_dimensions[dimension]; ++i)
            {
                result->push_back(i % modulus);
            }
        }
    }
    // Else recursive case
    else
    {
        map(coord, mask, dimension + 1, modulus, result);

        // Each inner array contains C = m_offsets[dimension + 1] cells.
        // Assuming our dimension describes indices [0, C, 2C, 3C, ...] we may
        // compute all cells for this dimension by using the indices for our
        // dimension added to each of the indices from the next dimension.
        // Using modular arithmetic, we avoid growing to an array of size larger
        // than modulus.
        //
        // This is a rather efficient way to do it, and ends up running in
        // $O(\product m_dimensions)$.  It also has the nice property that it
        // becomes more efficient (in practice) with each additional query term.

        // If the query specifies a subset of this dimension.
        if (mask[dimension])
        {
            size_t iC = coord[dimension] * m_offsets[dimension + 1];

            for (size_t i = 0; i < result->size(); ++i)
            {
                // Turn each number $n$ into $iC + n \mod modulus$.
                (*result)[i] = (iC + (*result)[i]) % modulus;
            }
        }
        // Else the query specifies this entire dimension.
        else
        {
            std::vector<size_t> newresult;

            for (size_t i = 0; i < m_dimensions[dimension]; ++i)
            {
                size_t iC = i * m_offsets[dimension + 1];

                for (size_t j = 0; j < result->size(); ++j)
                {
                    newresult.push_back((iC + (*result)[j]) % modulus);
                }
            }

            newresult.swap(*result);
        }
    }

    uniquify(result);
}
