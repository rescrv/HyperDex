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

#ifndef hyperdex_mapper_h_
#define hyperdex_mapper_h_

// STL
#include <vector>

// HyperDex
#include <hyperdex/query.h>

namespace hyperdex
{

class mapper
{
    public:
        // Dimensions is a vector <d1, d2, d3, ...> which specifies the number
        // of entities in each dimension of the hyperspace.
        mapper(const std::vector<size_t>& dimensions);
        ~mapper();

    public:
        // Map the specified row into a hyperspace represented by dimensions
        // assuming there exist only modulus entities in the space.
        void map(const hyperdex::query& query,
                 size_t modulus,
                 std::vector<size_t>* result) const;

    private:
        void uniquify(std::vector<size_t>* vec) const;

        void map(const std::vector<size_t>& coord,
                 const std::vector<bool>& mask,
                 size_t dimension,
                 size_t modulus,
                 std::vector<size_t>* result) const;

    private:
        std::vector<size_t> m_dimensions;
        std::vector<size_t> m_offsets;
};

} // namespace hyperdex

#endif // hyperdex_mapper_h_
