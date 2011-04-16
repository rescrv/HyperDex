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

// C
#include <cmath>
#include <cstdio>

// STL
#include <algorithm>
#include <stdexcept>

// HyperDex
#include "hyperdex/bloom.h"
#include "hyperdex/city.h"

hyperdex :: bloom :: bloom(uint64_t capacity,
                           double error_rate)
    : m_bits()
    , m_seeds()
{
    size_t num_bits = - ((capacity * log(error_rate)) / 0.480453);
    size_t num_funcs = std::min(static_cast<size_t>(1),
                                static_cast<size_t>(floor((0.7 * num_bits) / static_cast<double>(capacity))));
    
    // TODO:  Re-evaluate if these seeds are appropriate.
    for (size_t i = 0; i < num_funcs; ++i)
    {
        m_seeds.push_back(i * 514229);
    }

    m_bits.resize(num_bits / 8 + 1, 0);
}

hyperdex :: bloom :: bloom(const std::string& serialized)
    : m_bits()
    , m_seeds()
{
    const size_t header_size = sizeof(uint32_t) + sizeof(uint64_t);

    if (serialized.size() < header_size)
    {
        throw std::invalid_argument("Serialized bloom filter string is too small");
    }

    const char* cstr = serialized.c_str();
    uint64_t num_bytes = *reinterpret_cast<const uint64_t*>(cstr);
    uint32_t num_funcs = *reinterpret_cast<const uint32_t*>(cstr + sizeof(uint64_t));

    if (serialized.size() < header_size + num_bytes + (num_funcs * sizeof(uint32_t)))
    {
        throw std::runtime_error("Serialized bloom filter string is too small");
    }

    std::vector<uint8_t> bitarray(cstr + header_size, cstr + header_size + num_bytes);
    std::vector<uint32_t> seeds(cstr + header_size + num_bytes, cstr + header_size + num_bytes + num_funcs);
    m_bits.swap(bitarray);
    m_seeds.swap(seeds);
}

hyperdex :: bloom :: bloom(const bloom& other)
    : m_bits(other.m_bits)
    , m_seeds(other.m_seeds)
{
}

hyperdex :: bloom :: ~bloom()
{
}

void
hyperdex :: bloom :: add(const std::string& s)
{
    for (size_t i = 0; i < m_seeds.size(); ++i)
    {
        size_t pos = CityHash64WithSeed(s.c_str(), s.size(), m_seeds[i]) % m_bits.size();
        size_t byte = pos / 8;
        uint8_t bit = 1 << (pos % 8);
        __sync_or_and_fetch(&m_bits.front() + byte, bit);
    }
}

bool
hyperdex :: bloom :: check(const std::string& s)
{
    for (size_t i = 0; i < m_seeds.size(); ++i)
    {
        size_t pos = CityHash64WithSeed(s.c_str(), s.size(), m_seeds[i]) % m_bits.size();
        size_t byte = pos / 8;
        uint8_t bit = 1 << (pos % 8);

        if (!(m_bits[byte] & bit))
        {
            return false;
        }
    }

    return true;
}

#define MEMORY_REGION_OF(X) \
    reinterpret_cast<const char*>(&X), reinterpret_cast<const char*>(&X) + sizeof(X)

std::string
hyperdex :: bloom :: serialize()
                     const
{
    std::string ret;
    uint64_t num_bytes = m_bits.size();
    uint32_t num_funcs = m_seeds.size();
    ret += std::string(MEMORY_REGION_OF(num_bytes));
    ret += std::string(MEMORY_REGION_OF(num_funcs));
    ret += std::string(reinterpret_cast<const char*>(&m_bits.front()), m_bits.size() * sizeof(uint8_t));
    ret += std::string(reinterpret_cast<const char*>(&m_seeds.front()), m_seeds.size() * sizeof(uint32_t));
    return ret;
}

hyperdex::bloom&
hyperdex :: bloom :: operator = (const bloom& rhs)
{
    m_bits = rhs.m_bits;
    m_seeds = rhs.m_seeds;
    return *this;
}
