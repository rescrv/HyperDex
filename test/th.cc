// Copyright (c) 2013, Robert Escriva
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
//     * Neither the name of th nor the names of its contributors may be used to
//       endorse or promote products derived from this software without specific
//       prior written permission.
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
#include <cstring>

// C++
#include <iostream>

// STL
#include <algorithm>
#include <vector>

// th
#include "th.h"

static std::vector<th::test_base*>* _th_tests = NULL;

class escape_from_test_failure
{
    public:
        escape_from_test_failure() {}
};

th :: test_base :: test_base(const char* group,
                             const char* name,
                             const char* file,
                             size_t line)
    : m_group(group)
    , m_name(name)
    , m_file(file)
    , m_line(line)
{
    if (_th_tests == NULL)
    {
        _th_tests = new std::vector<th::test_base*>();
    }

    _th_tests->push_back(this);
}

void
th :: test_base :: run(bool quiet, bool* failed)
{
    if (!quiet) std::cerr << "===== Test " << m_group << "::" << m_name << " @ " << m_file << ":" << m_line << std::endl;

    try
    {
        this->_run();
        *failed = false;
    }
    catch (escape_from_test_failure& ttotf)
    {
        *failed = true;
    }
}

bool
th :: test_base :: operator < (const test_base& rhs) const
{
    return compare(rhs) < 0;
}

int
th :: test_base :: compare(const test_base& rhs) const
{
    const test_base& lhs(*this);
    int cmp;

    // Compare file
    cmp = strcmp(lhs.m_file, rhs.m_file);

    if (cmp < 0)
    {
        return -1;
    }
    if (cmp > 0)
    {
        return 1;
    }

    // Compare line
    if (lhs.m_line < rhs.m_line)
    {
        return -1;
    }
    if (lhs.m_line > rhs.m_line)
    {
        return 1;
    }

    // Compare group
    cmp = strcmp(lhs.m_group, rhs.m_group);

    if (cmp < 0)
    {
        return -1;
    }
    if (cmp > 0)
    {
        return 1;
    }

    // Compare name
    cmp = strcmp(lhs.m_name, rhs.m_name);

    if (cmp < 0)
    {
        return -1;
    }
    if (cmp > 0)
    {
        return 1;
    }

    return 0;
}

th :: predicate :: predicate(const char* file, size_t line, const char* a, const char* b)
    : m_file(file)
    , m_line(line)
    , m_a(a)
    , m_b(b)
{
}

void
th :: predicate :: assert_true(bool T)
{
    if (!T)
    {
        std::cerr << "FAIL @ " << m_file << ":" << m_line << ": tested " << m_a << "; expected true, but got false" << std::endl;
        th::fail();
    }
}

void
th :: predicate :: assert_false(bool F)
{
    if (F)
    {
        std::cerr << "FAIL @ " << m_file << ":" << m_line << ": tested " << m_a << "; expected false, but got true" << std::endl;
        th::fail();
    }
}

void
th :: predicate :: fail()
{
    std::cerr << "FAIL @ " << m_file << ":" << m_line << ": forced fail" << std::endl;
    th::fail();
}

static bool
compare_test_base_ptrs(const th::test_base* lhs, const th::test_base* rhs)
{
    return *lhs < *rhs;
}

int
th :: run_tests(bool quiet)
{
    if (!_th_tests)
    {
        return 0;
    }

    std::sort(_th_tests->begin(), _th_tests->end(), compare_test_base_ptrs);
    const std::vector<th::test_base*>& th_tests(*_th_tests);
    int failures = 0;

    for (size_t i = 0; i < th_tests.size(); ++i)
    {
        bool fail = false;
        th_tests[i]->run(quiet, &fail);

        if (fail)
        {
            ++failures;
        }
    }

    return failures;
}

#pragma GCC diagnostic ignored "-Wsuggest-attribute=noreturn"

void
th :: fail()
{
    escape_from_test_failure eftf;
    throw eftf;
}
