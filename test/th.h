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

#ifndef th_h_
#define th_h_

// C
#include <cstdlib>

// C++
#include <iostream>

#define TH_XSTR(x) #x
#define TH_STR(x) TH_XSTR(x)
#define TH_XCONCAT(x, y) x ## y
#define TH_CONCAT(x, y) TH_XCONCAT(x, y)

namespace th
{

int
run_tests(bool quiet);

void
fail();

class test_base
{
public:
	test_base(const char *group,
	          const char *name,
	          const char *file,
	          size_t line);
	virtual ~test_base() throw () {}

public:
	void run(bool quiet, bool *failed);

public:
	bool operator < (const test_base &rhs) const;

private:
	virtual void _run() = 0;
	int compare(const test_base &rhs) const;

private:
	test_base(const test_base &);
	test_base &operator = (const test_base &);

private:
	const char *m_group;
	const char *m_name;
	const char *m_file;
	size_t m_line;
};

#define BINARY_PREDICATE(english, compiler) \
	template <typename A, typename B> \
	void \
	assert_ ## english(const A& a, const B& b) \
	{ \
		if (!(a compiler b)) \
		{ \
			std::cerr << "FAIL @ " << m_file << ":" << m_line << ": tested " << m_a << " " TH_XSTR(compiler) " " << m_b << "; got " << a << " " TH_XSTR(compiler) " " << b << std::endl; \
			th::fail(); \
		} \
	}

class predicate
{
public:
	predicate(const char *file,
	          size_t line,
	          const char *a,
	          const char *b);
	void assert_true(bool T);
	void assert_false(bool F);
	BINARY_PREDICATE(lt, < )
	BINARY_PREDICATE(le, <= )
	BINARY_PREDICATE(eq, == )
	BINARY_PREDICATE(ne, != )
	BINARY_PREDICATE(ge, >= )
	BINARY_PREDICATE(gt, > )
	void fail();

private:
	const char *m_file;
	size_t m_line;
	const char *m_a;
	const char *m_b;
};

#undef BINARY_PREDICATE

} // namespace th

#define TEST(GROUP, NAME) \
	class TH_CONCAT(GROUP, TH_CONCAT(_, TH_CONCAT(NAME, TH_CONCAT(_, __LINE__)))) : public th::test_base \
	{ \
	public: \
		TH_CONCAT(GROUP, TH_CONCAT(_, TH_CONCAT(NAME, TH_CONCAT(_, __LINE__))))() \
			: test_base(TH_STR(GROUP), TH_STR(NAME), __FILE__, __LINE__) {} \
	protected: \
		virtual void _run(); \
	}; \
	TH_CONCAT(GROUP, TH_CONCAT(_, TH_CONCAT(NAME, TH_CONCAT(_, __LINE__)))) \
	TH_CONCAT(_test_instance_, TH_CONCAT(GROUP, TH_CONCAT(_, TH_CONCAT(NAME, TH_CONCAT(_, __LINE__))))); \
	void TH_CONCAT(GROUP, TH_CONCAT(_, TH_CONCAT(NAME, TH_CONCAT(_, __LINE__)))) :: _run()

#define ASSERT_TRUE(P)  th::predicate(__FILE__, __LINE__, TH_STR(P), NULL).assert_true(P)
#define ASSERT_FALSE(P) th::predicate(__FILE__, __LINE__, TH_STR(P), NULL).assert_false(P)
#define ASSERT_LT(a, b) th::predicate(__FILE__, __LINE__, TH_STR(a), TH_STR(b)).assert_lt(a, b)
#define ASSERT_LE(a, b) th::predicate(__FILE__, __LINE__, TH_STR(a), TH_STR(b)).assert_le(a, b)
#define ASSERT_EQ(a, b) th::predicate(__FILE__, __LINE__, TH_STR(a), TH_STR(b)).assert_eq(a, b)
#define ASSERT_NE(a, b) th::predicate(__FILE__, __LINE__, TH_STR(a), TH_STR(b)).assert_ne(a, b)
#define ASSERT_GE(a, b) th::predicate(__FILE__, __LINE__, TH_STR(a), TH_STR(b)).assert_ge(a, b)
#define ASSERT_GT(a, b) th::predicate(__FILE__, __LINE__, TH_STR(a), TH_STR(b)).assert_gt(a, b)
#define FAIL() th::predicate(__FILE__, __LINE__, NULL, NULL).fail()

#endif // th_h_
