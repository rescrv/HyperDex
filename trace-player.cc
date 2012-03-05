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

// C++
#include <fstream>
#include <iostream>

// STL
#include <string>

// BSD
#include <vis.h>

// po6
#include <po6/error.h>
#include <po6/io/fd.h>

// e
#include <e/convert.h>
#include <e/timer.h>

// HyperDex
#include "hyperclient/hyperclient.h"

class incompleteop
{
    public:
        incompleteop(uint64_t ln)
            : lineno(ln), status(), attrs(), attrs_sz(), m_ref(0) {}

    public:
        uint64_t lineno;
        hyperclient_returncode status;
        hyperclient_attribute* attrs;
        size_t attrs_sz;

    private:
        friend class e::intrusive_ptr<incompleteop>;

    private:
        incompleteop(const incompleteop& other);
        ~incompleteop() throw () {}

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        incompleteop& operator = (const incompleteop& rhs);

    private:
        size_t m_ref;
};

static int
usage();

static void
flush(hyperclient* cl,
      size_t outstanding,
      std::map<int64_t, e::intrusive_ptr<incompleteop> >* incomplete_keyops,
      std::map<int64_t, e::intrusive_ptr<incompleteop> >* incomplete_searches);

static void
handle_line(hyperclient* cl,
            const char* space,
            std::map<int64_t, e::intrusive_ptr<incompleteop> >* incomplete_keyops,
            std::map<int64_t, e::intrusive_ptr<incompleteop> >* incomplete_searches,
            uint64_t lineno,
            char* ln,
            char* nl);

int
main(int argc, char* argv[])
{
    if (argc != 6)
    {
        return usage();
    }

    const char* ip = argv[1];
    uint16_t port;
    const char* space = argv[3];
    const char* trace = argv[4];
    uint64_t outstanding = 0;

    try
    {
        port = e::convert::to_uint16_t(argv[2]);
    }
    catch (std::domain_error& e)
    {
        std::cerr << "The port number must be an integer." << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::out_of_range& e)
    {
        std::cerr << "The port number must be suitably small." << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        outstanding = e::convert::to_uint64_t(argv[5]);
    }
    catch (std::domain_error& e)
    {
        std::cerr << "The number of outstanding ops must be an integer." << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::out_of_range& e)
    {
        std::cerr << "The number of outstanding ops must be suitably small." << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        hyperclient cl(ip, port);
        std::fstream fin(trace);

        if (!fin)
        {
            std::cerr << __FILE__ << ":" << __LINE__ << " could not open trace:  " << strerror(errno) << std::endl;
            return EXIT_FAILURE;
        }

        std::map<int64_t, e::intrusive_ptr<incompleteop> > incomplete_keyops;
        std::map<int64_t, e::intrusive_ptr<incompleteop> > incomplete_searches;
        std::string line;
        uint64_t lineno = 0;

        while (std::getline(fin, line))
        {
            ++lineno;
            flush(&cl, outstanding, &incomplete_keyops, &incomplete_searches);
            std::vector<char> buf(line.c_str(), line.c_str() + line.size() + 1);
            handle_line(&cl, space, &incomplete_keyops, &incomplete_searches, lineno,
                        &buf.front(), &buf.back());
        }

        flush(&cl, 0, &incomplete_keyops, &incomplete_searches);
    }
    catch (po6::error& e)
    {
        std::cerr << "There was a system error:  " << e.what();
        return EXIT_FAILURE;
    }
    catch (std::runtime_error& e)
    {
        std::cerr << "There was a runtime error:  " << e.what();
        return EXIT_FAILURE;
    }
    catch (std::bad_alloc& e)
    {
        std::cerr << "There was a memory allocation error:  " << e.what();
        return EXIT_FAILURE;
    }
    catch (std::exception& e)
    {
        std::cerr << "There was a generic error:  " << e.what();
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

int
usage()
{
    std::cerr << "Usage:  trace-player <coordinator ip> <coordinator port> <space name> <trace> <outstanding>" << std::endl;
    return EXIT_FAILURE;
}

static uint64_t ops = 0;
static uint64_t oldtime = 0;

void
flush(hyperclient* cl,
      size_t outstanding,
      std::map<int64_t, e::intrusive_ptr<incompleteop> >* incomplete_keyops,
      std::map<int64_t, e::intrusive_ptr<incompleteop> >* incomplete_searches)
{
    uint64_t newtime = e::time();

    if (newtime - oldtime > 1000000000)
    {
        std::cout << oldtime / 1000000000 << " " << ops << std::endl;
        ops = 0;
        oldtime = newtime;
    }

    while (incomplete_keyops->size() + incomplete_searches->size() > outstanding)
    {
        newtime = e::time();

        if (newtime - oldtime > 1000000000)
        {
            std::cout << oldtime / 1000000000 << " " << ops << std::endl;
            ops = 0;
            oldtime = newtime;
        }

        hyperclient_returncode status;
        int64_t id = cl->loop(10, &status);

        if (id < 0 && status == HYPERCLIENT_TIMEOUT)
        {
            continue;
        }

        if (id < 0)
        {
            std::cerr << __FILE__ << ":" << __LINE__ << " loop error " << status << std::endl;
            break;
        }

        std::map<int64_t, e::intrusive_ptr<incompleteop> >::iterator op;

        if ((op = incomplete_searches->find(id)) != incomplete_searches->end())
        {
            if (op->second->status == HYPERCLIENT_SEARCHDONE)
            {
                incomplete_searches->erase(op);
            }
            else if (op->second->status == HYPERCLIENT_SUCCESS)
            {
                hyperclient_destroy_attrs(op->second->attrs, op->second->attrs_sz);
            }
            else
            {
                std::cerr << __FILE__ << ":" << __LINE__ << " lineno " << op->second->lineno << " encountered error " << op->second->status << std::endl;
            }
        }
        else if ((op = incomplete_keyops->find(id)) != incomplete_keyops->end())
        {
            if (op->second->status == HYPERCLIENT_SUCCESS)
            {
                hyperclient_destroy_attrs(op->second->attrs, op->second->attrs_sz);
            }
            else if (op->second->status == HYPERCLIENT_NOTFOUND)
            {
            }
            else
            {
                std::cerr << __FILE__ << ":" << __LINE__ << " lineno " << op->second->lineno << " encountered error " << op->second->status << std::endl;
            }

            incomplete_keyops->erase(op);
        }
        else
        {
            std::cerr << __FILE__ << ":" << __LINE__ << " id " << id << " seems to be orphaned" << std::endl;
        }

        ++ops;
    }
}

static bool
split_space(std::vector<char>* decoded, std::vector<std::pair<char*, char*> >* words,
            char* ln, char* nl)
{
    char* decode = &decoded->front();

    while (ln < nl)
    {
        if (isspace(*ln))
        {
            ++ln;
            continue;
        }

        char* end = ln;

        while (end < nl && !isspace(*end))
        {
            ++end;
        }

        *end = '\0';
        ssize_t decode_amt = strunvis(decode, ln);

        if (decode_amt < 0)
        {
            return false;
        }

        words->push_back(std::make_pair(decode, decode + decode_amt));
        decode = decode + decode_amt + 1;
        ln = end + 1;
    }

    return true;
}

void
handle_line(hyperclient* cl,
            const char* space,
            std::map<int64_t, e::intrusive_ptr<incompleteop> >* incomplete_keyops,
            std::map<int64_t, e::intrusive_ptr<incompleteop> >* incomplete_searches,
            uint64_t lineno,
            char* ln,
            char* nl)
{
    std::vector<char> decoded(nl - ln + 1);
    std::vector<std::pair<char*, char*> > words;

    if (!split_space(&decoded, &words, ln, nl))
    {
        std::cerr << __FILE__ << ":" << __LINE__ << " syntax error on " << lineno << std::endl;
        return;
    }

    if (words.size() < 1)
    {
        std::cerr << __FILE__ << ":" << __LINE__ << " syntax error on " << lineno << std::endl;
        return;
    }

    if (strcmp(words[0].first, "GET") == 0)
    {
        if (words.size() != 2)
        {
            std::cerr << __FILE__ << ":" << __LINE__ << " syntax error on " << lineno << std::endl;
            return;
        }

        e::intrusive_ptr<incompleteop> op = new incompleteop(lineno);
        int64_t id;

        if ((id = cl->get(space, words[1].first, words[1].second - words[1].first,
                          &op->status, &op->attrs, &op->attrs_sz)) < 0)
        {
            std::cerr << __FILE__ << ":" << __LINE__ << " GET op failed " << op->status << std::endl;
            return;
        }

        incomplete_keyops->insert(std::make_pair(id, op));
    }
    else if (strcmp(words[0].first, "PUT") == 0)
    {
        if (words.size() % 2 != 0)
        {
            std::cerr << __FILE__ << ":" << __LINE__ << " syntax error on " << lineno << std::endl;
            return;
        }

        std::vector<hyperclient_attribute> attrs;

        for (size_t i = 2; i < words.size(); i += 2)
        {
            hyperclient_attribute attr;
            attr.attr = words[i].first;
            attr.value = words[i + 1].first;
            attr.value_sz = words[i + 1].second - words[i + 1].first;
            attr.datatype = HYPERDATATYPE_STRING;
            attrs.push_back(attr);
        }

        e::intrusive_ptr<incompleteop> op = new incompleteop(lineno);
        int64_t id;

        if ((id = cl->put(space, words[1].first, words[1].second - words[1].first,
                          &attrs.front(), attrs.size(), &op->status)) < 0)
        {
            std::cerr << __FILE__ << ":" << __LINE__ << " PUT op failed " << id << ", " << op->status << std::endl;
            return;
        }

        incomplete_keyops->insert(std::make_pair(id, op));
    }
    else if (strcmp(words[0].first, "DEL") == 0)
    {
        if (words.size() != 2)
        {
            std::cerr << __FILE__ << ":" << __LINE__ << " syntax error on " << lineno << std::endl;
            return;
        }

        e::intrusive_ptr<incompleteop> op = new incompleteop(lineno);
        int64_t id;

        if ((id = cl->del(space, words[1].first, words[1].second - words[1].first,
                          &op->status)) < 0)
        {
            std::cerr << __FILE__ << ":" << __LINE__ << " GET op failed " << op->status << std::endl;
            return;
        }

        incomplete_keyops->insert(std::make_pair(id, op));
    }
    else if (strcmp(words[0].first, "SEARCH") == 0)
    {
        abort();
    }
    else
    {
        std::cerr << __FILE__ << ":" << __LINE__ << " syntax error on " << lineno << std::endl;
    }
}
