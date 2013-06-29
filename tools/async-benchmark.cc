// Copyright (c) 2013, Cornell University
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
//     * Neither the name of Replicant nor the names of its contributors may be
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

// POSIX
#include <errno.h>

// STL
#include <tr1/unordered_map>

// LevelDB
#include <hyperleveldb/db.h>

// po6
#include <po6/error.h>

// e
#include <e/guard.h>
#include <e/time.h>

// HyperDex
#include "client/hyperclient.hpp"
#include "tools/old.common.h"

static struct poptOption popts[] = {
    POPT_AUTOHELP
    CONNECT_TABLE
    POPT_TABLEEND
};

class outstanding
{
    public:
        outstanding();
        ~outstanding() throw ();

    public:
        void reset();

    public:
        char* line;
        size_t line_sz;
        enum hyperclient_returncode status;
        int64_t reqid;

    private:
        outstanding(const outstanding& other);
        outstanding& operator = (const outstanding& other);
};

outstanding :: outstanding()
    : line(NULL)
    , line_sz(0)
    , status(HYPERCLIENT_GARBAGE)
    , reqid(-1)
{
}

outstanding :: ~outstanding() throw ()
{
    reset();
}

void
outstanding :: reset()
{
    if (line)
    {
        free(line);
    }

    line = NULL;
    line_sz = 0;
    status = HYPERCLIENT_GARBAGE;
    reqid = -1;
}

static bool
readline_and_issue_put(const char* filename,
                       FILE* fin,
                       HyperClient* cl,
                       std::tr1::unordered_map<int64_t, size_t>* ops_map,
                       outstanding* o,
                       size_t idx,
                       bool* eof)
{
    while (true)
    {
        assert(o->line == NULL);
        assert(o->line_sz == 0);

        ssize_t amt = getline(&o->line, &o->line_sz, fin);

        if (amt < 0)
        {
            if (ferror(fin) != 0)
            {
                fprintf(stderr, "could not read from %s: %s\n", filename, strerror(ferror(fin)));
                return false;
            }

            if (feof(fin) != 0)
            {
                *eof = true;
                o->reset();
                return true;
            }

            fprintf(stderr, "unknown error when reading from %s\n", filename);
            return false;
        }

        if (amt < 1)
        {
            fprintf(stderr, "skipping blank line in %s\n", filename);
            o->reset();
            continue;
        }

        /* wipe out the newline */
        o->line[amt - 1] = '\0';
        o->line_sz = (size_t)amt;
        char* tmp = strchr(o->line, ' ');

        if (!tmp)
        {
            fprintf(stderr, "skipping invalid line in %s\n", filename);
            o->reset();
            continue;
        }

        *tmp = '\0';

        /* issue a "put" operation */
        char* k = o->line;
        size_t k_sz = tmp - k;
        char* v = tmp + 1;
        size_t v_sz = (o->line + amt - 1) - v;
        hyperclient_attribute attr;
        attr.attr = "v";
        attr.value = v;
        attr.value_sz = v_sz;
        attr.datatype = HYPERDATATYPE_STRING;
        o->reqid = cl->put("kv", k, k_sz, &attr, 1, &o->status);

        if (o->reqid < 0)
        {
            fprintf(stderr, "hyperclient_put encountered %d\n", o->status);
            return false;
        }

        (*ops_map)[o->reqid] = idx;
        return true;
    }
}

static bool
process_file(struct HyperClient* cl,
             struct outstanding* outstanding_ops,
             size_t outstanding_ops_sz,
             const char* filename)
{
    FILE* fin = fopen(filename, "r");

    if (!fin)
    {
        fprintf(stderr, "could not open %s: %s\n", filename, strerror(errno));
        return false;
    }

    std::tr1::unordered_map<int64_t, size_t> ops_map;
    bool eof = false;
    uint64_t processed = 0;
    uint64_t t_start = e::time();

    for (size_t i = 0; !eof && i < outstanding_ops_sz; ++i)
    {
        outstanding* o = &outstanding_ops[i];

        if (!readline_and_issue_put(filename, fin, cl, &ops_map, o, o - outstanding_ops, &eof))
        {
            fclose(fin);
            return false;
        }
    }

    while (!eof || !ops_map.empty())
    {
        hyperclient_returncode rc = HYPERCLIENT_GARBAGE;
        int64_t reqid = cl->loop(-1, &rc);

        if (reqid < 0)
        {
            fprintf(stderr, "hyperclient_loop encountered %d\n", rc);
            fclose(fin);
            return false;
        }

        ++processed;
        std::tr1::unordered_map<int64_t, size_t>::iterator it = ops_map.find(reqid);
        assert(it != ops_map.end());
        outstanding* o = &outstanding_ops[it->second];
        assert(o->reqid == reqid);
        ops_map.erase(it);
        o->reset();

        if (!eof)
        {
            if (!readline_and_issue_put(filename, fin, cl, &ops_map, o, o - outstanding_ops, &eof))
            {
                fclose(fin);
                return false;
            }
        }
    }

    uint64_t t_end = e::time();
    fclose(fin);
    double tput = processed;
    tput = tput / ((t_end - t_start) / 1000000000.);
    fprintf(stdout, "processd %s: %ld ops in %f seconds = %f ops/second\n", filename, processed, (t_end - t_start) / 1000000000., tput);
    return true;
}

int
main(int argc, const char* argv[])
{
    poptContext poptcon = NULL;
    poptcon = poptGetContext(NULL, argc, argv, popts, POPT_CONTEXT_POSIXMEHARDER);
    e::guard g = e::makeguard(poptFreeContext, poptcon); g.use_variable();
    poptSetOtherOptionHelp(poptcon, "[OPTIONS] [--] [<file> ...]");
    int rc;

    while ((rc = poptGetNextOpt(poptcon)) != -1)
    {
        switch (rc)
        {
            case 'h':
                if (!check_host())
                {
                    return EXIT_FAILURE;
                }
                break;
            case 'p':
                if (!check_port())
                {
                    return EXIT_FAILURE;
                }
                break;
            case POPT_ERROR_NOARG:
            case POPT_ERROR_BADOPT:
            case POPT_ERROR_BADNUMBER:
            case POPT_ERROR_OVERFLOW:
                fprintf(stderr, "%s %s\n", poptStrerror(rc), poptBadOption(poptcon, 0));
                return EXIT_FAILURE;
            case POPT_ERROR_OPTSTOODEEP:
            case POPT_ERROR_BADQUOTE:
            case POPT_ERROR_ERRNO:
            default:
                fprintf(stderr, "logic error in argument parsing\n");
                return EXIT_FAILURE;
        }
    }

    const char** args = poptGetArgs(poptcon);

    try
    {
        HyperClient h(_connect_host, _connect_port);
        size_t outstanding_ops_sz = 1024;
        outstanding* outstanding_ops = new outstanding[outstanding_ops_sz];

        for (size_t i = 0; args && args[i]; ++i)
        {
            if (!process_file(&h, outstanding_ops, outstanding_ops_sz, args[i]))
            {
                return EXIT_FAILURE;
            }

            for (size_t j = 0; j < outstanding_ops_sz; ++j)
            {
                outstanding_ops[j].reset();
            }
        }

        delete[] outstanding_ops;
        return EXIT_SUCCESS;
    }
    catch (po6::error& e)
    {
        std::cerr << "system error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
