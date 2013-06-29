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

static bool _hyperdex = true;
static bool _leveldb = false;

static struct poptOption popts[] = {
    POPT_AUTOHELP
    {"hyperdex", 0, POPT_ARG_NONE, NULL, 'H',
     "benchmark HyperDex (default: yes)", NULL},
    {"leveldb", 0, POPT_ARG_NONE, NULL, 'L',
     "benchmark LevelDB (default: no)", NULL},
    CONNECT_TABLE
    POPT_TABLEEND
};

static int
perform_hyperdex(void* _cl,
                 const char* k, size_t k_sz,
                 const char* v, size_t v_sz)
{
    struct hyperclient* cl = static_cast<struct hyperclient*>(_cl);
    hyperclient_attribute attr;
    attr.attr = "v";
    attr.value = v;
    attr.value_sz = v_sz;
    attr.datatype = HYPERDATATYPE_STRING;
    hyperclient_returncode rstatus;
    int64_t rid = hyperclient_put(cl, "kv", k, k_sz, &attr, 1, &rstatus);

    if (rid < 0)
    {
        fprintf(stderr, "hyperclient_put encountered %d\n", rstatus);
        return EXIT_FAILURE;
    }

    hyperclient_returncode lstatus;
    int64_t lid = hyperclient_loop(cl, -1, &lstatus);

    if (lid < 0)
    {
        fprintf(stderr, "hyperclient_loop encountered %d\n", lstatus);
        return EXIT_FAILURE;
    }

    if (lid != rid)
    {
        fprintf(stderr, "hyperclient_loop returned wrong id\n");
        return EXIT_FAILURE;
    }

    if (rid < 0)
    {
        fprintf(stderr, "hyperclient_loop encountered %d\n", rstatus);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

static int
perform_leveldb(void* _db,
                const char* k, size_t k_sz,
                const char* v, size_t v_sz)
{
    leveldb::WriteOptions opts;
    leveldb::DB* db = static_cast<leveldb::DB*>(_db);
    leveldb::Status st = db->Put(opts, leveldb::Slice(k, k_sz), leveldb::Slice(v, v_sz));

    if (!st.ok())
    {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

static int
process_file(const char* filename,
             int (*func)(void* ptr, 
                         const char* k, size_t k_sz,
                         const char* v, size_t v_sz),
             void* ptr)
{
    FILE* fin = fopen(filename, "r");

    if (!fin)
    {
        fprintf(stderr, "could not open %s: %s\n", filename, strerror(errno));
        return EXIT_FAILURE;
    }

    uint64_t t_start = e::time();
    uint64_t processed = 0;

    while (true)
    {
        char* line = NULL;
        size_t line_sz = 0;
        ssize_t amt = getline(&line, &line_sz, fin);

        if (amt < 0)
        {
            if (ferror(fin) != 0)
            {
                fprintf(stderr, "could not read from %s: %s\n", filename, strerror(ferror(fin)));
                fclose(fin);
                return EXIT_FAILURE;
            }

            if (feof(fin) != 0)
            {
                break;
            }

            fprintf(stderr, "unknown error when reading from %s\n", filename);
            fclose(fin);
            return EXIT_FAILURE;
        }

        if (amt < 1)
        {
            free(line);
            fprintf(stderr, "skipping blank line in %s\n", filename);
            continue;
        }

        /* wipe out the newline */
        line[amt - 1] = '\0';
        char* tmp = strchr(line, ' ');

        if (!tmp)
        {
            free(line);
            fprintf(stderr, "skipping invalid line in %s\n", filename);
            continue;
        }

        *tmp = '\0';

        /* issue a "put" operation */
        char* k = line;
        size_t k_sz = tmp - k;
        char* v = tmp + 1;
        size_t v_sz = (line + amt - 1) - v;
        int rc = (*func)(ptr, k, k_sz, v, v_sz);
        free(line);

        if (rc != EXIT_SUCCESS)
        {
            return rc;
        }

        ++processed;
    }

    uint64_t t_end = e::time();
    fclose(fin);
    double tput = processed;
    tput = tput / ((t_end - t_start) / 1000000000.);
    fprintf(stdout, "processd %s: %ld ops in %f seconds = %f ops/second\n", filename, processed, (t_end - t_start) / 1000000000., tput);
    return EXIT_SUCCESS;
}

int
main(int argc, const char* argv[])
{
    poptContext poptcon;
    poptcon = poptGetContext(NULL, argc, argv, popts, POPT_CONTEXT_POSIXMEHARDER);
    e::guard g = e::makeguard(poptFreeContext, poptcon); g.use_variable();
    poptSetOtherOptionHelp(poptcon, "[OPTIONS]");
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
            case 'H':
                _hyperdex = true;
                _leveldb = false;
                break;
            case 'L':
                _leveldb = true;
                _hyperdex = false;
                break;
            case POPT_ERROR_NOARG:
            case POPT_ERROR_BADOPT:
            case POPT_ERROR_BADNUMBER:
            case POPT_ERROR_OVERFLOW:
                std::cerr << poptStrerror(rc) << " " << poptBadOption(poptcon, 0) << std::endl;
                return EXIT_FAILURE;
            case POPT_ERROR_OPTSTOODEEP:
            case POPT_ERROR_BADQUOTE:
            case POPT_ERROR_ERRNO:
            default:
                std::cerr << "logic error in argument parsing" << std::endl;
                return EXIT_FAILURE;
        }
    }

    const char** args = poptGetArgs(poptcon);

    try
    {
        int (*func)(void* ptr, 
                    const char* k, size_t k_sz,
                    const char* v, size_t v_sz) = NULL;
        void* ptr = NULL;

        if (_hyperdex)
        {
            func = perform_hyperdex;
            ptr = hyperclient_create(_connect_host, _connect_port);
        }

        if (_leveldb)
        {
            func = perform_leveldb;
            leveldb::Options opts;
            opts.create_if_missing = true;
            //opts.write_buffer_size = 64ULL * 1024ULL * 1024ULL;
            //opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
            leveldb::DB* tmp_db;
            leveldb::Status st = leveldb::DB::Open(opts, ".", &tmp_db);

            if (!st.ok())
            {
                std::cerr << "could not open LevelDB: " << st.ToString() << std::endl;
                return EXIT_FAILURE;
            }

            ptr = tmp_db;
        }

        assert(func);

        for (size_t i = 0; args && args[i]; ++i)
        {
            rc = process_file(args[i], func, ptr);

            if (rc != EXIT_SUCCESS)
            {
                return rc;
            }
        }

        if (_hyperdex)
        {
            hyperclient_destroy(static_cast<hyperclient*>(ptr));
        }

        if (_leveldb)
        {
            delete static_cast<leveldb::DB*>(ptr);
        }

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
