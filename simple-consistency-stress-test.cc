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

// The purpose of this test is to expose consistency errors.

// This code does 0 endianness conversion because it should be run on exactly
// one host, and I needed to make it not rely upon the beXXtoh family of
// functions.

// C
#include <cstdlib>
#include <stdint.h>

// Popt
#include <popt.h>

// C++
#include <iostream>

// STL
#include <map>
#include <tr1/memory>
#include <vector>

// po6
#include <po6/net/location.h>
#include <po6/threads/barrier.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/guard.h>

// HyperClient
#include <hyperclient.h>

static long window = 128;
static long repetitions = 1024;
static int64_t threads = 16;
static const char* space = "consistency";
static const char* host = "127.0.0.1";
static po6::net::ipaddr coord(host);
static long port = 1234;
static std::auto_ptr<po6::threads::barrier> barrier;
static po6::threads::mutex results_lock;
static int done = 0;
static std::map<hyperclient_returncode, uint64_t> failed_puts;
static std::map<hyperclient_returncode, uint64_t> failed_loops;
static std::map<hyperclient_returncode, uint64_t> ops;
static uint64_t failed_writes = 0;
static uint64_t inconsistencies = 0;

extern "C"
{

static struct poptOption popts[] = {
    POPT_AUTOHELP
    {"window-size", 'w', POPT_ARG_LONG, &window, 'w',
        "the number of sequential keys which will be used for the test",
        "keys"},
    {"repetitiions", 'r', POPT_ARG_LONG, &repetitions, 'r',
        "the number of tests which will be run before exiting",
        "number"},
    {"threads", 't', POPT_ARG_LONG, &threads, 't',
        "the number of threads which will check for inconsistencies",
        "number"},
    {"space", 's', POPT_ARG_STRING, &space, 's',
        "the HyperDex space to use",
        "space"},
    {"host", 'h', POPT_ARG_STRING, &host, 'h',
        "the IP address of the coordinator",
        "IP"},
    {"port", 'p', POPT_ARG_LONG, &port, 'p',
        "the port number of the coordinator",
        "port"},
    POPT_TABLEEND
};

} // extern "C"

static void
writer_thread();
static void
reader_thread();

int
main(int argc, const char* argv[])
{
    poptContext poptcon;
    poptcon = poptGetContext(NULL, argc, argv, popts, POPT_CONTEXT_POSIXMEHARDER);
    e::guard g = e::makeguard(poptFreeContext, poptcon);
    g.use_variable();
    int rc;

    while ((rc = poptGetNextOpt(poptcon)) != -1)
    {
        switch (rc)
        {
            case 'w':
                if (window < 0)
                {
                    std::cerr << "window-size must be >= 0" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 'r':
                if (repetitions < 0)
                {
                    std::cerr << "repetitions must be >= 0" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 't':
                if (threads < 0)
                {
                    std::cerr << "threads must be >= 0" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 's':
                break;
            case 'h':
                try
                {
                    coord = po6::net::ipaddr(host);
                }
                catch (po6::error& e)
                {
                    std::cerr << "cannot parse coordinator address" << std::endl;
                    return EXIT_FAILURE;
                }
                catch (std::invalid_argument& e)
                {
                    std::cerr << "cannot parse coordinator address" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 'p':
                if (port >= (1 << 16))
                {
                    std::cerr << "port number out of range for TCP" << std::endl;
                    return EXIT_FAILURE;
                }

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

    barrier.reset(new po6::threads::barrier(threads + 1));
    po6::threads::thread writer(writer_thread);
    writer.start();
    std::vector<std::tr1::shared_ptr<po6::threads::thread> > readers;

    for (int64_t i = 0; i < threads; ++i)
    {
        std::tr1::shared_ptr<po6::threads::thread> tptr(new po6::threads::thread(reader_thread));
        readers.push_back(tptr);
        tptr->start();
    }

    writer.join();

    bool success = __sync_bool_compare_and_swap(&done, 0, 1);
    assert(success);

    for (int64_t i = 0; i < threads; ++i)
    {
        readers[i]->join();
    }

    po6::threads::mutex::hold hold(&results_lock);
    typedef std::map<hyperclient_returncode, uint64_t>::iterator result_iter_t;
    std::cout << "Failed puts:" << std::endl;

    for (result_iter_t o = failed_puts.begin(); o != failed_puts.end(); ++o)
    {
        std::cout << o->first << "\t" << o->second << std::endl;
    }

    std::cout << std::endl << "Failed loops:" << std::endl;

    for (result_iter_t o = failed_loops.begin(); o != failed_loops.end(); ++o)
    {
        std::cout << o->first << "\t" << o->second << std::endl;
    }

    std::cout << std::endl << "Normal ops:" << std::endl;

    for (result_iter_t o = ops.begin(); o != ops.end(); ++o)
    {
        std::cout << o->first << "\t" << o->second << std::endl;
    }

    std::cout << std::endl << "failed writes:  " << failed_writes << std::endl
              << "Inconsistencies:  " << inconsistencies << std::endl;
    return EXIT_SUCCESS;
}

static void
writer_thread()
{
    std::map<hyperclient_returncode, uint64_t> lfailed_puts;
    std::map<hyperclient_returncode, uint64_t> lfailed_loops;
    std::map<hyperclient_returncode, uint64_t> lops;
    uint64_t lfailed_writes = 0;
    hyperclient cl(host, port);
    bool fail = false;

    for (int64_t i = 0; i < window; ++i)
    {
        int64_t key = i;
        int64_t did;
        hyperclient_returncode dstatus;

        const char* keystr = reinterpret_cast<const char*>(&key);
        did = cl.del(space, keystr, sizeof(key), &dstatus);

        if (did < 0)
        {
            std::cerr << "delete failed with " << dstatus << std::endl;
            fail = true;
            break;
        }

        int64_t lid;
        hyperclient_returncode lstatus;
        lid = cl.loop(-1, &lstatus);

        if (lid < 0)
        {
            std::cerr << "loop failed with " << lstatus << std::endl;
            fail = true;
            break;
        }

        assert(lid == did);

        if (dstatus != HYPERCLIENT_SUCCESS && dstatus != HYPERCLIENT_NOTFOUND)
        {
            std::cerr << "delete returned " << dstatus << std::endl;
            fail = true;
            break;
        }
    }

    barrier->wait();

    if (fail)
    {
        std::cerr << "the above errors are fatal" << std::endl;
        return;
    }

    std::cout << "starting the consistency stress test" << std::endl;

    for (int64_t r = 0; r < repetitions; ++r)
    {
        for (int64_t i = 0; i < window; ++i)
        {
            uint64_t count = 0;

            for (count = 0; count < 65536; ++count)
            {
                int64_t key = i;
                int64_t val = r;
                int64_t pid;
                hyperclient_attribute attr;
                hyperclient_returncode pstatus;

                const char* keystr = reinterpret_cast<const char*>(&key);
                attr.attr = "repetition";
                attr.value = reinterpret_cast<const char*>(&val);
                attr.value_sz = sizeof(val);
                attr.datatype = HYPERDATATYPE_STRING;
                pid = cl.put(space, keystr, sizeof(key), &attr, 1, &pstatus);

                if (pid < 0)
                {
                    ++lfailed_puts[pstatus];
                    continue;
                }

                int64_t lid;
                hyperclient_returncode lstatus;
                lid = cl.loop(-1, &lstatus);

                if (lid < 0)
                {
                    ++lfailed_loops[lstatus];
                    continue;
                }

                assert(lid == pid);
                ++lops[pstatus];
                break;
            }

            if (count == 65536)
            {
                ++lfailed_writes;
            }
        }

        std::cout << "done " << r << "/" << repetitions << std::endl;
    }

    po6::threads::mutex::hold hold(&results_lock);
    typedef std::map<hyperclient_returncode, uint64_t>::iterator result_iter_t;

    for (result_iter_t o = lfailed_puts.begin(); o != lfailed_puts.end(); ++o)
    {
        failed_puts[o->first] += o->second;
    }

    for (result_iter_t o = lfailed_loops.begin(); o != lfailed_loops.end(); ++o)
    {
        failed_loops[o->first] += o->second;
    }

    for (result_iter_t o = lops.begin(); o != lops.end(); ++o)
    {
        ops[o->first] += o->second;
    }

    failed_writes = lfailed_writes;
}

static void
reader_thread()
{
    std::map<hyperclient_returncode, uint64_t> lfailed_puts;
    std::map<hyperclient_returncode, uint64_t> lfailed_loops;
    std::map<hyperclient_returncode, uint64_t> lops;
    uint64_t linconsistencies = 0;
    hyperclient cl(host, port);
    barrier->wait();

    while (!__sync_bool_compare_and_swap(&done, 1, 1))
    {
        for (int64_t i = window - 1; i >= 0; --i)
        {
            int64_t oldval = 0;

            while (true)
            {
                int64_t key = i;
                int64_t gid;
                hyperclient_attribute* attrs = NULL;
                size_t attrs_sz = 0;
                hyperclient_returncode gstatus;

                const char* keystr = reinterpret_cast<const char*>(&key);
                gid = cl.get(space, keystr, sizeof(key), &gstatus, &attrs, &attrs_sz);

                if (gid < 0)
                {
                    ++lfailed_puts[gstatus];
                    continue;
                }

                int64_t lid;
                hyperclient_returncode lstatus;
                lid = cl.loop(-1, &lstatus);

                if (lid < 0)
                {
                    ++lfailed_loops[lstatus];
                    continue;
                }

                assert(lid == gid);
                ++lops[gstatus];

                if (gstatus == HYPERCLIENT_SUCCESS)
                {
                    assert(attrs_sz == 1);
                    assert(strcmp(attrs[0].attr, "repetition") == 0);
                    int64_t val = 0;
                    memmove(&val, attrs[0].value, attrs[0].value_sz);
                    assert(attrs[0].datatype == HYPERDATATYPE_STRING);

                    if (val < oldval)
                    {
                        ++inconsistencies;
                    }

                    oldval = val;
                }

                if (attrs)
                {
                    hyperclient_destroy_attrs(attrs, attrs_sz);
                }

                break;
            }
        }
    }

    po6::threads::mutex::hold hold(&results_lock);
    typedef std::map<hyperclient_returncode, uint64_t>::iterator result_iter_t;

    for (result_iter_t o = lfailed_puts.begin(); o != lfailed_puts.end(); ++o)
    {
        failed_puts[o->first] += o->second;
    }

    for (result_iter_t o = lfailed_loops.begin(); o != lfailed_loops.end(); ++o)
    {
        failed_loops[o->first] += o->second;
    }

    for (result_iter_t o = lops.begin(); o != lops.end(); ++o)
    {
        ops[o->first] += o->second;
    }

    inconsistencies += linconsistencies;
}
