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

// POSIX
#include <errno.h>

// STL
#include <string>
#include <vector>

// po6
#include <po6/net/ipaddr.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/convert.h>
#include <e/lockfree_fifo.h>
#include <e/timer.h>

// HyperDex
#include <hyperdex/client.h>

static e::lockfree_fifo<std::string> lines;
static size_t blocked = 0;
static po6::threads::mutex unblock_lock;
static po6::threads::cond unblock(&unblock_lock);
static bool done = false;
static po6::net::ipaddr ip;
static uint16_t port;
static std::string space;
static uint16_t num_threads;

static int
usage();

static void
worker();

int
main(int argc, char* argv[])
{
    if (argc != 5)
    {
        return usage();
    }

    space = argv[3];

    try
    {
        ip = argv[1];
    }
    catch (std::invalid_argument& e)
    {
        std::cerr << "The IP address must be an IPv4 or IPv6 address." << std::endl;
        return EXIT_FAILURE;
    }

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
        num_threads = e::convert::to_uint16_t(argv[4]);
    }
    catch (std::domain_error& e)
    {
        std::cerr << "The number of threads must be an integer." << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::out_of_range& e)
    {
        std::cerr << "The number of threads must be suitably small." << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        // XXX Create a map of valuename -> valueindex

        std::vector<std::tr1::shared_ptr<po6::threads::thread> > threads;

        for (size_t i = 0; i < num_threads; ++i)
        {
            std::tr1::shared_ptr<po6::threads::thread> t(new po6::threads::thread(worker));
            t->start();
            threads.push_back(t);
        }

        unblock_lock.lock();
        while (blocked != num_threads)
        {
            unblock_lock.unlock();
            std::cerr << "blocked=" << blocked << ", num_threads=" << num_threads << std::endl;
            e::sleep_ms(0, 100);
            unblock_lock.lock();
        }
        unblock_lock.unlock();

        std::cerr << "starting..." << std::endl;

        e::stopwatch stopwatch;
        stopwatch.start();
        size_t count = 0;
        std::string line;
        blocked = 0;

        while (std::getline(std::cin, line))
        {
            if (!std::cin)
            {
                break;
            }

            ++count;
            lines.push(line);

            if (count == 1000)
            {
                unblock.broadcast();
            }
        }

        if (count <= 1000)
        {
            unblock.broadcast();
        }

        done = true;
        __sync_synchronize();

        while (blocked != num_threads)
        {
            e::sleep_ms(0, 10);
            __sync_synchronize();
        }

        uint64_t nanosecs = stopwatch.peek();
        std::cerr << "test took " << nanosecs << " nanoseconds." << std::endl;

        for (std::vector<std::tr1::shared_ptr<po6::threads::thread> >::iterator t = threads.begin();
                t != threads.end(); ++t)
        {
            (*t)->join();
        }
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

}

int
usage()
{
    std::cerr << "Usage:  hyperdex-cli <coordinator ip> <coordinator port> <space name> <threads>"
              << std::endl;
    return EXIT_FAILURE;
}

void
worker()
{
    bool finish = false;
    hyperdex::client cl(po6::net::location(ip, port));
    std::string line;

    {
        po6::threads::mutex::hold hold(&unblock_lock);
        ++blocked;
        unblock.wait();
    }

    while (!finish)
    {
        while (!lines.pop(&line))
        {
            e::sleep_ms(0, 1);
            __sync_synchronize();

            if (done)
            {
                finish = true;
                break;
            }
        }

        if (finish)
        {
            continue;
        }

        std::istringstream istr(line);
        std::string command = "";
        std::getline(istr, command, '\t');

        if (command == "GET")
        {
            std::string tmp;

            if (!std::getline(istr, tmp, '\t'))
            {
                std::cerr << "Could not read key for " << line << std::endl;
            }

            e::buffer key(tmp.c_str(), tmp.size());
            std::vector<e::buffer> value;

            switch (cl.get(space, key, &value))
            {
                case hyperdex::SUCCESS:
                case hyperdex::NOTFOUND:
                    break;
                case hyperdex::INVALID:
                    std::cerr << "GET returned INVALID" << std::endl;
                    break;
                case hyperdex::ERROR:
                    std::cerr << "GET returned ERROR" << std::endl;
                    break;
                case hyperdex::DISKFULL:
                    std::cerr << "GET returned DISKFULL" << std::endl;
                    break;
                default:
                    std::cerr << "GET returned unknown response" << std::endl;
                    break;
            }
        }
        else if (command == "PUT")
        {
            std::string tmp;

            if (!std::getline(istr, tmp, '\t'))
            {
                std::cerr << "Could not read key for " << line << std::endl;
            }

            e::buffer key(tmp.c_str(), tmp.size());
            std::vector<e::buffer> value;

            while (std::getline(istr, tmp, '\t'))
            {
                e::buffer val(tmp.c_str(), tmp.size());
                value.push_back(e::buffer());
                value.back().swap(val);
            }

            switch (cl.put(space, key, value))
            {
                case hyperdex::SUCCESS:
                case hyperdex::NOTFOUND:
                    break;
                case hyperdex::INVALID:
                    std::cerr << "PUT returned INVALID" << std::endl;
                    break;
                case hyperdex::ERROR:
                    std::cerr << "PUT returned ERROR" << std::endl;
                    break;
                case hyperdex::DISKFULL:
                    std::cerr << "PUT returned DISKFULL" << std::endl;
                    break;
                default:
                    std::cerr << "PUT returned unknown response" << std::endl;
                    break;
            }
        }
        else if (command == "DEL")
        {
            std::string tmp;

            if (!std::getline(istr, tmp, '\t'))
            {
                std::cerr << "Could not read key for " << line << std::endl;
            }

            e::buffer key(tmp.c_str(), tmp.size());

            switch (cl.del(space, key))
            {
                case hyperdex::SUCCESS:
                case hyperdex::NOTFOUND:
                    break;
                case hyperdex::INVALID:
                    std::cerr << "DEL returned INVALID" << std::endl;
                    break;
                case hyperdex::ERROR:
                    std::cerr << "DEL returned ERROR" << std::endl;
                    break;
                case hyperdex::DISKFULL:
                    std::cerr << "DEL returned DISKFULL" << std::endl;
                    break;
                default:
                    std::cerr << "DEL returned unknown response" << std::endl;
                    break;
            }
        }
        else if (command == "QUERY")
        {
            std::string arity;
            std::string attr;
            std::string val;
            bool wellformed = true;

            if (!std::getline(istr, arity, '\t'))
            {
                wellformed = false;
            }

            hyperdex::search s(e::convert::to_uint16_t(arity));

            while (wellformed && std::getline(istr, attr, '\t'))
            {
                if (!std::getline(istr, val, '\t'))
                {
                    wellformed = false;
                }

                uint16_t ind = e::convert::to_uint16_t(attr.c_str());
                s.set(ind, e::buffer(val.c_str(), val.size()));
            }

            if (wellformed)
            {
                hyperdex::client::search_results r = cl.search(space, s);

                for (size_t i = 0; i < 1000 && r.valid(); ++i, r.next())
                    ;
            }
        }
        else
        {
            std::cerr << "Unknown commands cannot be acted upon.";
        }
    }

    std::cout << "DONE" << std::endl;
    po6::threads::mutex::hold hold(&unblock_lock);
    ++blocked;
}
