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
#include <tr1/memory>
#include <vector>

// po6
#include <po6/net/ipaddr.h>
#include <po6/threads/barrier.h>
#include <po6/threads/thread.h>

// e
#include <e/convert.h>
#include <e/locking_fifo.h>
#include <e/timer.h>

// HyperDex
#include <hyperdex/client.h>

static e::locking_fifo<std::string> lines;
static po6::threads::mutex unblock_lock;
static po6::threads::cond unblock(&unblock_lock);
static po6::net::ipaddr ip;
static uint16_t port;
static std::string space;
static uint16_t num_threads;

static int
usage();

static void
worker(po6::threads::barrier*);

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

    po6::threads::barrier bar(num_threads + 1);

    try
    {
        std::vector<std::tr1::shared_ptr<po6::threads::thread> > threads;

        for (size_t i = 0; i < num_threads; ++i)
        {
            std::tr1::shared_ptr<po6::threads::thread> t(new po6::threads::thread(std::tr1::bind(worker, &bar)));
            t->start();
            threads.push_back(t);
        }

        e::stopwatch stopwatch;
        bar.wait();
        stopwatch.start();
        std::string line;
        size_t i = 0;

        while (std::getline(std::cin, line))
        {
            lines.push(line);

            if (lines.size() > 10000)
            {
                e::sleep_ms(0, 1);
            }

            ++i;
        }

        bar.wait();

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

    return EXIT_SUCCESS;
}

int
usage()
{
    std::cerr << "Usage:  hyperdex-cli <coordinator ip> <coordinator port> <space name> <threads>"
              << std::endl;
    return EXIT_FAILURE;
}

void
worker(po6::threads::barrier* bar)
{
    hyperdex::client cl(po6::net::location(ip, port));
    cl.connect();
    std::string line;

    bar->wait();

    while (lines.pop(&line))
    {
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
                case hyperdex::WRONGARITY:
                    std::cerr << "Get returned WRONGARITY." << std::endl;
                    break;
                case hyperdex::NOTASPACE:
                    std::cerr << "Get returned NOTASPACE." << std::endl;
                    break;
                case hyperdex::COORDFAIL:
                    std::cerr << "Get returned COORDFAIL." << std::endl;
                    break;
                case hyperdex::SERVERERROR:
                    std::cerr << "Get returned SERVERERROR." << std::endl;
                    break;
                case hyperdex::CONNECTFAIL:
                    std::cerr << "Get returned CONNECTFAIL." << std::endl;
                    break;
                case hyperdex::DISCONNECT:
                    std::cerr << "Get returned DISCONNECT." << std::endl;
                    break;
                case hyperdex::RECONFIGURE:
                    std::cerr << "Get returned RECONFIGURE." << std::endl;
                    break;
                case hyperdex::LOGICERROR:
                    std::cerr << "Get returned LOGICERROR." << std::endl;
                    break;
                default:
                    std::cerr << "Get returned unknown status." << std::endl;
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
                    break;
                case hyperdex::NOTFOUND:
                    std::cerr << "Put returned NOTFOUND." << std::endl;
                    break;
                case hyperdex::WRONGARITY:
                    std::cerr << "Put returned WRONGARITY." << std::endl;
                    break;
                case hyperdex::NOTASPACE:
                    std::cerr << "Put returned NOTASPACE." << std::endl;
                    break;
                case hyperdex::COORDFAIL:
                    std::cerr << "Put returned COORDFAIL." << std::endl;
                    break;
                case hyperdex::SERVERERROR:
                    std::cerr << "Put returned SERVERERROR." << std::endl;
                    break;
                case hyperdex::CONNECTFAIL:
                    std::cerr << "Put returned CONNECTFAIL." << std::endl;
                    break;
                case hyperdex::DISCONNECT:
                    std::cerr << "Put returned DISCONNECT." << std::endl;
                    break;
                case hyperdex::RECONFIGURE:
                    std::cerr << "Put returned RECONFIGURE." << std::endl;
                    break;
                case hyperdex::LOGICERROR:
                    std::cerr << "Put returned LOGICERROR." << std::endl;
                    break;
                default:
                    std::cerr << "Put returned unknown status." << std::endl;
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
                case hyperdex::WRONGARITY:
                    std::cerr << "Del returned WRONGARITY." << std::endl;
                    break;
                case hyperdex::NOTASPACE:
                    std::cerr << "Del returned NOTASPACE." << std::endl;
                    break;
                case hyperdex::COORDFAIL:
                    std::cerr << "Del returned COORDFAIL." << std::endl;
                    break;
                case hyperdex::SERVERERROR:
                    std::cerr << "Del returned SERVERERROR." << std::endl;
                    break;
                case hyperdex::CONNECTFAIL:
                    std::cerr << "Del returned CONNECTFAIL." << std::endl;
                    break;
                case hyperdex::DISCONNECT:
                    std::cerr << "Del returned DISCONNECT." << std::endl;
                    break;
                case hyperdex::RECONFIGURE:
                    std::cerr << "Del returned RECONFIGURE." << std::endl;
                    break;
                case hyperdex::LOGICERROR:
                    std::cerr << "Del returned LOGICERROR." << std::endl;
                    break;
                default:
                    std::cerr << "Del returned unknown status." << std::endl;
            }
        }
        else if (command == "QUERY")
        {
            std::string attr;
            std::string val;
            bool wellformed = true;
            std::map<std::string, e::buffer> search;

            while (std::getline(istr, attr, '\t'))
            {
                if (!std::getline(istr, val, '\t'))
                {
                    wellformed = false;
                }

                search.insert(std::make_pair(attr, e::buffer(val.c_str(), val.size())));
            }

            if (wellformed)
            {
                hyperdex::client::search_results r;

                switch (cl.search(space, search, &r))
                {
                    case hyperdex::SUCCESS:
                    case hyperdex::NOTFOUND:
                        break;
                    case hyperdex::WRONGARITY:
                        std::cerr << "Del returned WRONGARITY." << std::endl;
                        break;
                    case hyperdex::NOTASPACE:
                        std::cerr << "Del returned NOTASPACE." << std::endl;
                        break;
                    case hyperdex::COORDFAIL:
                        std::cerr << "Del returned COORDFAIL." << std::endl;
                        break;
                    case hyperdex::SERVERERROR:
                        std::cerr << "Del returned SERVERERROR." << std::endl;
                        break;
                    case hyperdex::CONNECTFAIL:
                        std::cerr << "Del returned CONNECTFAIL." << std::endl;
                        break;
                    case hyperdex::DISCONNECT:
                        std::cerr << "Del returned DISCONNECT." << std::endl;
                        break;
                    case hyperdex::RECONFIGURE:
                        std::cerr << "Del returned RECONFIGURE." << std::endl;
                        break;
                    case hyperdex::LOGICERROR:
                        std::cerr << "Del returned LOGICERROR." << std::endl;
                        break;
                    default:
                        std::cerr << "Del returned unknown status." << std::endl;
                }

                for (size_t i = 0; i < 1000 && r.valid(); ++i, r.next())
                    ;
            }
        }
        else
        {
            std::cerr << "Unknown commands cannot be acted upon.";
        }
    }

    bar->wait();
}
