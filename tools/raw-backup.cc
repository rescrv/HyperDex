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
#include <cstdlib>

// e
#include <e/popt.h>

// HyperDex
#include <hyperdex/admin.hpp>

class connect_opts
{
    public:
        connect_opts()
            : m_ap() , m_host("127.0.0.1") , m_port(2012)
        {
            m_ap.arg().name('h', "host")
                      .description("connect to the daemon on an IP address or hostname (default: 127.0.0.1)")
                      .metavar("addr").as_string(&m_host);
            m_ap.arg().name('p', "port")
                      .description("connect to the daemon on an alternative port (default: 2012)")
                      .metavar("port").as_long(&m_port);
        }
        ~connect_opts() throw () {}

    public:
        const e::argparser& parser() { return m_ap; }
        const char* host() { return m_host; }
        uint16_t port() { return m_port; }
        bool validate()
        {
            if (m_port <= 0 || m_port >= (1 << 16))
            {
                std::cerr << "port number to connect to is out of range" << std::endl;
                return false;
            }

            return true;
        }

        private:
            connect_opts(const connect_opts&);
            connect_opts& operator = (const connect_opts&);

    private:
        e::argparser m_ap;
        const char* m_host;
        long m_port;
};

int
main(int argc, const char* argv[])
{
    connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS] <backup-name>");
    ap.add("Connect to a daemon:", conn.parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (!conn.validate())
    {
        std::cerr << "invalid host:port specification\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 1)
    {
        std::cerr << "please specify a name for the backup" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    const char* name = ap.args()[0];

    try
    {
        hyperdex_admin_returncode rc;

        if (hyperdex_admin_raw_backup(conn.host(), conn.port(), name, &rc) < 0)
        {
            std::cerr << "could not take raw backup: " << rc << std::endl;
            return EXIT_FAILURE;
        }

        return EXIT_SUCCESS;
    }
    catch (std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
