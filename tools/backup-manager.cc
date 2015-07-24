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

// POSIX
#include <errno.h>
#include <sys/types.h>
#include <sys/wait.h>

// STL
#include <string>

// po6
#include <po6/io/fd.h>

// HyperDex
#include <hyperdex/admin.hpp>
#include "tools/common.h"

using hyperdex::connect_opts;

struct daemon_backup
{
    daemon_backup() : sid(), addr(), path() {}
    daemon_backup(uint64_t s,
                  const std::string& a,
                  const std::string& p)
        : sid(s), addr(a), path(p) {}
    ~daemon_backup() {}
    uint64_t sid;
    std::string addr;
    std::string path;
};

static bool
get_time(std::string* now)
{
    time_t tmp_t;
    struct tm* tmp_tm;

    tmp_t = time(NULL);
    tmp_tm = localtime(&tmp_t);

    if (tmp_tm == NULL)
    {
        perror("could not read time");
        return false;
    }

    char tmp[1024];

    if (strftime(tmp, 1024, "%Y-%m-%dT%H:%M:%S", tmp_tm) == 0)
    {
        std::cerr << "could not read time" << std::endl;
        return false;
    }

    *now = std::string(tmp);
    return true;
}

static bool
take_backup(const connect_opts& conn,
            const char* name,
            std::vector<daemon_backup>* daemons)
{
    hyperdex::Admin h(conn.host(), conn.port());
    hyperdex_admin_returncode rrc;
    const char* backups = NULL;
    int64_t rid = h.backup(name, &rrc, &backups);

    if (rid < 0)
    {
        std::cerr << "Backup failed.\n"
                  << "Please read the above messages for the cause and potential solutions\n";
        return false;
    }

    hyperdex_admin_returncode lrc;
    int64_t lid = h.loop(-1, &lrc);

    if (lid < 0)
    {
        std::cerr << "Backup failed.\n"
                  << "Please read the above messages for the cause and potential solutions\n";
        return false;
    }

    assert(rid == lid);

    if (rrc != HYPERDEX_ADMIN_SUCCESS)
    {
        std::cerr << "backup failed: " << h.error_message() << std::endl;
        return false;
    }

    if (!backups)
    {
        return true;
    }

    const char* ptr = backups;
    const char* end = ptr + strlen(ptr);

    while (ptr < end)
    {
        const char* eol = strchr(ptr, '\n');
        eol = eol ? eol : end;
        std::string line(ptr, eol);
        ptr = eol + 1;
        long long unsigned int num;
        std::vector<char> loc_buf(line.size());
        std::vector<char> path_buf(line.size());

        if (sscanf(line.c_str(), "%llu %s %[^\t\n]", 
                   &num, &loc_buf[0], &path_buf[0]) < 3)
        {
            std::cerr << "could not parse backup information for daemons:\n"
                      << backups << std::flush;
            return false;
        }

        daemons->push_back(daemon_backup(num,
                                         std::string(&loc_buf[0]),
                                         std::string(&path_buf[0])));
    }

    return true;
}

static bool
read_latest(const std::string& base, bool* has_previous, std::string* previous)
{
    std::string latest_path = po6::path::join(base, "LATEST");
    po6::io::fd latest(open(latest_path.c_str(), O_RDONLY));

    if (latest.get() < 0 && errno == ENOENT)
    {
        *has_previous = false;
        return true;
    }
    else if (latest.get() < 0)
    {
        return false;
    }

    std::vector<char> buf(PATH_MAX);
    ssize_t amt = latest.xread(&buf[0], buf.size());

    if (amt < 0)
    {
        std::cerr << "could not read the LATEST backup: "
                  << strerror(errno) << std::endl;
        return false;
    }

    buf.resize(amt);
    *has_previous = true;
    *previous = po6::path::join(base, std::string(buf.begin(), buf.end()));
    return true;
}

static bool
record_latest(const std::string& base, const std::string& now)
{
    std::string latest_path = po6::path::join(base, "LATEST");
    po6::io::fd latest(open(latest_path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR));

    if (latest.get() < 0)
    {
        std::cerr << "could not record the backup in LATEST: "
                  << strerror(errno) << std::endl;
        return false;
    }

    if (latest.xwrite(now.c_str(), now.size()) < static_cast<ssize_t>(now.size()))
    {
        std::cerr << "could not record the backup in LATEST: "
                  << strerror(errno) << std::endl;
        return false;
    }

    latest.close();
    return true;
}

static bool
fork_exec_wait(const std::vector<std::string>& args)
{
    pid_t child = fork();

    if (child > 0)
    {
        int status = 0;

        if (waitpid(child, &status, 0) < 0)
        {
            std::cerr << "could not wait for child: " << strerror(errno) << std::endl;
            return false;
        }

        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
        {
            std::cerr << "child process failed" << std::endl;
            return false;
        }

        return true;
    }
    else if (child == 0)
    {
        std::vector<const char*> arg_ptrs;
        arg_ptrs.reserve(args.size() + 1);

        for (size_t i = 0; i < args.size(); ++i)
        {
            arg_ptrs.push_back(args[i].c_str());
        }

        arg_ptrs.push_back(NULL);
        execvp(arg_ptrs[0], const_cast<char* const*>(&arg_ptrs[0]));
        std::cerr << "could not exec: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }
    else
    {
        std::cerr << "could not fork: " << strerror(errno) << std::endl;
        return false;
    }
}

int
main(int argc, const char* argv[])
{
    bool _cleanup = true;
    const char* _data = ".";
    const char* _user = NULL;
    connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.add("Connect to a cluster:", conn.parser());
    ap.arg().name('b', "backup-dir")
            .description("store backups in this directory (default: .)")
            .metavar("dir").as_string(&_data);
    ap.arg().name('u', "user")
            .description("username to use for ssh connections (default: this user)")
            .metavar("user").as_string(&_user);

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

    using po6::path::join;

    try
    {
        bool success = true;
        std::string base(_data);

        if (!po6::path::realpath(base, &base))
        {
            std::cerr << "error: " << po6::strerror(errno) << std::endl;
            return EXIT_FAILURE;
        }

        if (chdir(base.c_str()) < 0)
        {
            std::cerr << "could not change directory to: " << _data << std::endl;
            return EXIT_FAILURE;
        }

        std::string now;
        std::vector<daemon_backup> daemons;

        if (!get_time(&now))
        {
            return EXIT_FAILURE;
        }

        if (!take_backup(conn, now.c_str(), &daemons))
        {
            return EXIT_FAILURE;
        }

        bool has_previous = false;
        std::string previous;

        if (!read_latest(base, &has_previous, &previous))
        {
            return EXIT_FAILURE;
        }

        std::string backupdir(join(base, now));

        if (mkdir(backupdir.c_str(), S_IRWXU) < 0)
        {
            std::cerr << "could not make local directory for the backup: "
                      << strerror(errno) << std::endl;
            return EXIT_FAILURE;
        }

        if (rename(join(base, now + ".coordinator.bin").c_str(),
                   join(backupdir, "coordinator.bin").c_str()) < 0)
        {
            std::cerr << "could not rename coordinator backup: "
                      << strerror(errno) << std::endl;
            return EXIT_FAILURE;
        }

        for (size_t i = 0; i < daemons.size(); ++i)
        {
            char buf[21];
            sprintf(buf, "%lu", daemons[i].sid);
            std::string daemon_dir(join(base, now, buf));
            bool prev = false;
            std::string link_dest;

            if (has_previous)
            {
                struct stat stbuf;
                std::string daemon_prev(join(previous, buf));
                int status = stat(daemon_prev.c_str(), &stbuf);

                if (status < 0 && errno == ENOENT)
                {
                    prev = false;
                }
                else if (status < 0)
                {
                    prev = false;
                    std::cerr << "could not stat prev backup for " << daemons[i].sid << ": "
                              << strerror(errno) << std::endl;
                    success = false;
                }
                else
                {
                    prev = true;
                    link_dest  = "--link-dest=";
                    link_dest += daemon_prev.c_str();
                }
            }

            std::vector<std::string> args;
            args.push_back("rsync");
            args.push_back("-a");
            args.push_back("--delete");

            if (prev)
            {
                args.push_back(link_dest);
            }

            args.push_back("--");
            std::string rsync_url = daemons[i].addr + ":" + daemons[i].path + "/";

            if (_user)
            {
                args.push_back(std::string(_user) + "@" + rsync_url);
            }
            else
            {
                args.push_back(rsync_url);
            }

            args.push_back(daemon_dir.c_str());

            if (!fork_exec_wait(args))
            {
                success = false;
            }
        }

        if (success && _cleanup)
        {
            for (size_t i = 0; i < daemons.size(); ++i)
            {
                std::vector<std::string> args;
                args.push_back("ssh");

                if (_user)
                {
                    args.push_back("-l");
                    args.push_back(_user);
                }

                args.push_back(daemons[i].addr);
                args.push_back("rm");
                args.push_back("-r");
                args.push_back("--");
                args.push_back(daemons[i].path);

                if (!fork_exec_wait(args))
                {
                    success = false;
                }
            }
        }

        if (success && !record_latest(base, now))
        {
            success = false;
        }

        return success ? EXIT_SUCCESS : EXIT_FAILURE;
    }
    catch (std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
