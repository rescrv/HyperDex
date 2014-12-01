// Copyright (c) 2014, Cornell University
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

#ifndef _INCLUDE_DOCUMENT_PATH_
#define _INCLUDE_DOCUMENT_PATH_

#include <string>

namespace hyperdex
{
namespace doc
{

// This class represents a flat path
// e.g '$.cornell.cs.systems'
class path
{
public:
    path(const char* cstr)
        : pathstr(cstr)
    {}

    path(const std::string& path_ = "")
        : pathstr(path_)
    {}

    bool empty() const
    {
        return str().size() == 0;
    }

    size_t size() const
    {
        return str().size();
    }

    void append(const std::string& elem)
    {
        if(empty())
        {
            pathstr = elem;
        }
        else
        {
            pathstr += "." + elem;
        }
    }

    // Does this path have a subtree?
    // If not is is just the name of a single element
    bool has_subtree() const
    {
        return pathstr.find(".") != std::string::npos;
    }

    // Split path at the first .
    bool split(std::string& root_name, path& subtree) const
    {
        size_t pos = pathstr.find('.');

        if(pos == std::string::npos)
        {
            return false;
        }

        root_name = pathstr.substr(0, pos);
        subtree.pathstr = pathstr.substr(pos+1);

        return true;
    }

    // Split path at the last '.'
    // i.e. Retrieve last element in the path
    bool split_reverse(path& parent_path, std::string& child_name) const
    {
        size_t pos = pathstr.find_last_of('.');

        if(pos == std::string::npos)
        {
            return false;
        }

        child_name = pathstr.substr(pos+1);
        parent_path.pathstr = pathstr.substr(0, pos);
        return true;
    }

    // Get a string representation of the path
    const std::string& str() const
    {
        return pathstr;
    }

    // Get a cstring representation of the path
    const char* c_str() const
    {
        return pathstr.c_str();
    }

private:
    std::string pathstr;
};

}
}

#endif

