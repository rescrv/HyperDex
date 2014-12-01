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

#ifndef hyperdex_common_json_path_h_
#define hyperdex_common_json_path_h_

#include "namespace.h"
#include <string>

BEGIN_HYPERDEX_NAMESPACE

// This class represents a flat json path
// e.g '$.cornell.cs.systems'
class json_path
{
public:
    json_path(const char* cstr)
        : path(cstr)
    {}

    json_path(const std::string& path_ = "")
        : path(path_)
    {}

    json_path(const e::slice& slice)
        : path(slice.cdata(), slice.size())
    {}

    bool empty() const
    {
        return str().size() == 0;
    }

    size_t size() const
    {
        return str().size();
    }

    void append(const std::string elem)
    {
        if(empty())
        {
            path = elem;
        }
        else
        {
            path += "." + elem;
        }
    }

    // Does this path have a subtree?
    // If not is is just the name of a single element
    bool has_subtree() const
    {
        return path.find(".") != std::string::npos;
    }

    // Split path at the first .
    bool split(std::string& root_name, json_path& subtree) const
    {
        size_t pos = path.find('.');

        if(pos == std::string::npos)
        {
            return false;
        }

        root_name = path.substr(0, pos);
        subtree.path = path.substr(pos+1);

        return true;
    }

    // Split path at the last '.'
    // i.e. Retrieve last element in the path
    bool split_reverse(json_path& parent_path, std::string& child_name) const
    {
        size_t pos = path.find_last_of('.');

        if(pos == std::string::npos)
        {
            return false;
        }

        child_name = path.substr(pos+1);
        parent_path.path = path.substr(0, pos);
        return true;
    }

    // Get a string representation of the path
    const std::string& str() const
    {
        return path;
    }

private:
    std::string path;
};

END_HYPERDEX_NAMESPACE

#endif
