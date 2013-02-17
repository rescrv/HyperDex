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

// STL
#include <sstream>

// HyperDex
#include "client/description.h"

hyperclient :: description :: description(const char** desc)
    : m_ref(0)
    , m_desc(desc)
    , m_msgs()
{
}

hyperclient :: description :: ~description() throw ()
{
}

void
hyperclient :: description :: compile()
{
    std::ostringstream msg;

    for (size_t i = 0; i < m_msgs.size(); ++i)
    {
        msg << m_msgs[i].first << ": " << m_msgs[i].second << "\n";
    }

    std::string out(msg.str());
    char* ret = static_cast<char*>(malloc(out.size() + 1));

    if (ret)
    {
        strncpy(ret, out.c_str(), out.size() + 1);

        for (size_t i = out.size(); i > 0; --i)
        {
            if (ret[i] == '\n' || ret[i] == '\0')
            {
                ret[i] = '\0';
            }
            else
            {
                break;
            }
        }

        *m_desc = ret;
    }
    else
    {
        *m_desc = NULL;
    }
}

bool
hyperclient :: description :: last_reference()
{
    return m_ref == 1;
}

void
hyperclient :: description :: add_text(const hyperdex::virtual_server_id& vid,
                                       const e::slice& text)
{
    m_msgs.push_back(std::make_pair(vid, std::string(reinterpret_cast<const char*>(text.data()), text.size())));
}

void
hyperclient :: description :: add_text(const hyperdex::virtual_server_id& vid,
                                       const char* text)
{
    m_msgs.push_back(std::make_pair(vid, std::string(text)));
}
