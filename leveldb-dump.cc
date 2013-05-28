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

// C
#include <cstdlib>

// C++
#include <iostream>

// STL
#include <cctype>
#include <memory>
#include <vector>

// e
#include <e/slice.h>

// LevelDB
#include <hyperleveldb/db.h>
#include <hyperleveldb/filter_policy.h>

void
decode(const char* data, size_t data_sz, std::vector<char>* out)
{
    out->resize(data_sz * 4 + 1);
    char* ptr = &out->front();

    for (size_t i = 0; i < data_sz; ++i)
    {
        if (isalnum(data[i]) ||
            (ispunct(data[i]) && data[i] != '\'') ||
            data[i] == ' ')
        {
            *ptr = data[i];
            ++ptr;
        }
        else if (data[i] == '\n')
        {
            *ptr = '\\';
            ++ptr;
            *ptr = 'n';
            ++ptr;
        }
        else if (data[i] == '\r')
        {
            *ptr = '\\';
            ++ptr;
            *ptr = 'r';
            ++ptr;
        }
        else if (data[i] == '\t')
        {
            *ptr = '\\';
            ++ptr;
            *ptr = 't';
            ++ptr;
        }
        else if (data[i] == '\'')
        {
            *ptr = '\\';
            ++ptr;
            *ptr = '\'';
            ++ptr;
        }
        else
        {
            *ptr = '\\';
            ++ptr;
            *ptr = 'x';
            ++ptr;
            sprintf(ptr, "%02x", data[i] & 0xff);
            ptr += 2;
        }
    }
}

int
main(int argc, const char* argv[])
{
    for (int i = 1; i < argc; ++i)
    {
        std::cout << "dumping db at " << argv[i] << std::endl;
        leveldb::Options oopts;
        oopts.create_if_missing = false;
        oopts.filter_policy = leveldb::NewBloomFilterPolicy(10);
        std::string name(argv[i]);
        leveldb::DB* db;
        leveldb::Status st = leveldb::DB::Open(oopts, name, &db);

        if (!st.ok())
        {
            std::cout << "could not open LevelDB: " << st.ToString() << std::endl;
            continue;
        }

        std::auto_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
        it->SeekToFirst();

        while (it->Valid())
        {
            std::vector<char> k;
            std::vector<char> v;
            decode(it->key().data(), it->key().size(), &k);
            decode(it->value().data(), it->value().size(), &v);
            std::cout << "\"" << &k[0] << "\" -> \"" << &v[0] << "\"\n";
            it->Next();
        }

        std::cout << std::flush;
        it.reset();
        delete db;
    }
}
