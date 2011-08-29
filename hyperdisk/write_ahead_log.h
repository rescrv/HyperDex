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

#ifndef hyperdisk_write_ahead_log_h_
#define hyperdisk_write_ahead_log_h_

// e
#include <e/buffer.h>
#include <e/locking_iterable_fifo.h>

// HyperDisk
#include "hyperdisk/returncode.h"

// Forward Declarations
namespace hyperdisk
{
class write_ahead_log_reference;
}

namespace hyperdisk
{

class write_ahead_log
{
    public:
        write_ahead_log();
        ~write_ahead_log() throw ();

    public:
        write_ahead_log_reference reference();
        returncode put(const e::buffer& key, uint64_t key_hash,
                       const std::vector<e::buffer>& value,
                       const std::vector<uint64_t>& value_hashes,
                       uint64_t version);
        returncode del(const e::buffer& key, uint64_t key_hash);

    public:
        // All of the functions in this block return meaningless values if
        // has_records is false.
        bool has_records();
        const e::buffer& key();
        uint64_t key_hash();
        bool has_value();
        const std::vector<e::buffer>& value();
        const std::vector<uint64_t>& value_hashes();
        uint64_t version();

    private:
        friend class write_ahead_log_reference;

    private:
        class log_entry;

    private:
        e::locking_iterable_fifo<log_entry> m_log;
};

class write_ahead_log_reference
{
    public:
        write_ahead_log_reference(const write_ahead_log_reference&);
        ~write_ahead_log_reference() throw ();

    public:
        returncode get(const e::buffer& key, uint64_t key_hash,
                       std::vector<e::buffer>* value, uint64_t* version);

    public:
        write_ahead_log_reference& operator = (const write_ahead_log_reference&);

    private:
        friend class write_ahead_log;

    private:
        write_ahead_log_reference(const e::locking_iterable_fifo<write_ahead_log::log_entry>::iterator& it);

    private:
        e::locking_iterable_fifo<write_ahead_log::log_entry>::iterator m_it;
};

} // namespace hyperdisk

#endif // hyperdisk_write_ahead_log_h_
