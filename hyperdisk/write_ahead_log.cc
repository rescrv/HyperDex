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

// HyperDisk
#include "write_ahead_log.h"

class hyperdisk::write_ahead_log::log_entry
{
    public:
        log_entry();
        log_entry(const e::buffer& key,
                  uint64_t key_hash,
                  const std::vector<e::buffer>& value,
                  const std::vector<uint64_t>& value_hashes,
                  uint64_t version);
        log_entry(const e::buffer& key,
                  uint64_t key_hash);

    public:
        bool has_value;
        e::buffer key;
        uint64_t key_hash;
        std::vector<e::buffer> value;
        std::vector<uint64_t> value_hashes;
        uint64_t version;
};

hyperdisk :: write_ahead_log :: log_entry :: log_entry()
    : has_value()
    , key()
    , key_hash()
    , value()
    , value_hashes()
    , version()
{
}

hyperdisk :: write_ahead_log :: log_entry :: log_entry(const e::buffer& k,
                                                       uint64_t kh,
                                                       const std::vector<e::buffer>& v,
                                                       const std::vector<uint64_t>& vh,
                                                       uint64_t vr)
    : has_value(true)
    , key(k)
    , key_hash(kh)
    , value(v)
    , value_hashes(vh)
    , version(vr)
{
}

hyperdisk :: write_ahead_log :: log_entry :: log_entry(const e::buffer& k,
                                                       uint64_t kh)
    : has_value(false)
    , key(k)
    , key_hash(kh)
    , value()
    , value_hashes()
    , version()
{
}

hyperdisk :: write_ahead_log :: write_ahead_log()
    : m_log()
{
}

hyperdisk :: write_ahead_log :: ~write_ahead_log() throw ()
{
}

hyperdisk::write_ahead_log_reference
hyperdisk :: write_ahead_log :: reference()
{
    return write_ahead_log_reference(m_log.iterate());
}

hyperdisk::returncode
hyperdisk :: write_ahead_log :: put(const e::buffer& k, uint64_t kh,
                                    const std::vector<e::buffer>& v,
                                    const std::vector<uint64_t>& vh,
                                    uint64_t vr)
{
    m_log.append(log_entry(k, kh, v, vh, vr));
    return SUCCESS;
}

hyperdisk::returncode
hyperdisk :: write_ahead_log :: del(const e::buffer& k, uint64_t kh)
{
    m_log.append(log_entry(k, kh));
    return SUCCESS;
}

bool
hyperdisk :: write_ahead_log :: has_records()
{
    return !m_log.empty();
}

const e::buffer&
hyperdisk :: write_ahead_log :: key()
{
    return m_log.oldest().key;
}

uint64_t
hyperdisk :: write_ahead_log :: key_hash()
{
    return m_log.oldest().key_hash;
}

bool
hyperdisk :: write_ahead_log :: has_value()
{
    return m_log.oldest().has_value;
}

const std::vector<e::buffer>&
hyperdisk :: write_ahead_log :: value()
{
    return m_log.oldest().value;
}

const std::vector<uint64_t>&
hyperdisk :: write_ahead_log :: value_hashes()
{
    return m_log.oldest().value_hashes;
}

uint64_t
hyperdisk :: write_ahead_log :: version()
{
    return m_log.oldest().version;
}

hyperdisk :: write_ahead_log_iterator :: write_ahead_log_iterator (const write_ahead_log_iterator& other)
    : m_it(other.m_it)
{
}

hyperdisk :: write_ahead_log_iterator :: ~write_ahead_log_iterator () throw ()
{
}

bool
hyperdisk :: write_ahead_log_iterator :: valid()
{
    return m_it.valid();
}

void
hyperdisk :: write_ahead_log_iterator :: next()
{
    return m_it.next();
}

bool
hyperdisk :: write_ahead_log_iterator :: has_value()
{
    return m_it->has_value;
}

uint64_t
hyperdisk :: write_ahead_log_iterator :: version()
{
    return m_it->version;
}

e::buffer&
hyperdisk :: write_ahead_log_iterator :: key()
{
    return m_it->key;
}

std::vector<e::buffer>&
hyperdisk :: write_ahead_log_iterator :: value()
{
    return m_it->value;
}

hyperdisk :: write_ahead_log_reference :: write_ahead_log_reference(const write_ahead_log_reference& other)
    : m_it(other.m_it)
{
}

hyperdisk :: write_ahead_log_reference :: ~write_ahead_log_reference() throw ()
{
}

hyperdisk::returncode
hyperdisk :: write_ahead_log_reference :: get(const e::buffer& key,
                                              uint64_t key_hash,
                                              std::vector<e::buffer>* value,
                                              uint64_t* version)
{
    hyperdisk::returncode ret = NOTFOUND;
    std::vector<e::buffer> tmp_value;
    uint64_t tmp_version;

    for (; m_it.valid(); m_it.next())
    {
        if (m_it->key_hash == key_hash && m_it->key == key)
        {
            if (m_it->has_value)
            {
                tmp_value = m_it->value;
                tmp_version = m_it->version;
                ret = SUCCESS;
            }
            else
            {
                ret = NOTFOUND;
            }
        }
    }

    if (ret == SUCCESS)
    {
        *value = tmp_value;
        *version = tmp_version;
    }

    return ret;
}

hyperdisk::write_ahead_log_reference&
hyperdisk :: write_ahead_log_reference :: operator = (const write_ahead_log_reference& rhs)
{
    m_it = rhs.m_it;
    return *this;
}

hyperdisk :: write_ahead_log_reference :: write_ahead_log_reference(const e::locking_iterable_fifo<write_ahead_log::log_entry>::iterator& it)
    : m_it(it)
{
}
