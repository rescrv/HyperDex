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

#ifndef hyperdaemon_replication_manager_deferred
#define hyperdaemon_replication_manager_deferred

class hyperdaemon::replication_manager::deferred
{
    public:
        deferred(const bool has_value,
                 std::auto_ptr<e::buffer> backing,
                 const e::slice& key,
                 const std::vector<e::slice>& value,
                 const hyperdex::entityid& from_ent,
                 const hyperdex::instance& from_inst,
                 const hyperdisk::reference& ref);
        ~deferred() throw ();

    public:
        std::tr1::shared_ptr<e::buffer> backing;
        const bool has_value;
        const e::slice key;
        const std::vector<e::slice> value;
        const hyperdex::entityid from_ent;
        const hyperdex::instance from_inst;
        hyperdisk::reference ref;

    private:
        friend class e::intrusive_ptr<deferred>;

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        size_t m_ref;
};

hyperdaemon :: replication_manager :: deferred :: deferred(const bool hv,
                                                           std::auto_ptr<e::buffer> b,
                                                           const e::slice& k,
                                                           const std::vector<e::slice>& val,
                                                           const hyperdex::entityid& e,
                                                           const hyperdex::instance& i,
                                                           const hyperdisk::reference& r)
    : backing(b.release())
    , has_value(hv)
    , key(k)
    , value(val)
    , from_ent(e)
    , from_inst(i)
    , ref(r)
    , m_ref(0)
{
}

hyperdaemon :: replication_manager :: deferred :: ~deferred()
                                                  throw ()
{
}

#endif // hyperdaemon_replication_manager_deferred
