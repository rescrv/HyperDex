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

#ifndef hyperdaemon_network_worker_h_
#define hyperdaemon_network_worker_h_

// Forward Declarations
namespace hyperdaemon
{
class datalayer;
class logical;
class ongoing_state_transfers;
class replication_manager;
class searches;
}

namespace hyperdaemon
{

// One instance can be shared among many threads.
class network_worker
{
    public:
        network_worker(datalayer* data,
                       logical* comm,
                       searches* ssss,
                       ongoing_state_transfers* ost,
                       replication_manager* repl);
        ~network_worker() throw ();

    public:
        void run();
        void shutdown();

    private:
        network_worker(const network_worker&);

    private:
        network_worker& operator = (const network_worker&);

    private:
        bool m_continue;
        datalayer* m_data;
        logical* m_comm;
        searches* m_ssss;
        ongoing_state_transfers* m_ost;
        replication_manager* m_repl;
};

} // namespace hyperdaemon

#endif // hyperdaemon_network_worker_h_
