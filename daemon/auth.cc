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

// po6
#include <po6/time.h>

// HyperDex
#include <hyperdex/client.h>
#include "common/auth_wallet.h"
#include "daemon/auth.h"

BEGIN_HYPERDEX_NAMESPACE

struct general_caveat
{
    general_caveat() : check(NULL), ptr(NULL) {}
    general_caveat(int (*c)(void* f, const unsigned char* pred, size_t pred_sz), void* p) : check(c), ptr(p) {}
    int (*check)(void* f, const unsigned char* pred, size_t pred_sz);
    void* ptr;
};

END_HYPERDEX_NAMESPACE

void
hyperdex :: sanitize_secrets(const schema& sc, std::vector<e::slice>* value)
{
    for (size_t i = 1; i < sc.attrs_sz; ++i)
    {
        if (sc.attrs[i].type == HYPERDATATYPE_MACAROON_SECRET && i <= value->size())
        {
            (*value)[i - 1] = e::slice();
        }
    }
}

static void
destroy_macaroons(std::vector<macaroon*>* MS)
{
    for (size_t i = 0; i < MS->size(); ++i)
    {
        macaroon_destroy((*MS)[i]);
    }
}

bool
hyperdex :: auth_verify(const schema& sc,
                        bool has_value,
                        const std::vector<e::slice>* value,
                        auth_wallet* aw,
                        const char** exact,
                        general_caveat* general)
{
    if (!sc.authorization)
    {
        return true;
    }

    if (!has_value || !aw)
    {
        return false;
    }

    bool has_secret = false;
    e::slice secret;

    for (size_t i = 1; i < sc.attrs_sz; ++i)
    {
        if (sc.attrs[i].type == HYPERDATATYPE_MACAROON_SECRET)
        {
            has_secret = true;
            secret = (*value)[i - 1];
        }
    }

    // build the verifier
    struct macaroon_verifier* V = macaroon_verifier_create();

    if (!V)
    {
        throw std::bad_alloc();
    }

    while (exact && *exact)
    {
        macaroon_returncode err;

        if (macaroon_verifier_satisfy_exact(V, reinterpret_cast<const unsigned char*>(*exact), strlen(*exact), &err) < 0)
        {
            has_secret = false;
        }

        ++exact;
    }

    while (general && general->check)
    {
        macaroon_returncode err;

        if (macaroon_verifier_satisfy_general(V, general->check, general->ptr, &err) < 0)
        {
            has_secret = false;
        }

        ++general;
    }

    e::guard g1 = e::makeguard(macaroon_verifier_destroy, V);
    std::vector<macaroon*> MS;
    e::guard g2 = e::makeguard(destroy_macaroons, &MS);

    if (!aw->get_macaroons(&MS) || MS.empty())
    {
        return false;
    }

    macaroon_returncode err;
    int rc = macaroon_verify(V, MS[0], secret.data(), secret.size(), &MS[1], MS.size() - 1, &err);
    return rc == 0 && has_secret;
}

#define TIME_PRED "time < "
#define TIME_PRED_SZ (sizeof(TIME_PRED) - 1)

static int
check_time(void* t, const unsigned char* pred, size_t pred_sz)
{
    if (pred_sz < TIME_PRED_SZ ||
        memcmp(pred, TIME_PRED, TIME_PRED_SZ) != 0)
    {
        return -1;
    }

    std::string tmp(reinterpret_cast<const char*>(pred) + TIME_PRED_SZ, pred_sz - TIME_PRED_SZ);
    char* end = NULL;
    unsigned long long expiry = strtoull(tmp.c_str(), &end, 10);

    if (end == NULL || *end != '\0')
    {
        return -1;
    }

    return (*reinterpret_cast<uint64_t*>(t) < expiry) ? 0 : -1;
}

bool
hyperdex :: auth_verify_read(const schema& sc,
                             bool has_value,
                             const std::vector<e::slice>* value,
                             auth_wallet* aw)
{
    const char* exact[] = {"op = read", NULL};
    uint64_t time = po6::time() / 1000000000ULL;
    general_caveat general[] = {general_caveat(check_time, &time), general_caveat()};
    return auth_verify(sc, has_value, value, aw, exact, general);
}


bool
hyperdex :: auth_verify_write(const schema& sc,
                              bool has_value,
                              const std::vector<e::slice>* value,
                              const key_change& kc)
{
    if (!sc.authorization)
    {
        return true;
    }

    if (!has_value)
    {
        uint16_t attrnum = sc.lookup_attr(HYPERDEX_ATTRIBUTE_SECRET);

        if (attrnum == sc.attrs_sz)
        {
            return false;
        }

        for (size_t i = 0; i < kc.funcs.size(); ++i)
        {
            if (kc.funcs[i].attr == attrnum &&
                kc.funcs[i].name == FUNC_SET &&
                kc.funcs[i].arg1_datatype == HYPERDATATYPE_MACAROON_SECRET)
            {
                return true;
            }
        }

        return false;
    }
    else
    {
        const char* exact[] = {"op = write", NULL};
        uint64_t time = po6::time() / 1000000000ULL;
        general_caveat general[] = {general_caveat(check_time, &time), general_caveat()};
        return auth_verify(sc, has_value, value, kc.auth.get(), exact, general);
    }
}
