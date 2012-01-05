# Copyright (c) 2011, Cornell University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

cdef extern from "stdint.h":
    ctypedef short int int16_t
    ctypedef unsigned short int uint16_t
    ctypedef int int32_t
    ctypedef unsigned int uint32_t
    ctypedef long int int64_t
    ctypedef unsigned long int uint64_t
    ctypedef long unsigned int size_t

cdef extern from "sys/socket.h":
    ctypedef uint16_t in_port_t
    cdef struct sockaddr

cdef extern from "../hyperclient.h":

    cdef struct hyperclient

    cdef struct hyperclient_attribute:
        char* attr
        char* value
        size_t value_sz

    cdef struct hyperclient_range_query:
        char* attr
        unsigned long attr_sz
        uint64_t lower
        uint64_t upper

    cdef enum hyperclient_returncode:
        HYPERCLIENT_SUCCESS      = 8448
        HYPERCLIENT_NOTFOUND     = 8449
        HYPERCLIENT_UNKNOWNSPACE = 8450
        HYPERCLIENT_COORDFAIL    = 8451
        HYPERCLIENT_SERVERERROR  = 8452
        HYPERCLIENT_CONNECTFAIL  = 8453
        HYPERCLIENT_DISCONNECT   = 8454
        HYPERCLIENT_RECONFIGURE  = 8455
        HYPERCLIENT_LOGICERROR   = 8456
        HYPERCLIENT_TIMEOUT      = 8457
        HYPERCLIENT_UNKNOWNATTR  = 8458
        HYPERCLIENT_DUPEATTR     = 8459
        HYPERCLIENT_SEEERRNO     = 8460
        HYPERCLIENT_NONEPENDING  = 8461
        HYPERCLIENT_EXCEPTION    = 8575

    hyperclient* hyperclient_create(char* coordinator, in_port_t port)
    void hyperclient_destroy(hyperclient* client)
    int64_t hyperclient_get(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_returncode* status, hyperclient_attribute** attrs, size_t* attrs_sz)
    int64_t hyperclient_put(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_del(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_returncode* status)
    int64_t hyperclient_search(hyperclient* client, char* space, hyperclient_attribute* eq, size_t eq_sz, hyperclient_range_query* rn, size_t rn_sz, hyperclient_returncode* status, char** key, size_t* key_sz, hyperclient_attribute** attrs, size_t* attrs_sz)
    int64_t hyperclient_loop(hyperclient* client, int timeout, hyperclient_returncode* status)

import collections

cdef class DeferredRemove:
    cdef int64_t _reqid
    cdef hyperclient_returncode _status

    def __cinit__(self, Client c, bytes space, bytes key):
        cdef char* space_cstr = space
        cdef char* key_cstr = key
        self._reqid = hyperclient_del(c._client, space_cstr, key_cstr, len(key), &self._status)

    def wait(self):
        return True

cdef class Client:
    cdef hyperclient* _client

    def __init__(self, address, port):
        self._client = hyperclient_create(address, port)

    def __dealloc__(self):
        hyperclient_destroy(self._client)

    def lookup(self, bytes space, bytes key):
        async = self.async_lookup(space, key)
        return async.wait()

    def insert(self, bytes space, bytes key, dict value):
        async = self.async_insert(space, key, value)
        return async.wait()

    def remove(self, bytes space, bytes key):
        async = self.async_remove(space, key)
        return async.wait()

    def search(self, bytes space, dict predicate):
        async = self.async_search(space, predicate)
        return async.wait()

    def async_lookup(self, bytes space, bytes key):
        return True

    def async_insert(self, bytes space, bytes key, dict value):
        return True

    def async_remove(self, bytes space, bytes key):
        return DeferredRemove(self, space, key)

    def async_search(self, bytes space, dict predicate):
        return True
