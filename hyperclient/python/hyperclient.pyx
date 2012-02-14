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

cdef extern from "stdlib.h":

    void* malloc(size_t size)
    void free(void* ptr)

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
        HYPERCLIENT_SEARCHDONE   = 8450
        HYPERCLIENT_UNKNOWNSPACE = 8512
        HYPERCLIENT_COORDFAIL    = 8513
        HYPERCLIENT_SERVERERROR  = 8514
        HYPERCLIENT_CONNECTFAIL  = 8515
        HYPERCLIENT_DISCONNECT   = 8516
        HYPERCLIENT_RECONFIGURE  = 8517
        HYPERCLIENT_LOGICERROR   = 8518
        HYPERCLIENT_TIMEOUT      = 8519
        HYPERCLIENT_UNKNOWNATTR  = 8520
        HYPERCLIENT_DUPEATTR     = 8521
        HYPERCLIENT_SEEERRNO     = 8522
        HYPERCLIENT_NONEPENDING  = 8523
        HYPERCLIENT_DONTUSEKEY   = 8524
        HYPERCLIENT_EXCEPTION    = 8574
        HYPERCLIENT_ZERO         = 8575
        HYPERCLIENT_A            = 8576
        HYPERCLIENT_B            = 8577

    hyperclient* hyperclient_create(char* coordinator, in_port_t port)
    void hyperclient_destroy(hyperclient* client)
    int64_t hyperclient_get(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_returncode* status, hyperclient_attribute** attrs, size_t* attrs_sz)
    int64_t hyperclient_put(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_del(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_returncode* status)
    int64_t hyperclient_search(hyperclient* client, char* space, hyperclient_attribute* eq, size_t eq_sz, hyperclient_range_query* rn, size_t rn_sz, hyperclient_returncode* status, char** key, size_t* key_sz, hyperclient_attribute** attrs, size_t* attrs_sz)
    int64_t hyperclient_loop(hyperclient* client, int timeout, hyperclient_returncode* status)
    void hyperclient_destroy_attrs(hyperclient_attribute* attrs, size_t attrs_sz)

import collections

class HyperClientException(Exception): pass

cdef class Deferred:

    cdef Client _client
    cdef int64_t _reqid
    cdef hyperclient_returncode _status
    cdef bint _finished

    def __cinit__(self, Client client, *args):
        self._client = client
        self._reqid = 0
        self._status = HYPERCLIENT_ZERO
        self._finished = False

    def finished(self):
        return self._finished or self._reqid < 0

    def wait(self):
        if not self._finished and self._reqid > 0:
            while self._reqid not in self._client._pending:
                self._client.loop()
            if self._reqid > 0:
                # Do this here in case remove throws
                self._finished = True
                self._client._pending.remove(self._reqid)
        self._finished = True
        # XXX Do a better job raising exceptions
        if self._status not in (HYPERCLIENT_SUCCESS, HYPERCLIENT_NOTFOUND):
            raise HyperClientException(self._status)

cdef class DeferredLookup(Deferred):

    cdef hyperclient_attribute* _attrs
    cdef size_t _attrs_sz
    cdef bytes _space

    def __cinit__(self, Client client, bytes space, bytes key):
        self._attrs = <hyperclient_attribute*> NULL
        self._attrs_sz = 0
        self._space = space
        cdef char* space_cstr = space
        cdef char* key_cstr = key
        self._reqid = hyperclient_get(client._client, space_cstr,
                                      key_cstr, len(key),
                                      &self._status,
                                      &self._attrs, &self._attrs_sz)

    def __dealloc__(self):
        if self._attrs:
            hyperclient_destroy_attrs(self._attrs, self._attrs_sz)

    def wait(self):
        Deferred.wait(self)
        if self._status != HYPERCLIENT_SUCCESS:
            return None
        attrs = {}
        for i in range(self._attrs_sz):
            attrs[self._attrs[i].attr] = \
                    self._attrs[i].value[:self._attrs[i].value_sz]
        attrstr = ' '.join(sorted(attrs.keys()))
        return collections.namedtuple(self._space, attrstr)(**attrs)


cdef class DeferredInsert(Deferred):

    def __cinit__(self, Client client, bytes space, bytes key, dict value):
        cdef char* space_cstr = space
        cdef char* key_cstr = key
        cdef hyperclient_attribute* attrs = \
                <hyperclient_attribute*> \
                malloc(sizeof(hyperclient_attribute) * len(value))
        for i, a in enumerate(value.iteritems()):
            attrs[i].attr = a[0]
            attrs[i].value = a[1]
            attrs[i].value_sz = len(a[1])
        self._reqid = hyperclient_put(client._client, space_cstr,
                                      key_cstr, len(key),
                                      attrs, len(value),
                                      &self._status)
        free(attrs)

    def wait(self):
        Deferred.wait(self)
        return self._status == HYPERCLIENT_SUCCESS

cdef class DeferredRemove(Deferred):

    def __cinit__(self, Client client, bytes space, bytes key):
        cdef char* space_cstr = space
        cdef char* key_cstr = key
        self._reqid = hyperclient_del(client._client, space_cstr,
                                      key_cstr, len(key), &self._status)

    def wait(self):
        Deferred.wait(self)
        return self._status == HYPERCLIENT_SUCCESS

cdef class Client:
    cdef hyperclient* _client
    cdef set _pending

    def __cinit__(self, address, port):
        self._client = hyperclient_create(address, port)
        self._pending = set([])

    def __dealloc__(self):
        if self._client:
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
        #async = self.async_search(space, predicate)
        #return async.wait()
        return False

    def async_lookup(self, bytes space, bytes key):
        return DeferredLookup(self, space, key)

    def async_insert(self, bytes space, bytes key, dict value):
        return DeferredInsert(self, space, key, value)

    def async_remove(self, bytes space, bytes key):
        return DeferredRemove(self, space, key)

    def async_search(self, bytes space, dict predicate):
        #return DeferredSearch(self, space, predicate)
        return False

    def loop(self):
        cdef hyperclient_returncode rc
        ret = hyperclient_loop(self._client, -1, &rc)
        if ret < 0:
            raise HyperClientException(rc)
        else:
            self._pending.add(ret)
