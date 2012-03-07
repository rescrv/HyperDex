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

    cdef enum hyperclient_datatype:
        HYPERDATATYPE_STRING    = 8960
        HYPERDATATYPE_UINT64    = 8961
        HYPERDATATYPE_GARBAGE   = 9087

    cdef struct hyperclient_attribute:
        char* attr
        char* value
        size_t value_sz
        hyperclient_datatype datatype

    cdef struct hyperclient_range_query:
        char* attr
        unsigned long attr_sz
        uint64_t lower
        uint64_t upper

    cdef enum hyperclient_returncode:
        HYPERCLIENT_SUCCESS      = 8448
        HYPERCLIENT_NOTFOUND     = 8449
        HYPERCLIENT_SEARCHDONE   = 8450
        HYPERCLIENT_CMPFAIL      = 8451
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
        HYPERCLIENT_WRONGTYPE    = 8525
        HYPERCLIENT_EXCEPTION    = 8574
        HYPERCLIENT_ZERO         = 8575
        HYPERCLIENT_A            = 8576
        HYPERCLIENT_B            = 8577

    hyperclient* hyperclient_create(char* coordinator, in_port_t port)
    void hyperclient_destroy(hyperclient* client)
    int64_t hyperclient_get(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_returncode* status, hyperclient_attribute** attrs, size_t* attrs_sz)
    int64_t hyperclient_put(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_condput(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_attribute* condattrs, size_t condattrs_sz, hyperclient_attribute* attrs, size_t attrs_sz, hyperclient_returncode* status)
    int64_t hyperclient_del(hyperclient* client, char* space, char* key, size_t key_sz, hyperclient_returncode* status)
    int64_t hyperclient_search(hyperclient* client, char* space, hyperclient_attribute* eq, size_t eq_sz, hyperclient_range_query* rn, size_t rn_sz, hyperclient_returncode* status, hyperclient_attribute** attrs, size_t* attrs_sz)
    int64_t hyperclient_loop(hyperclient* client, int timeout, hyperclient_returncode* status)
    void hyperclient_destroy_attrs(hyperclient_attribute* attrs, size_t attrs_sz)

import collections
import struct

class HyperClientException(Exception):

    def __init__(self, status, attr=None):
        self._s = {HYPERCLIENT_SUCCESS: 'Success'
                  ,HYPERCLIENT_NOTFOUND: 'Not Found'
                  ,HYPERCLIENT_SEARCHDONE: 'Search Done'
                  ,HYPERCLIENT_CMPFAIL: 'Conditional Operation Did Not Match Object'
                  ,HYPERCLIENT_UNKNOWNSPACE: 'Unknown Space'
                  ,HYPERCLIENT_COORDFAIL: 'Coordinator Failure'
                  ,HYPERCLIENT_SERVERERROR: 'Server Error'
                  ,HYPERCLIENT_CONNECTFAIL: 'Connection Failure'
                  ,HYPERCLIENT_DISCONNECT: 'Connection Reset'
                  ,HYPERCLIENT_RECONFIGURE: 'Reconfiguration'
                  ,HYPERCLIENT_LOGICERROR: 'Logic Error (file a bug)'
                  ,HYPERCLIENT_TIMEOUT: 'Timeout'
                  ,HYPERCLIENT_UNKNOWNATTR: 'Unknown attribute "%s"' % attr
                  ,HYPERCLIENT_DUPEATTR: 'Duplicate attribute "%s"' % attr
                  ,HYPERCLIENT_SEEERRNO: 'See ERRNO'
                  ,HYPERCLIENT_NONEPENDING: 'None pending'
                  ,HYPERCLIENT_DONTUSEKEY: "Don't use the key in the search predicate"
                  ,HYPERCLIENT_WRONGTYPE: 'Attribute "%s" has the wrong type' % attr
                  ,HYPERCLIENT_EXCEPTION: 'Internal Error (file a bug)'
                  }.get(status, 'Unknown Error (file a bug)')

    def __str__(self):
        return self._s


cdef _dict_to_attrs(list value, hyperclient_attribute** attrs):
    cdef list backings = []
    attrs[0] = <hyperclient_attribute*> \
               malloc(sizeof(hyperclient_attribute) * len(value))
    if attrs[0] == NULL:
        raise MemoryError()
    for i, a in enumerate(value):
        a, v = a
        attrs[0][i].attr = a
        if isinstance(v, int):
            backing = struct.pack('<Q', v)
            backings.append(backing)
            attrs[0][i].value = backing
            attrs[0][i].value_sz = 8
            attrs[0][i].datatype = HYPERDATATYPE_UINT64
        else:
            backing = v
            backings.append(backing)
            attrs[0][i].value = v
            attrs[0][i].value_sz = len(v)
            attrs[0][i].datatype = HYPERDATATYPE_STRING
    return backings


cdef _attrs_to_dict(hyperclient_attribute* attrs, size_t attrs_sz):
    ret = {}
    for idx in range(attrs_sz):
        if attrs[idx].datatype == HYPERDATATYPE_UINT64:
            s = attrs[idx].value[:attrs[idx].value_sz]
            i = len(s)
            if i > 8:
                s = s[:8]
            elif i < 8:
                s += (8 - i) * '\x00'
            ret[attrs[idx].attr] = struct.unpack('<Q', s)[0]
        elif attrs[idx].datatype == HYPERDATATYPE_STRING:
            ret[attrs[idx].attr] = attrs[idx].value[:attrs[idx].value_sz]
        else:
            raise ValueError("Server returned garbage value (file a bug)")
    return ret


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
        if self._status not in (HYPERCLIENT_SUCCESS, HYPERCLIENT_NOTFOUND, HYPERCLIENT_CMPFAIL):
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
        if self._reqid < 0:
            raise HyperClientException(self._status)

    def __dealloc__(self):
        if self._attrs:
            hyperclient_destroy_attrs(self._attrs, self._attrs_sz)

    def wait(self):
        Deferred.wait(self)
        if self._status != HYPERCLIENT_SUCCESS:
            return None
        attrs = _attrs_to_dict(self._attrs, self._attrs_sz)
        attrstr = ' '.join(sorted(attrs.keys()))
        return collections.namedtuple(self._space, attrstr)(**attrs)


cdef class DeferredInsert(Deferred):

    def __cinit__(self, Client client, bytes space, bytes key, dict value):
        cdef char* space_cstr = space
        cdef char* key_cstr = key
        cdef hyperclient_attribute* attrs = NULL
        try:
            backings = _dict_to_attrs(value.items(), &attrs)
            self._reqid = hyperclient_put(client._client, space_cstr,
                                          key_cstr, len(key),
                                          attrs, len(value),
                                          &self._status)
            if self._reqid < 0:
                idx = -1 - self._reqid
                attr = None
                if attrs and attrs[idx].attr:
                    attr = attrs[idx].attr
                raise HyperClientException(self._status, attr)
        finally:
            if attrs:
                free(attrs)

    def wait(self):
        Deferred.wait(self)
        return self._status == HYPERCLIENT_SUCCESS


cdef class DeferredConditionalInsert(Deferred):

    def __cinit__(self, Client client, bytes space, bytes key, dict condition, dict value):
        cdef char* space_cstr = space
        cdef char* key_cstr = key
        cdef hyperclient_attribute* condattrs = NULL
        cdef hyperclient_attribute* attrs = NULL
        if len(condition):
            condattrs = <hyperclient_attribute*> \
                malloc(sizeof(hyperclient_attribute) * len(condition))
        if len(value):
            attrs = <hyperclient_attribute*> \
                malloc(sizeof(hyperclient_attribute) * len(value))
        try:
            for i, a in enumerate(condition.iteritems()):
                a, v = a
                if isinstance(v, int):
                    v = struct.pack('<Q', v)
                    t = HYPERDATATYPE_UINT64
                else:
                    t = HYPERDATATYPE_STRING
                condattrs[i].attr = a
                condattrs[i].value = v
                condattrs[i].value_sz = len(v)
                condattrs[i].datatype = t
            for i, a in enumerate(value.iteritems()):
                a, v = a
                if isinstance(v, int):
                    v = struct.pack('<Q', v)
                    t = HYPERDATATYPE_UINT64
                else:
                    t = HYPERDATATYPE_STRING
                attrs[i].attr = a
                attrs[i].value = v
                attrs[i].value_sz = len(v)
                attrs[i].datatype = t
            self._reqid = hyperclient_condput(client._client, space_cstr,
                                          key_cstr, len(key),
					  condattrs, len(condition),
                                          attrs, len(value),
                                          &self._status)
            attr = None			
            if self._reqid < 0:
                index = -1 - self._reqid
                if index < len(condition):
                    attr = condattrs[index].attr
                index -= len(condition)
                if index >= 0 and index < len(value):
                    attr = attrs[index].attr
                raise HyperClientException(self._status, attr)
        finally:
            free(attrs)
            free(condattrs)

    def wait(self):
        Deferred.wait(self)
        return self._status == HYPERCLIENT_SUCCESS


cdef class DeferredRemove(Deferred):

    def __cinit__(self, Client client, bytes space, bytes key):
        cdef char* space_cstr = space
        cdef char* key_cstr = key
        self._reqid = hyperclient_del(client._client, space_cstr,
                                      key_cstr, len(key), &self._status)
        if self._reqid < 0:
            raise HyperClientException(self._status)

    def wait(self):
        Deferred.wait(self)
        return self._status == HYPERCLIENT_SUCCESS


cdef class Search:

    cdef Client _client
    cdef int64_t _reqid
    cdef hyperclient_returncode _status
    cdef bint _finished
    cdef hyperclient_attribute* _attrs
    cdef size_t _attrs_sz
    cdef bytes _space
    cdef list _backlogged

    def __cinit__(self, Client client, bytes space, dict predicate):
        self._client = client
        self._reqid = 0
        self._status = HYPERCLIENT_ZERO
        self._finished = False
        self._attrs = <hyperclient_attribute*> NULL
        self._attrs_sz = 0
        self._space = space
        self._backlogged = []
        cdef uint64_t lower
        cdef uint64_t upper
        equalities = []
        ranges = []
        for attr, params in predicate.iteritems():
            if isinstance(params, tuple):
                (lower, upper) = params
                ranges.append((attr, lower, upper))
            elif isinstance(params, int):
                equalities.append((attr, params))
            elif isinstance(params, bytes):
                equalities.append((attr, params))
            else:
                errstr = "Attribute '{attr}' has incorrect type (expected int, (int, int) or bytes, got {type}"
                raise TypeError(errstr.format(attr=attr, type=str(type(params))[7:-2]))
        cdef hyperclient_attribute* eq = NULL
        cdef hyperclient_range_query* rm = NULL
        try:
            eq = <hyperclient_attribute*> \
                 malloc(sizeof(hyperclient_attribute) * len(equalities))
            rn = <hyperclient_range_query*> \
                 malloc(sizeof(hyperclient_range_query) * len(ranges))
            backings = _dict_to_attrs(equalities, &eq)
            for i, (attr, lower, upper) in enumerate(ranges):
                rn[i].attr = attr
                rn[i].lower = lower
                rn[i].upper = upper
            self._reqid = hyperclient_search(client._client,
                                             self._space,
                                             eq, len(equalities),
                                             rn, len(ranges),
                                             &self._status,
                                             &self._attrs,
                                             &self._attrs_sz)
            if self._reqid < 0:
                idx = -1 - self._reqid
                attr = None
                if idx < len(equalities) and eq and eq[idx].attr:
                    attr = eq[idx].attr
                idx -= len(equalities)
                if idx < len(ranges) and rn and rn[idx].attr:
                    attr = rn[idx].attr
                raise HyperClientException(self._status, attr)
            client._searches[self._reqid] = self
        finally:
            if eq: free(eq)
            if rn: free(rn)

    def __iter__(self):
        return self

    def __next__(self):
        while not self._finished and not self._backlogged:
            self._client.loop()
        if self._backlogged:
            return self._backlogged.pop()
        del self._client._searches[self._reqid]
        raise StopIteration()

    def _callback(self):
        if self._status == HYPERCLIENT_SEARCHDONE:
            self._finished = True
        elif self._status == HYPERCLIENT_SUCCESS:
            try:
                attrs = _attrs_to_dict(self._attrs, self._attrs_sz)
            finally:
                if self._attrs:
                    free(self._attrs)
            attrstr = ' '.join(sorted(attrs.keys()))
            self._backlogged.append(collections.namedtuple(self._space, attrstr)(**attrs))
        else:
            self._backlogged.append(HyperClientException(self._status))


cdef class Client:
    cdef hyperclient* _client
    cdef set _pending
    cdef dict _searches

    def __cinit__(self, address, port):
        self._client = hyperclient_create(address, port)
        self._pending = set([])
        self._searches = {}

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
        return Search(self, space, predicate)

    def conditional_insert(self, bytes space, bytes key, dict condition, dict value):
        async = self.async_conditional_insert(space, key, condition, value)
        return async.wait()

    def async_lookup(self, bytes space, bytes key):
        return DeferredLookup(self, space, key)

    def async_insert(self, bytes space, bytes key, dict value):
        return DeferredInsert(self, space, key, value)

    def async_conditional_insert(self, bytes space, bytes key, dict condition, dict value):
        return DeferredConditionalInsert(self, space, key, condition, value)

    def async_remove(self, bytes space, bytes key):
        return DeferredRemove(self, space, key)

    def loop(self):
        cdef hyperclient_returncode rc
        ret = hyperclient_loop(self._client, -1, &rc)
        if ret < 0:
            raise HyperClientException(rc)
        else:
            if ret in self._searches:
                self._searches[ret]._callback()
            else:
                self._pending.add(ret)
