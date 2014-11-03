# Copyright (c) 2013, Cornell University
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

from cpython cimport bool

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

cdef extern from "hyperdex/admin.h":

    cdef struct hyperdex_admin

    cdef struct hyperdex_admin_perf_counter:
        uint64_t id
        uint64_t time
        char* property
        uint64_t measurement

    cdef enum hyperdex_admin_returncode:
        HYPERDEX_ADMIN_SUCCESS      = 8704
        HYPERDEX_ADMIN_NOMEM        = 8768
        HYPERDEX_ADMIN_NONEPENDING  = 8769
        HYPERDEX_ADMIN_POLLFAILED   = 8770
        HYPERDEX_ADMIN_TIMEOUT      = 8771
        HYPERDEX_ADMIN_INTERRUPTED  = 8772
        HYPERDEX_ADMIN_SERVERERROR  = 8773
        HYPERDEX_ADMIN_COORDFAIL    = 8774
        HYPERDEX_ADMIN_BADSPACE     = 8775
        HYPERDEX_ADMIN_DUPLICATE    = 8776
        HYPERDEX_ADMIN_NOTFOUND     = 8777
        HYPERDEX_ADMIN_LOCALERROR   = 8778
        HYPERDEX_ADMIN_INTERNAL     = 8829
        HYPERDEX_ADMIN_EXCEPTION    = 8830
        HYPERDEX_ADMIN_GARBAGE      = 8831

    hyperdex_admin* hyperdex_admin_create(char* coordinator, uint16_t port)
    void hyperdex_admin_destroy(hyperdex_admin* admin)
    int64_t hyperdex_admin_add_space(hyperdex_admin* admin, char* description, hyperdex_admin_returncode* status)
    int64_t hyperdex_admin_rm_space(hyperdex_admin* admin, char* space, hyperdex_admin_returncode* status)
    int64_t hyperdex_admin_wait_until_stable(hyperdex_admin* admin, hyperdex_admin_returncode* status)
    int64_t hyperdex_admin_dump_config(hyperdex_admin* admin, hyperdex_admin_returncode* status, const char** config)
    int64_t hyperdex_admin_enable_perf_counters(hyperdex_admin* admin, hyperdex_admin_returncode* status, hyperdex_admin_perf_counter* pc)
    void hyperdex_admin_disable_perf_counters(hyperdex_admin* admin)
    int64_t hyperdex_admin_add_index(hyperdex_admin* admin, const char* space, const char* attribute, hyperdex_admin_returncode* status)
    int64_t hyperdex_admin_list_subspaces(hyperdex_admin* admin, const char* space, hyperdex_admin_returncode* status, const char** subspaces)
    int64_t hyperdex_admin_list_indices(hyperdex_admin* admin, const char* space, hyperdex_admin_returncode* status, const char** indices)
    int64_t hyperdex_admin_rm_index(hyperdex_admin* admin, uint64_t idxid, hyperdex_admin_returncode* status)
    int64_t hyperdex_admin_list_spaces(hyperdex_admin* admin, hyperdex_admin_returncode* status, const char** spaces)
    int64_t hyperdex_admin_loop(hyperdex_admin* admin, int timeout, hyperdex_admin_returncode* status) nogil

import collections
import struct

# Dictionary that behaves like a C-struct
class Index:
    def __init__(self, identifier_, attribute_):
        self.identifier = int(identifier_)
        self.attribute = attribute_

class Subspace:
    def __init__(self, identifier_, attributes_):
        self.identifier = int(identifier_)
        self.attributes = attributes_

class HyperDexAdminException(Exception):
    def __init__(self, status, attr=None):
        self._status = status
        self._s = {HYPERDEX_ADMIN_SUCCESS: 'Success'
                  ,HYPERDEX_ADMIN_NOMEM: 'Memory allocation failed'
                  ,HYPERDEX_ADMIN_NONEPENDING: 'None pending'
                  ,HYPERDEX_ADMIN_POLLFAILED: 'Polling Failed'
                  ,HYPERDEX_ADMIN_TIMEOUT: 'Timeout'
                  ,HYPERDEX_ADMIN_INTERRUPTED: 'Interrupted by a signal'
                  ,HYPERDEX_ADMIN_SERVERERROR: 'Server Error'
                  ,HYPERDEX_ADMIN_COORDFAIL: 'Coordinator Failure'
                  ,HYPERDEX_ADMIN_BADSPACE: 'Bad space description'
                  ,HYPERDEX_ADMIN_DUPLICATE: 'Duplicate entry'
                  ,HYPERDEX_ADMIN_NOTFOUND: 'Entity not found'
                  ,HYPERDEX_ADMIN_LOCALERROR: 'Local error during backup'
                  ,HYPERDEX_ADMIN_INTERNAL: 'Internal Error (file a bug)'
                  ,HYPERDEX_ADMIN_EXCEPTION: 'Internal Exception (file a bug)'
                  ,HYPERDEX_ADMIN_GARBAGE: 'Internal Corruption (file a bug)'
                  }.get(status, 'Unknown Error (file a bug)')
        self._e = {HYPERDEX_ADMIN_SUCCESS: 'HYPERDEX_ADMIN_SUCCESS'
                  ,HYPERDEX_ADMIN_NOMEM: 'HYPERDEX_ADMIN_NOMEM'
                  ,HYPERDEX_ADMIN_NONEPENDING: 'HYPERDEX_ADMIN_NONEPENDING'
                  ,HYPERDEX_ADMIN_POLLFAILED: 'HYPERDEX_ADMIN_POLLFAILED'
                  ,HYPERDEX_ADMIN_TIMEOUT: 'HYPERDEX_ADMIN_TIMEOUT'
                  ,HYPERDEX_ADMIN_INTERRUPTED: 'HYPERDEX_ADMIN_INTERRUPTED'
                  ,HYPERDEX_ADMIN_SERVERERROR: 'HYPERDEX_ADMIN_SERVERERROR'
                  ,HYPERDEX_ADMIN_COORDFAIL: 'HYPERDEX_ADMIN_COORDFAIL'
                  ,HYPERDEX_ADMIN_BADSPACE: 'HYPERDEX_ADMIN_BADSPACE'
                  ,HYPERDEX_ADMIN_DUPLICATE: 'HYPERDEX_ADMIN_DUPLICATE'
                  ,HYPERDEX_ADMIN_NOTFOUND: 'HYPERDEX_ADMIN_NOTFOUND'
                  ,HYPERDEX_ADMIN_LOCALERROR: 'HYPERDEX_ADMIN_LOCALERROR'
                  ,HYPERDEX_ADMIN_INTERNAL: 'HYPERDEX_ADMIN_INTERNAL'
                  ,HYPERDEX_ADMIN_EXCEPTION: 'HYPERDEX_ADMIN_EXCEPTION'
                  ,HYPERDEX_ADMIN_GARBAGE: 'HYPERDEX_ADMIN_GARBAGE'
                  }.get(status, 'BUG')

    def status(self):
        return self._status

    def symbol(self):
        return self._e

    def __str__(self):
        return 'HyperDexAdminException(%s, %s)' % (self._e, self._s)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        return self._status == other._status

    def __ne__(self, other):
        return not (self == other)

cdef class DeferredAddSpace:

    cdef Admin _admin
    cdef int64_t _reqid
    cdef hyperdex_admin_returncode _status
    cdef bint _finished

    def __cinit__(self, Admin admin, bytes space):
        self._admin = admin
        self._reqid = 0
        self._status = HYPERDEX_ADMIN_GARBAGE
        self._finished = False
        self._reqid = hyperdex_admin_add_space(self._admin._admin,
                                               space, &self._status)
        if self._reqid < 0:
            raise HyperDexAdminException(self._status)
        self._admin._ops[self._reqid] = self

    def _callback(self):
        self._finished = True
        del self._admin._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._admin.loop()
        self._finished = True
        if self._status == HYPERDEX_ADMIN_SUCCESS:
            return True
        else:
            raise HyperDexAdminException(self._status)

cdef class DeferredRmSpace:

    cdef Admin _admin
    cdef int64_t _reqid
    cdef hyperdex_admin_returncode _status
    cdef bint _finished

    def __cinit__(self, Admin admin, bytes space):
        self._admin = admin
        self._reqid = 0
        self._status = HYPERDEX_ADMIN_GARBAGE
        self._finished = False
        self._reqid = hyperdex_admin_rm_space(self._admin._admin,
                                              space, &self._status)
        if self._reqid < 0:
            raise HyperDexAdminException(self._status)
        self._admin._ops[self._reqid] = self

    def _callback(self):
        self._finished = True
        del self._admin._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._admin.loop()
        self._finished = True
        if self._status == HYPERDEX_ADMIN_SUCCESS:
            return True
        else:
            raise HyperDexAdminException(self._status)

cdef class DeferredAddIndex:
    cdef Admin _admin
    cdef int64_t _reqid
    cdef hyperdex_admin_returncode _status
    cdef bint _finished

    def __cinit__(self, Admin admin, bytes space, bytes attr):
        self._admin = admin
        self._reqid = 0
        self._status = HYPERDEX_ADMIN_GARBAGE
        self._finished = False
        self._reqid = hyperdex_admin_add_index(self._admin._admin,
                                              space, attr, &self._status)
        if self._reqid < 0:
            raise HyperDexAdminException(self._status)
        self._admin._ops[self._reqid] = self

    def _callback(self):
        self._finished = True
        del self._admin._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._admin.loop()
        self._finished = True
        if self._status == HYPERDEX_ADMIN_SUCCESS:
            return True
        else:
            raise HyperDexAdminException(self._status)

cdef class DeferredRmIndex:
    cdef Admin _admin
    cdef int64_t _reqid
    cdef hyperdex_admin_returncode _status
    cdef bint _finished

    def __cinit__(self, Admin admin, idxid):
        self._admin = admin
        self._reqid = 0
        self._status = HYPERDEX_ADMIN_GARBAGE
        self._finished = False

        self._reqid = hyperdex_admin_rm_index(self._admin._admin,
                                              idxid, &self._status)
        if self._reqid < 0:
            raise HyperDexAdminException(self._status)
        self._admin._ops[self._reqid] = self

    def _callback(self):
        self._finished = True
        del self._admin._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._admin.loop()
        self._finished = True
        if self._status == HYPERDEX_ADMIN_SUCCESS:
            return True
        else:
            raise HyperDexAdminException(self._status)

cdef class DeferredWaitUntilStable:
    cdef Admin _admin
    cdef int64_t _reqid
    cdef hyperdex_admin_returncode _status
    cdef bint _finished

    def __cinit__(self, Admin admin):
        self._admin = admin
        self._reqid = 0
        self._status = HYPERDEX_ADMIN_GARBAGE
        self._finished = False
        self._reqid = hyperdex_admin_wait_until_stable(self._admin._admin, &self._status)

        if self._reqid < 0:
            raise HyperDexAdminException(self._status)
        self._admin._ops[self._reqid] = self

    def _callback(self):
        self._finished = True
        del self._admin._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._admin.loop()
        self._finished = True
        if self._status == HYPERDEX_ADMIN_SUCCESS:
            return True
        else:
            raise HyperDexAdminException(self._status)

cdef class DeferredListSubspaces:
    cdef Admin _admin
    cdef int64_t _reqid
    cdef hyperdex_admin_returncode _status
    cdef const char* _cstr
    cdef list _subspaces
    cdef bint _finished

    def __cinit__(self, Admin admin, subspaces):
        self._admin = admin
        self._reqid = 0
        self._status = HYPERDEX_ADMIN_GARBAGE
        self._cstr = NULL
        self._subspaces = []
        self._finished = False
        self._reqid = hyperdex_admin_list_subspaces(self._admin._admin, subspaces, &self._status, &self._cstr)

        if self._reqid < 0:
            raise HyperDexAdminException(self._status)
        self._admin._ops[self._reqid] = self

    def _callback(self):
        for entry in self._cstr.split('\n'):
            if entry is not '':
                identifier, attrs_ = entry.split(':')
                attrs = attrs_.split(',')
                self._subspaces.append(Subspace(identifier, attrs))

        self._finished = True
        del self._admin._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._admin.loop()

        self._finished = True

        if self._status == HYPERDEX_ADMIN_SUCCESS:
            return self._subspaces
        else:
            raise HyperDexAdminException(self._status)

cdef class DeferredListIndices:
    cdef Admin _admin
    cdef int64_t _reqid
    cdef hyperdex_admin_returncode _status
    cdef const char* _cstr
    cdef list _indices
    cdef bint _finished

    def __cinit__(self, Admin admin, space):
        self._admin = admin
        self._reqid = 0
        self._status = HYPERDEX_ADMIN_GARBAGE
        self._cstr = NULL
        self._indices = []
        self._finished = False
        self._reqid = hyperdex_admin_list_indices(self._admin._admin, space, &self._status, &self._cstr)

        if self._reqid < 0:
            raise HyperDexAdminException(self._status)
        self._admin._ops[self._reqid] = self

    def _callback(self):
        for entry in self._cstr.split('\n'):
            if entry is not '':
                id_, attr_ = entry.split(':')
                self._indices.append(Index(id_, attr_))

        self._finished = True
        del self._admin._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._admin.loop()

        self._finished = True

        if self._status == HYPERDEX_ADMIN_SUCCESS:
            return self._indices
        else:
            raise HyperDexAdminException(self._status)

cdef class DeferredListSpaces:
    cdef Admin _admin
    cdef int64_t _reqid
    cdef hyperdex_admin_returncode _status
    cdef const char* _cstr
    cdef list _spaces
    cdef bint _finished

    def __cinit__(self, Admin admin):
        self._admin = admin
        self._reqid = 0
        self._status = HYPERDEX_ADMIN_GARBAGE
        self._cstr = NULL
        self._spaces = []
        self._finished = False
        self._reqid = hyperdex_admin_list_spaces(self._admin._admin, &self._status, &self._cstr)

        if self._reqid < 0:
            raise HyperDexAdminException(self._status)
        self._admin._ops[self._reqid] = self

    def _callback(self):
        for entry in self._cstr.split('\n'):
            if entry is not '':
                self._spaces.append(entry)

        self._finished = True
        del self._admin._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._admin.loop()

        self._finished = True

        if self._status == HYPERDEX_ADMIN_SUCCESS:
            return self._spaces
        else:
            raise HyperDexAdminException(self._status)

cdef class DeferredString:
    cdef Admin _admin
    cdef int64_t _reqid
    cdef hyperdex_admin_returncode _status
    cdef const char* _cstr
    cdef str _pstr
    cdef bint _finished

    def __cinit__(self, Admin admin, *args):
        self._admin = admin
        self._reqid = 0
        self._status = HYPERDEX_ADMIN_GARBAGE
        self._cstr = NULL
        self._pstr = ""
        self._finished = False
        self._reqid = hyperdex_admin_dump_config(self._admin._admin,
                                                 &self._status,
                                                 &self._cstr)
        if self._reqid < 0:
            raise HyperDexAdminException(self._status)
        self._admin._ops[self._reqid] = self

    def _callback(self):
        if self._cstr:
            self._pstr = self._cstr.encode('ascii')
        self._finished = True
        del self._admin._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._admin.loop()
        self._finished = True
        if self._status == HYPERDEX_ADMIN_SUCCESS:
            ret = {'cluster': 0, 'version': 0, 'servers': []}
            for line in self._pstr.split('\n'):
                if line.startswith('cluster'):
                    ret['cluster'] = int(line.split(' ')[1])
                if line.startswith('version'):
                    ret['version'] = int(line.split(' ')[1])
                if line.startswith('server'):
                    sid, loc, state = line.split(' ')[1:]
                    server = {'id': sid, 'location': loc, 'state': state}
                    ret['servers'].append(server)
            return ret
        else:
            raise HyperDexAdminException(self._status)

cdef class DeferredPerfCounter:

    cdef Admin _admin
    cdef int64_t _reqid
    cdef hyperdex_admin_returncode _status
    cdef hyperdex_admin_perf_counter _pc
    cdef bint _finished
    cdef list _backlogged

    def __cinit__(self, Admin admin, *args):
        self._admin = admin
        self._reqid = 0
        self._status = HYPERDEX_ADMIN_GARBAGE
        self._finished = False
        self._backlogged = []

    def __iter__(self):
        return self

    def __next__(self):
        while not self._finished and not self._backlogged:
            self._admin.loop()
        if self._backlogged:
            return self._backlogged.pop()
        raise StopIteration()

    def _callback(self):
        if self._status == HYPERDEX_ADMIN_SUCCESS:
            self._backlogged.append({'server': self._pc.id
                                    ,'time': self._pc.time
                                    ,'property': self._pc.property
                                    ,'measurement': self._pc.measurement})
        else:
            self._backlogged.append(HyperDexAdminException(self._status))

cdef class Admin:
    cdef hyperdex_admin* _admin
    cdef dict _ops
    cdef DeferredPerfCounter _pc

    def __cinit__(self, address, port):
        self._admin = hyperdex_admin_create(address, port)
        self._ops = {}
        self._pc = None

    def __dealloc__(self):
        if self._admin:
            hyperdex_admin_destroy(self._admin)

    def async_dump_config(self):
        return DeferredString(self)

    def dump_config(self):
        return self.async_dump_config().wait()

    def async_add_space(self, space):
        return DeferredAddSpace(self, space)

    def add_space(self, space):
        return self.async_add_space(space).wait()

    def async_rm_space(self, space):
        return DeferredRmSpace(self, space)

    def rm_space(self, space):
        return self.async_rm_space(space).wait()

    def async_add_index(self, space, attr):
        return DeferredAddIndex(self, space, attr)

    def add_index(self, space, attr):
        return self.async_add_index(space, attr).wait()

    def async_rm_index(self, idxid):
        if isinstance(idxid, Index):
            idxid = idxid.identifier

        if not isinstance(idxid, int):
            raise ValueError("Index ID must be an integer (or an instance of Index)")

        return DeferredRmIndex(self, idxid)

    def rm_index(self, idxid):
        return self.async_rm_index(idxid).wait()

    def async_list_spaces(self):
        return DeferredListSpaces(self)

    def list_indices(self, space):
        return self.async_list_indices(space).wait()

    def async_list_indices(self, space):
        return DeferredListIndices(self, space)

    def list_spaces(self):
        return self.async_list_spaces().wait()

    def async_list_subspaces(self, space):
        return DeferredListSubspaces(self, space)

    def list_subspaces(self, space):
        return self.async_list_subspaces(space).wait()

    def async_wait_until_stable(self):
        return DeferredWaitUntilStable(self)

    def wait_until_stable(self):
        return self.async_wait_until_stable().wait()

    def enable_perf_counters(self):
        cdef hyperdex_admin_returncode rc
        if self._pc:
            return self._pc
        self._pc = DeferredPerfCounter(self)
        self._pc._reqid = hyperdex_admin_enable_perf_counters(self._admin,
                                                              &self._pc._status,
                                                              &self._pc._pc)
        if self._pc._reqid < 0:
            rc = self._pc._status
            self._pc = None
            raise HyperDexAdminException(rc)
        self._ops[self._pc._reqid] = self._pc
        return self._pc

    def disable_perf_counters(self):
        hyperdex_admin_disable_perf_counters(self._admin)
        self._pc._finished = True
        del self._ops[self._pc._reqid]
        self._pc = None

    def loop(self):
        cdef hyperdex_admin_returncode rc
        with nogil:
            ret = hyperdex_admin_loop(self._admin, -1, &rc)
        if ret < 0:
            raise HyperDexAdminException(rc)
        else:
            assert ret in self._ops
            op = self._ops[ret]
            # We cannot refer to self._ops[ret] after this call as
            # _callback() may remove ret from self._ops.
            op._callback()
            return op
