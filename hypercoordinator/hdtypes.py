# Copyright (c) 2012, Cornell University
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
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import collections
import hdjson


class Dimension(object):

    def __init__(self, name, datatype):
        self._name = name
        self._datatype = datatype

    @property
    def name(self):
        return self._name

    @property
    def datatype(self):
        return self._datatype
    
    def __repr__(self):
        return hdjson.Encoder().encode(self)

class Space(object):

    def __init__(self, name, dimensions, subspaces):
        self._name = name
        self._dimensions = tuple(dimensions)
        self._subspaces = tuple(subspaces)

    @property
    def name(self):
        return self._name

    @property
    def dimensions(self):
        return self._dimensions

    @property
    def subspaces(self):
        return self._subspaces

    def __repr__(self):
        return hdjson.Encoder().encode(self)

class Subspace(object):

    def __init__(self, dimensions, nosearch, regions):
        self._dimensions = tuple(dimensions)
        self._nosearch = tuple(nosearch)
        self._regions = tuple(regions)

    @property
    def dimensions(self):
        return self._dimensions

    @property
    def nosearch(self):
        return self._nosearch

    @property
    def regions(self):
        return self._regions

    def __repr__(self):
        return hdjson.Encoder().encode(self)

class Region(object):

    def __init__(self, prefix, mask, desired_f, replicas=None, transfers=None):
        self._prefix = prefix
        self._mask = mask
        self._desired_f = desired_f
        self._replicas = replicas or []
        self._transfers = transfers or []

    @property
    def prefix(self):
        return self._prefix

    @property
    def mask(self):
        return self._mask

    @property
    def desired_f(self):
        return self._desired_f

    @property
    def current_f(self):
        return len(self._replicas) - 1

    @property
    def replicas(self):
        return tuple(self._replicas)

    @property
    def transfers(self):
        return tuple([i for x, i in self._transfers])

    @property
    def transfer_in_progress(self):
        if self._transfers:
            return self._transfers[0]
        else:
            return (None, None)

    def add_replica(self, replica):
        assert replica is not None
        self._replicas.append(replica)

    def remove_instances(self, badreplicas):
        self._replicas = [r for r in self._replicas if r not in badreplicas]
        self._transfers = [t for t in self._transfers if t[1] not in badreplicas]

    def transfer_initiate(self, xferid, instid):
        self._transfers.append((xferid, instid))

    def transfer_golive(self, xferid):
        if not self._transfers:
            raise RuntimeError('transfer "golive" message for unknown xferid')
        if self._transfers[0][0] != xferid:
            raise RuntimeError('transfer "golive" message for wrong xferid')
        if self._replicas[-1] != self._transfers[0][1]:
            self._replicas.append(self._transfers[0][1])
            return True
        return False

    def transfer_complete(self, xferid):
        if not self._transfers:
            raise RuntimeError('transfer "complete" message for unknown xferid')
        if self._transfers[0][0] != xferid:
            raise RuntimeError('transfer "complete" message for wrong xferid')
        if not self._replicas or self._transfers[0][1] != self._replicas[-1]:
            raise RuntimeError('transfer "complete" message must come after "golive"')
        self._transfers = self._transfers[1:]

    def transfer_fail(self, xferid):
        if self._transfers and self._transfers[0][0] == xferid:
            if self._replicas and self._replicas[-1] == self._transfers[0][1]:
                self._replicas.pop()
        self._transfers = [(x, i) for x, i in self._transfers if x != xferid]

    def __eq__(self, other):
        return self.prefix == other.prefix and \
               self.mask == other.mask and \
               self.desired_f == other.desired_f

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return hdjson.Encoder().encode(self)


InstanceBindings = collections.namedtuple('Instance', 'addr inport inver outport outver')


class Instance(object):

    def __init__(self, addr, inport, inver, outport, outver, pid, token, configs = None, last_acked = 0, last_rejected = 0):
        self._addr = addr
        self._inport = inport
        self._inver = inver
        self._outport = outport
        self._outver = outver
        self._pid = pid
        self._token = token
        self._configs = [] if not configs else configs
        self._last_acked = last_acked
        self._last_rejected = last_rejected

    @property
    def addr(self):
        return self._addr

    @property
    def inport(self):
        return self._inport

    @property
    def inver(self):
        return self._inver

    @property
    def outport(self):
        return self._outport

    @property
    def outver(self):
        return self._outver

    @property
    def pid(self):
        return self._pid
        
    @property
    def token(self):
        return self._token
        
    @property
    def last_acked(self):
        return self._last_acked

    def add_config(self, num, data):
        assert not self._configs or num > self._configs[-1][0]
        self._configs.append((num, data))

    def ack_config(self, num):
        if not self._configs:
            raise RuntimeError("acking config when none are present")
        if self._configs[0][0] != num:
            raise RuntimeError("acking config number which does not match the oldest pending")
        self._configs = self._configs[1:]
        self._last_acked = num

    def reject_config(self, num):
        if not self._configs:
            raise RuntimeError("rejecting config when none are present")
        if self._configs[0][0] != num:
            raise RuntimeError("rejecting config number which does not match the oldest pending")
        self._configs = self._configs[1:]
        self._last_rejected = num

    def next_config(self):
        if self._configs:
            return self._configs[0]
        return (None, None)

    def bindings(self):
        return InstanceBindings(self._addr, self._inport, self._inver,
                                self._outport, self._outver)
    
    def __repr__(self):
        return hdjson.Encoder().encode(self)
