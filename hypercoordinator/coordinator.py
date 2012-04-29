#!/usr/bin/env python

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
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import collections
import datetime
import logging
import random
import binascii
import select
import shlex
import json
import copy
import socket
import sys
import os

import pyparsing

import hypercoordinator.parser
from hypercoordinator import hdtypes

import hdjson

PIPE_BUF = getattr(select, 'PIPE_BUF', 512)
RESTRICTED_SPACES = set([0, 2**32 - 1, 2**32 - 2, 2**32 - 3, 2**32 - 4])

# Format strings for configuration lines
SPACE_LINE = 'space {name} {id} {dims}\n'
SUBSPACE_LINE = 'subspace {space} {subspace} {hashes}\n'
REGION_LINE = 'region {space} {subspace} {prefix} {mask} {hosts}\n'
TRANSFER_LINE = 'transfer {xferid} {space} {subspace} {prefix} {mask} {instid}\n'
HOST_LINE = 'host {id} {ip} {inport} {inver} {outport} {outver}'
QUIESCE_LINE = 'quiesce {state_id}\n'
SHUTDOWN_LINE = 'shutdown\n'


def normalize_address(addr):
    try:
        tmp = socket.inet_pton(socket.AF_INET, addr)
        return socket.inet_ntop(socket.AF_INET, tmp)
    except socket.error:
        try:
            tmp = socket.inet_pton(socket.AF_INET6, addr)
            return socket.inet_ntop(socket.AF_INET6, tmp)
        except socket.error:
            return None


class EndConnection(Exception): pass
class KillConnection(Exception): pass


class Coordinator(object):

    class DuplicateSpace(Exception): pass
    class UnknownSpace(Exception): pass
    class ExhaustedPorts(Exception): pass
    class ExhaustedSpaces(Exception): pass
    class InvalidStateData(Exception): pass
    class InvalidState(Exception): pass
    class ServiceLevelNotMet(Exception): pass

    S_STARTUP, S_NORMAL, S_QUIESCE, S_SHUTDOWN = 'STARTUP', 'NORMAL', 'QUIESCE', 'SHUTDOWN'
    SL_DESIRED, SL_DEGRADED, SL_DATALOSS = 'DESIRED', 'DEGRADED', 'DATALOSS'
    STATE_VERSION = 2

    def __init__(self, state_data=None):
        self._portcounters = collections.defaultdict(lambda: 1)
        self._instance_counter = 1
        self._instances_by_token = {}
        self._instances_by_bindings = {}
        self._instances_by_id = {}
        self._failed_instances = set()
        self._space_counter = 1
        self._spaces_by_name = {}
        self._spaces_by_id = {}
        self._rand = random.Random()
        self._config_counter = 0
        self._config_data = ''
        self._xfer_counter = 0
        self._xfers_by_id = {}
        self._quiesce_state_id = ''
        self._quiesce_config_num = -1
        self._state = Coordinator.S_STARTUP
        if state_data:
            # loading saved coord. state (restart after shutdown)
            self._load_state(state_data)
            self._quiesce_state_id = ''
            self._quiesce_config_num = -1
            self._state = Coordinator.S_STARTUP
        else:
            # fresh start
            self._state = Coordinator.S_NORMAL

    def _compute_port_epoch(self, addr, port):
        ver = self._portcounters[(addr, port)]
        self._portcounters[(addr, port)] += 1
        if ver >= (1 << 16):
            raise Coordinator.ExhaustedPorts()
        return ver

    def register_instance(self, addr, inport, outport, pid, token):
        # create new instance
        inver = self._compute_port_epoch(addr, inport)
        outver = self._compute_port_epoch(addr, outport)
        inst = hdtypes.Instance(addr, inport, inver, outport, outver, pid, token)
        # do not send config before go-live
        if self._state != Coordinator.S_STARTUP:
            inst.add_config(self._config_counter, self._config_data)
        # have we seen this instance before? 
        instid = self._instances_by_token.get(token, 0)
        if instid != 0:
            # host restat or reconnect - replace old instance with the new one
            # XXX should we do something different on reconnect only?
            # XXX preserve last_acked and last_rejected?
            oldinst = self._instances_by_id[instid]
            del self._instances_by_bindings[oldinst.bindings()]
            self._instances_by_bindings[inst.bindings()] = instid
            self._instances_by_id[instid] = inst
        else:
            # new instance
            instid = self._instance_counter
            self._instance_counter += 1
            self._instances_by_id[instid] = inst
            self._instances_by_bindings[inst.bindings()] = instid
            self._instances_by_token[token] = instid
        return inst.bindings()

    def keepalive_instance(self, bindings):
        pass

    def add_space(self, space):
        if self._state != Coordinator.S_NORMAL:
            raise Coordinator.InvalidState()
        if not self._service_level_met():
            raise Coordinator.ServiceLevelNotMet()
        if space.name in self._spaces_by_name:
            raise Coordinator.DuplicateSpace()
        spaceid = None
        for i in xrange(1 << 32):
            tmp = self._space_counter
            self._space_counter = (self._space_counter + 1) % 2**32
            if tmp not in self._spaces_by_id and \
               tmp not in RESTRICTED_SPACES:
                spaceid = tmp
                break
        if spaceid is None:
            raise Coordinator.ExhaustedSpaces()
        self._spaces_by_id[spaceid] = space
        self._spaces_by_name[space.name] = spaceid
        self._initial_layout(space)
        self._regenerate()

    def del_space(self, space):
        if self._state != Coordinator.S_NORMAL:
            raise Coordinator.InvalidState()
        if not self._service_level_met():
            raise Coordinator.ServiceLevelNotMet()
        if space not in self._spaces_by_name:
            raise Coordinator.UnknownSpace()
        spacenum = self._spaces_by_name[space]
        del self._spaces_by_name[space]
        del self._spaces_by_id[spacenum]
        self._regenerate()
        
    def lst_spaces(self):
        return self._spaces_by_name.keys()
        
    def get_space(self, space):
        if space not in self._spaces_by_name:
            raise Coordinator.UnknownSpace()
        spacenum = self._spaces_by_name[space]
        return self._spaces_by_id[spacenum]

    def quiesce(self):
        if self._state != Coordinator.S_NORMAL:
            raise Coordinator.InvalidState()
        # ask all hosts to save state under unique state id
        self._quiesce_state_id =  binascii.hexlify(os.urandom(16))
        self._state = Coordinator.S_QUIESCE
        logging.info('Cluster is quiescing under state id {0}.'.format(self._quiesce_state_id))
        self._regenerate()
        # remember current config num, so we can check if hosts ACKd quiesce
        self._quiesce_config_num = self._config_counter
        return self._quiesce_state_id

    def shutdown(self):
        if self._state != Coordinator.S_QUIESCE:
            raise Coordinator.InvalidState()
        # check if all not failed hosts ACKd quiesce
        w = 0
        for instid, inst in self._instances_by_id.iteritems():
            if instid not in self._failed_instances:
                if inst.last_acked < self._quiesce_config_num:
                    w += 1
        if w:
            raise Coordinator.InvalidState("Cannot shutdown yet, waiting for {0} hosts to quiesce.".format(w))
        # send shutdown config to all hosts 
        self._state = Coordinator.S_SHUTDOWN
        logging.info('Requesting all nodes to shut down.')
        self._regenerate()
        # not waiting for ack, return state
        return self._dump_state()
        
    def get_status(self):
        # hand picked status for human/client consumption
        s = {}
        s['instance_counter'] = self._instance_counter
        s['instances'] = self._instances_by_id
        s['failed_instances'] = list(self._failed_instances)
        s['space_counter'] = self._space_counter
        s['spaces'] = self._spaces_by_id
        s['config_counter'] = self._config_counter
        s['config_data'] = self._config_data
        s['xfer_counter'] = self._xfer_counter
        s['xfers'] = self._xfers_by_id
        s['state'] = self._state
        s['quiesce_state_id'] = self._quiesce_state_id
        s['service_level'] = self._service_level()
        s['service_level_met'] = self._service_level_met()
        return s

    def go_live(self):
        if self._state != Coordinator.S_STARTUP:
            raise Coordinator.InvalidState()
        # TODO:
        # - check we have enough nodes joined back, fail with InvalidState() if not yet
        # - send config with state_id to be restored to all hosts
        # - fail the nodes that have not replied on time
        self._state = Coordinator.S_NORMAL
        logging.info('Pushing config to all hosts.')
        self._regenerate()
        logging.info('Cluster is now fully operational.')

    def backup_state(self):
        return self._dump_state()

    def _compute_transfer_id(self, spaceid, subspaceid, regionid):
        xferid = None
        for i in xrange(1 << 16):
            tmp = self._xfer_counter
            self._xfer_counter = (self._xfer_counter + 1) % 2**16
            if tmp not in self._xfers_by_id:
                xferid = tmp
                break
        if xferid is None:
            return None
        self._xfers_by_id[xferid] = (spaceid, subspaceid, regionid)
        return xferid

    def fail_host(self, ip, port):
        badinstids = set()
        for instid, inst in self._instances_by_id.iteritems():
            if inst.addr == ip and \
                    (inst.inport == port or inst.outport == port):
                badinstids.add(instid)
        self._failed_instances |= badinstids
        for si, space in self._spaces_by_id.iteritems():
            for ssi, subspace in enumerate(space.subspaces):
                for ri, region in enumerate(subspace.regions):
                    region.remove_instances(badinstids)
                    for i in range(region.desired_f - region.current_f):
                        xferid = self._compute_transfer_id(si, ssi, ri)
                        newrepl = self._select_replica(region.replicas + region.transfers)
                        if xferid is not None and newrepl is not None:
                            region.transfer_initiate(xferid, newrepl)
        self._regenerate()

    def ack_config(self, bindings, num):
        instid = self._instances_by_bindings[bindings]
        inst = self._instances_by_id[instid]
        inst.ack_config(num)

    def reject_config(self, bindings, num):
        instid = self._instances_by_bindings[bindings]
        inst = self._instances_by_id[instid]
        inst.reject_config(num)

    def fetch_configs(self, instances):
        configs = {}
        hosts = {}
        for inst in instances:
            if inst is None:
                configs[self._config_counter] = self._config_data
                hosts[None] = self._config_counter
            elif inst in self._instances_by_bindings:
                instid = self._instances_by_bindings[inst]
                realinst = self._instances_by_id[instid]
                num, data = realinst.next_config()
                if num is not None:
                    configs[num] = data
                    hosts[inst] = num
        return configs, hosts

    def transfer_fail(self, xferid):
        if xferid not in self._xfers_by_id:
            return
        spaceid, subspaceid, regionid = self._xfers_by_id[xferid]
        if spaceid not in self._spaces_by_id:
            return
        self._spaces_by_id[spaceid].subspaces[subspaceid].regions[regionid].transfer_fail(xferid)
        del self._xfers_by_id[xferid]
        self._regenerate()

    def transfer_golive(self, xferid):
        if xferid not in self._xfers_by_id:
            return
        spaceid, subspaceid, regionid = self._xfers_by_id[xferid]
        if spaceid not in self._spaces_by_id:
            return
        region = self._spaces_by_id[spaceid].subspaces[subspaceid].regions[regionid]
        if region.transfer_golive(xferid):
            self._regenerate()

    def transfer_complete(self, xferid):
        if xferid not in self._xfers_by_id:
            return
        spaceid, subspaceid, regionid = self._xfers_by_id[xferid]
        if spaceid not in self._spaces_by_id:
            return
        self._spaces_by_id[spaceid].subspaces[subspaceid].regions[regionid].transfer_complete(xferid)
        del self._xfers_by_id[xferid]
        self._regenerate()

    def _initial_layout(self, space):
        still_assigning = True
        while still_assigning:
            still_assigning = False
            for subspace in space.subspaces:
                for region in subspace.regions:
                    if region.current_f < region.desired_f:
                        replica = self._select_replica(region.replicas)
                        if replica is not None:
                            still_assigning = True
                            region.add_replica(replica)

    def _select_replica(self, exclude):
        hosts = dict([(k, 0) for k in self._instances_by_id.keys()])
        for spacenum, space in self._spaces_by_id.iteritems():
            for subspacenum, subspace in enumerate(space.subspaces):
                for region in subspace.regions:
                    for replica in region.replicas:
                        hosts[replica] += 1
        frequencies = collections.defaultdict(list)
        for host, frequency in hosts.iteritems():
            if host not in exclude and host not in self._failed_instances:
                frequencies[frequency].append(host)
        if not frequencies:
            return None
        least_loaded = min(frequencies.iteritems())[1]
        return self._rand.choice(least_loaded)

    def _regenerate(self):
        used_hosts = set()
        self._config_counter += 1
        config = 'version {0}\n'.format(self._config_counter)
        for spaceid, space in self._spaces_by_id.iteritems():
            spacedims = ' '.join([d.name + ' ' + d.datatype for d in space.dimensions])
            config += SPACE_LINE \
                      .format(name=space.name, id=spaceid, dims=spacedims)
            for subspaceid, subspace in enumerate(space.subspaces):
                hashes = []
                for dim in space.dimensions:
                    if dim.name in subspace.dimensions:
                        hashes.append('true')
                    else:
                        hashes.append('false')
                    if dim.name not in subspace.nosearch:
                        hashes.append('true')
                    else:
                        hashes.append('false')
                config += SUBSPACE_LINE \
                          .format(space=spaceid, subspace=subspaceid,
                                  hashes=' '.join(hashes))
                for region in subspace.regions:
                    hosts = [str(r) for r in region.replicas]
                    for host in region.replicas:
                        used_hosts.add(host)
                    config += REGION_LINE \
                              .format(space=spaceid, subspace=subspaceid,
                                      prefix=region.prefix,
                                      mask=hex(region.mask).rstrip('L'),
                                      hosts=' '.join(hosts))
                    xferid, instid = region.transfer_in_progress
                    if xferid is not None:
                        config += TRANSFER_LINE \
                                  .format(xferid=str(xferid),
                                          space=spaceid,
                                          subspace=subspaceid,
                                          prefix=region.prefix,
                                          mask=hex(region.mask).rstrip('L'),
                                          instid=str(instid))
                        used_hosts.add(instid)
        # quiesce request
        if (self._state == Coordinator.S_QUIESCE):
            config += QUIESCE_LINE.format(state_id = self._quiesce_state_id)
        # shutdown request
        if (self._state == Coordinator.S_SHUTDOWN):
            config += SHUTDOWN_LINE
        host_lines = []
        for hostid in sorted(used_hosts):
            inst = self._instances_by_id[hostid]
            host_line = HOST_LINE \
                        .format(id=hostid, ip=inst.addr, inport=inst.inport,
                                inver=inst.inver, outport=inst.outport,
                                outver=inst.outver)
            host_lines.append(host_line)
        self._config_data = ('\n'.join(host_lines) + '\n' + config).strip()
        # do not send config before go-live
        if self._state != Coordinator.S_STARTUP:
            for instid, inst in self._instances_by_id.iteritems():
                inst.add_config(self._config_counter, self._config_data)
        # XXX notify threads to poll for new configs
        
    def _service_level(self):
        sl = Coordinator.SL_DESIRED
        for space in self._spaces_by_id.values():
            for subspace in space.subspaces:
                for region in subspace.regions:
                    if region.current_f < 0:
                        # no need to search more, it is bad
                        return Coordinator.SL_DATALOSS
                    if region.current_f < region.desired_f:
                        # continue search, some region could be worse
                        sl = Coordinator.SL_DEGRADED
        return sl

    def _service_level_met(self):
        return self._service_level() != Coordinator.SL_DATALOSS

    def _dump_state(self):
        e = hdjson.Encoder()
        s = {}
        for attr, value in self.__dict__.iteritems():
            # dict. with non-string keys must be normalized for JSON encoding
            if attr in [ "_portcounters", "_instances_by_bindings" ]:
                s[attr] = e.normalizeDictKeys(value, hdjson.KEYS_TUPLE)
            elif attr in [ "_instances_by_id", "_spaces_by_id", "_xfers_by_id" ]:
                s[attr] = e.normalizeDictKeys(value, hdjson.KEYS_INT)
            else:
                s[attr] = value
        s['state_version'] = Coordinator.STATE_VERSION
        return hdjson.Encoder(**hdjson.HumanReadable).encode(s)
        
    def _load_state(self, state):
        d = hdjson.Decoder()
        try:
            s = d.decode(state)
        except ValueError as e:
            logging.error("Error decoding given state information".format(e))
            raise Coordinator.InvalidStateData()
        if not s.has_key('state_version'):
            logging.error("Invalid state data used (corrupted or invalid)")
            raise Coordinator.InvalidStateData()
        if s['state_version'] != Coordinator.STATE_VERSION:
            logging.error("Invalid state data used (incompatible version)")
            raise Coordinator.InvalidStateData()
        for attr, value in s.iteritems():
            # dict. with non-string keys must be manually denormalized from JSON encoding
            if attr in [ "_instances_by_bindings" ]:
                setattr(self, attr, d.denormalizeDictKeys(value, hdjson.KEYS_TUPLE))
            elif attr in [ "_instances_by_id", "_spaces_by_id", "_xfers_by_id" ]:
                setattr(self, attr, d.denormalizeDictKeys(value, hdjson.KEYS_INT))
            # default dict must be manually created
            elif attr == "_portcounters":
                v = d.denormalizeDictKeys(value, hdjson.KEYS_TUPLE)
                setattr(self, attr, collections.defaultdict(lambda: 1, v))
            else:
                setattr(self, attr, value)
        logging.info('State from shutdown {0} restored'.format(self._quiesce_state_id))


class HostConnection(object):

    def __init__(self, coordinator, sock):
        self._connectime = datetime.datetime.now()
        self._coordinator = coordinator
        self._sock = sock
        self._ibuffer = ''
        self.outgoing = ''
        self._identified = None
        self._instance = None # Must be None unless self._identified is INSTANCE
        self._has_config_pending = False
        self._pending_config_num = 0
        self._id = 'Unidentified({0}, {1})'.format(self._sock.getpeername()[0], self._sock.getpeername()[1])
        logging.info('new host uses ID ' + self._id)

    def handle_in(self):
        data = self._sock.recv(PIPE_BUF)
        if len(data) == 0:
            raise EndConnection()
        self._ibuffer += data
        while '\n' in self._ibuffer:
            data, self._ibuffer = self._ibuffer.split('\n', 1)
            commandline = shlex.split(data)
            if len(commandline) == 1 and commandline[0] == 'client':
                self.identify_as_client()
            elif len(commandline) == 6 and commandline[0] == 'instance':
                self.identify_as_instance(*commandline[1:])
            elif len(commandline) == 1 and commandline[0] == 'ACK':
                if self._identified == 'INSTANCE':
                    self._coordinator.ack_config(self._instance, self._pending_config_num)
                    logging.debug("{0} acked config {1}".format(self._id, self._pending_config_num))
                self._has_config_pending = False
            elif len(commandline) == 1 and commandline[0] == 'BAD':
                if self._identified == 'INSTANCE':
                    self._coordinator.reject_config(self._instance, self._pending_config_num)
                    logging.error("{0} rejected config (there is a bug!)".format(self._id))
                self._has_config_pending = False
            elif len(commandline) == 2 and commandline[0] == 'fail_host':
                self.fail_host(commandline[1])
            elif len(commandline) == 2 and commandline[0] == 'transfer_fail':
                self.transfer_fail(commandline[1])
                logging.info("transfer fail {0}".format(commandline[1]))
            elif len(commandline) == 2 and commandline[0] == 'transfer_golive':
                self.transfer_golive(commandline[1])
                logging.debug("transfer golive {0}".format(commandline[1]))
            elif len(commandline) == 2 and commandline[0] == 'transfer_complete':
                self.transfer_complete(commandline[1])
                logging.debug("transfer complete {0}".format(commandline[1]))
            else:
                raise KillConnection('unrecognized command "{0}"'.format(repr(data)))

    def handle_out(self):
        data = self.outgoing[:PIPE_BUF]
        sz = self._sock.send(data)
        self.outgoing = self.outgoing[sz:]

    def identify_as_client(self):
        if self._identified:
            raise KillConnection('client identifying twice')
        self._identified = 'CLIENT'
        oldid = self._id
        addr, port = self._sock.getpeername()
        self._id = 'Client({0}, {1})'.format(addr, port)
        logging.info('{0} identified by {1}'.format(oldid, self._id))

    def identify_as_instance(self, addr, incoming, outgoing, pid, token):
        if self._identified:
            raise KillConnection('instance identifying twice')
        self._identified = 'INSTANCE'
        addr = normalize_address(addr)
        if not addr:
            raise KillConnection('instance claims to bind to unparseable address')
        try:
            incoming = int(incoming)
        except ValueError:
            raise KillConnection('instance claims to bind to non-numeric incoming port')
        try:
            outgoing = int(outgoing)
        except ValueError:
            raise KillConnection('instance claims to bind to non-numeric outgoing port')
        try:
            pid = int(pid)
        except ValueError:
            raise KillConnection('instance claims to have non-numeric PID')
        if len(token) != 32:
            raise KillConnection('instance uses token of improper length')
        self._instance = self._coordinator.register_instance(addr, incoming, outgoing, pid, token)
        oldid = self._id
        self._id = str(self._instance)
        logging.info('{0} identified by {1}'.format(oldid, self._id))

    def fail_host(self, host):
        if ':' not in host:
            raise KillConnection('host reports failure of invalid address')
        ip, port = host.rsplit(':', 1)
        ip = normalize_address(ip)
        if ip is None:
            raise KillConnection('host reports failure of invalid address')
        try:
            port = int(port)
        except ValueError:
            raise KillConnection('host reports failure of invalid address')
        logging.info("report of failure for {0}".format(host))
        self._coordinator.fail_host(ip, port)

    def transfer_fail(self, xferid):
        try:
            xferid = int(xferid)
        except ValueError:
            raise KillConnection("host uses non-numeric transfer id for transfer_fail")
        self._coordinator.transfer_fail(xferid)

    def transfer_golive(self, xferid):
        try:
            xferid = int(xferid)
        except ValueError:
            raise KillConnection("host uses non-numeric transfer id for transfer_golive")
        self._coordinator.transfer_golive(xferid)

    def transfer_complete(self, xferid):
        try:
            xferid = int(xferid)
        except ValueError:
            raise KillConnection("host uses non-numeric transfer id for transfer_complete")
        self._coordinator.transfer_complete(xferid)

    def send_config(self, num, data):
        self._has_config_pending = True
        self.outgoing += (data + '\nend of line').strip() + '\n'
        self._pending_config_num = num

    def has_config_pending(self):
        return self._has_config_pending

    def last_config_num(self):
        return self._pending_config_num


class ControlConnection(object):

    def __init__(self, coordinator, sock, conns):
        self._connectime = datetime.datetime.now()
        self._coordinator = coordinator
        self._sock = sock
        self._ibuffer = ''
        self.outgoing = ''
        self._delim = '\n'
        self._currreq = None
        self._identified = 'CONTROL'
        self._id = str(self._sock.getpeername())
        self._conns = conns

    def handle_in(self):
        data = self._sock.recv(PIPE_BUF)
        if len(data) == 0:
            raise EndConnection()
        self._ibuffer += data
        while self._delim in self._ibuffer:
            data, self._ibuffer = self._ibuffer.split(self._delim, 1)
            try:
                req = json.loads(data)
            except ValueError as e:
                raise KillConnection("Control connection got non-JSON message {0}".format(data))
            if type(req) != dict:
                raise KillConnection("Control connection got invalid JSON message {0}".format(data))
            for r, rv in req.items():
                self._currreq = r
                if r == 'add-space':
                    self.add_space(rv)
                elif r == 'del-space':
                    self.del_space(rv)
                elif r == 'lst-spaces':
                    self.lst_spaces()
                elif r == 'get-space':
                    self.get_space(rv)
                elif r == 'quiesce':
                    self.quiesce()
                elif r == 'shutdown':
                    self.shutdown()
                elif r == 'get-status':
                    self.get_status()
                elif r == 'go-live':
                    self.go_live()
                elif r == 'backup-state':
                    self.backup_state()
                else:
                    raise KillConnection("Control connection got invalid request {0}".format(r))
                    
    def handle_out(self):
        data = self.outgoing[:PIPE_BUF]
        sz = self._sock.send(data)
        self.outgoing = self.outgoing[sz:]

    def get_status(self):
        status = self._coordinator.get_status()
        s = hdjson.Encoder(**hdjson.HumanReadable).encode(status)
        self.outgoing += json.dumps({self._currreq:s}) + '\n'

    def add_space(self, data):
        try:
            parser = (hypercoordinator.parser.space + pyparsing.stringEnd)
            space = parser.parseString(data)[0]
            self._coordinator.add_space(space)
        except ValueError as e:
            return self._fail(str(e))
        except pyparsing.ParseException as e:
            return self._fail(str(e))
        except Coordinator.DuplicateSpace as e:
            return self._fail("Space already exists")
        except Coordinator.InvalidState as e:
            return self._fail("Coordinator state does not allow handling this request")
        except Coordinator.ServiceLevelNotMet as e:
            return self._fail("Service level must be met before adding spaces (need more daemons)")
        self.outgoing += json.dumps({self._currreq:'SUCCESS'}) + '\n'
        logging.info("created new space \"{0}\"".format(space.name))

    def del_space(self, space):
        try:
            self._coordinator.del_space(space)
        except ValueError as e:
            return self._fail(str(e))
        except Coordinator.UnknownSpace as e:
            return self._fail("Space does not exist")
        except Coordinator.InvalidState as e:
            return self._fail("Coordinator state does not allow handling this request")
        except Coordinator.ServiceLevelNotMet as e:
            return self._fail("Service level must be met before deleting spaces (need more daemons)")
        self.outgoing += json.dumps({self._currreq:'SUCCESS'}) + '\n'
        logging.info("removed space \"{0}\"".format(space))

    def lst_spaces(self):
        spaces = '\n'.join([s for s in self._coordinator.lst_spaces()])
        self.outgoing += json.dumps({self._currreq:spaces}) + '\n'

    def get_space(self, space):
        try:
            space = self._coordinator.get_space(space)
        except ValueError as e:
            return self._fail(str(e))
        except Coordinator.UnknownSpace as e:
            return self._fail("Space does not exist")
        s = hdjson.Encoder(**hdjson.HumanReadable).encode(space)
        self.outgoing += json.dumps({self._currreq:s}) + '\n'

    def quiesce(self):
        try:
            state_id = self._coordinator.quiesce()
        except Coordinator.InvalidState as e:
            return self._fail("Coordinator state does not allow handling this request")
        self.outgoing += json.dumps({self._currreq:state_id}) + '\n'
        logging.info("quiesce request processed")

    def shutdown(self):
        try:
            state = self._coordinator.shutdown()
        except Coordinator.InvalidState as e:
            return self._fail("Coordinator state does not allow handling this request")
        self.outgoing += json.dumps({self._currreq:state}) + '\n'
        logging.info("shutdown request processed")

    def go_live(self):
        try:
            self._coordinator.go_live()
        except Coordinator.InvalidState as e:
            return self._fail("Coordinator state does not allow handling this request")
        except Coordinator.ServiceLevelNotMet as e:
            return self._fail("Service level must be met before go live (need more daemons)")
        self.outgoing += json.dumps({self._currreq:'SUCCESS'}) + '\n'
        logging.info("go live request processed")

    def _fail(self, msg):
        error = 'failing control connection {0}: {1}'.format(self._id, msg)
        self.outgoing += json.dumps({self._currreq:'ERROR', 'error':msg}) + '\n'
        logging.error(error)

        
class CoordinatorServer(object):

    def __init__(self, bindto, control_port, host_port, state_data=None):
        self._control_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        self._control_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._control_listen.bind((bindto, control_port))
        self._control_listen.listen(128)
        self._host_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        self._host_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._host_listen.bind((bindto, host_port))
        self._host_listen.listen(65536)
        self._p = select.poll()
        self._p.register(self._host_listen)
        self._p.register(self._control_listen)
        self._conns = {}
        self._coord = Coordinator(state_data)

    def run(self):
        instances_to_fds = {}
        client_fds = set()
        while True:
            fds = self._p.poll(1000)
            for fd, ev in fds:
                if fd == self._control_listen.fileno():
                    conn = self._control_listen.accept()
                    if conn:
                        sock, addr = conn
                        self._p.register(sock)
                        self._conns[sock.fileno()] = sock, ControlConnection(self._coord, sock, self._conns)
                if fd == self._host_listen.fileno():
                    conn = self._host_listen.accept()
                    if conn:
                        sock, addr = conn
                        self._p.register(sock)
                        self._conns[sock.fileno()] = sock, HostConnection(self._coord, sock)
                if fd in self._conns:
                    sock, conn = self._conns[fd]
                    remove = False
                    try:
                        if ev & select.POLLERR:
                            remove = True
                        elif ev & select.POLLHUP:
                            remove = True
                        elif ev & select.POLLNVAL:
                            remove = True
                        if not remove and ev & select.POLLIN:
                            conn.handle_in()
                        if not remove and ev & select.POLLOUT:
                            conn.handle_out()
                            if conn.outgoing == '':
                                self._p.modify(fd, select.POLLIN)
                    except KillConnection as kc:
                        logging.error('connection killed: ' + str(kc))
                        remove = True
                    except EndConnection as ec:
                        remove = True
                    except socket.error as e:
                        remove = True
                    if remove:
                        del self._conns[fd]
                        if fd in client_fds:
                            client_fds.remove(fd)
                        if conn._identified == 'INSTANCE' and \
                           conn._instance in instances_to_fds:
                            del instances_to_fds[conn._instance]
                        self._p.unregister(fd)
                        try:
                            sock.shutdown(socket.SHUT_RDWR)
                            sock.close()
                        except socket.error as e:
                            pass
                    else:
                        if conn._identified == 'CLIENT':
                            client_fds.add(fd)
                        if conn._identified == 'INSTANCE' and \
                           not conn.has_config_pending():
                            instances_to_fds[conn._instance] = fd
            instances = set(instances_to_fds.keys())
            instances.add(None)
            configs, hosts = self._coord.fetch_configs(instances)
            for instance, confignum in hosts.iteritems():
                c = configs[confignum]
                if instance is None:
                    for fd in client_fds:
                        conn = self._conns[fd][1]
                        if conn.last_config_num() < confignum:
                            conn.send_config(confignum, c)
                            self._p.modify(fd, select.POLLIN | select.POLLOUT)
                elif instance in instances_to_fds:
                    fd = instances_to_fds[instance]
                    self._conns[fd][1].send_config(confignum, c)
                    self._p.modify(fd, select.POLLIN | select.POLLOUT)
                    del instances_to_fds[instance]


def main(argv):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--bindto', default='')
    parser.add_argument('-c', '--control-port', type=int, default=0)
    parser.add_argument('-p', '--host-port', type=int, default=0)
    parser.add_argument('-l', '--logging', default='info',
            choices=['debug', 'info', 'warn', 'error', 'critical',
                     'DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'])
    parser.add_argument('-s', '--state-file', default='')
    args = parser.parse_args(argv)
    level = {'debug': logging.DEBUG
            ,'info': logging.INFO
            ,'warn': logging.WARN
            ,'error': logging.ERROR
            ,'critical': logging.CRITICAL
            }.get(args.logging.lower(), None)
    try:
        logging.basicConfig(level=level)
        state_data = open(args.state_file, 'r').read() if args.state_file else ""
        cs = CoordinatorServer(args.bindto, args.control_port, args.host_port, state_data)
        logging.info('Coordinator started')
        cs.run()
    except Coordinator.InvalidStateData as ise:
        logging.error("Error loading state from file {0}".format(args.state_file))
    except socket.error as se:
        logging.error("%s [%d]" % (se.strerror, se.errno))
    except IOError as ioe:
        logging.error("%s [%d]" % (ioe.strerror, ioe.errno))

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
