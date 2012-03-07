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
import select
import shlex
import socket
import sys

import pyparsing

import hdcoordinator.parser
from hdcoordinator import hdtypes


PIPE_BUF = getattr(select, 'PIPE_BUF', 512)
RESTRICTED_SPACES = set([0, 2**32 - 1, 2**32 - 2, 2**32 - 3, 2**32 - 4])

# Format strings for configuration lines
SPACE_LINE = 'space {name} {id} {dims}\n'
SUBSPACE_LINE = 'subspace {space} {subspace} {hashes}\n'
REGION_LINE = 'region {space} {subspace} {prefix} {mask} {hosts}\n'
TRANSFER_LINE = 'transfer {xferid} {space} {subspace} {prefix} {mask} {instid}\n'
HOST_LINE = 'host {id} {ip} {inport} {inver} {outport} {outver}'

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

    def __init__(self):
        self._portcounters = collections.defaultdict(lambda: 1)
        self._instance_counter = 1
        self._instances_by_addr = {}
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

    def _compute_port_epoch(self, addr, port):
        ver = self._portcounters[(addr, port)]
        self._portcounters[(addr, port)] += 1
        if ver >= (1 << 16):
            raise Coordinator.ExhaustedPorts()
        return ver

    def register_instance(self, addr, inport, outport, pid, token):
        instid = self._instances_by_addr.get((addr, inport, outport, pid, token), 0)
        if instid != 0:
            return self._instances_by_id[instid].bindings()
        inver = self._compute_port_epoch(addr, inport)
        outver = self._compute_port_epoch(addr, outport)
        inst = hdtypes.Instance(addr, inport, inver, outport, outver, pid, token)
        inst.add_config(self._config_counter, self._config_data)
        instid = self._instance_counter
        self._instance_counter += 1
        self._instances_by_id[instid] = inst
        self._instances_by_bindings[inst.bindings()] = instid
        self._instances_by_addr[(addr, inport, outport, pid, token)] = instid
        return inst.bindings()

    def keepalive_instance(self, bindings):
        pass

    def add_space(self, space):
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
        if space not in self._spaces_by_name:
            raise Coordinator.UnknownSpace()
        spacenum = self._spaces_by_name[space]
        del self._spaces_by_name[space]
        del self._spaces_by_id[spacenum]
        self._regenerate()

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
        host_lines = []
        for hostid in sorted(used_hosts):
            inst = self._instances_by_id[hostid]
            host_line = HOST_LINE \
                        .format(id=hostid, ip=inst.addr, inport=inst.inport,
                                inver=inst.inver, outport=inst.outport,
                                outver=inst.outver)
            host_lines.append(host_line)
        self._config_data = ('\n'.join(host_lines) + '\n' + config).strip()
        for instid, inst in self._instances_by_id.iteritems():
            inst.add_config(self._config_counter, self._config_data)
        # XXX notify threads to poll for new configs


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
        self._mode = "COMMAND"
        self._action = None
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
            if self._mode == 'COMMAND':
                commandline = shlex.split(data)
                if len(commandline) == 2 and commandline == ['add', 'space']:
                    self._delim = '\n.\n'
                    self._mode = 'DATA'
                    self._action = self.add_space
                elif len(commandline) == 3 and commandline[:2] == ['del', 'space']:
                    self.del_space(commandline[2])
            elif self._mode == 'DATA':
                self._action(data)
                self._delim = '\n'
                self._mode = 'COMMAND'
            else:
                raise KillConnection('Control connection with unknown mode {0}'.format(repr(self._mode)))

    def handle_out(self):
        data = self.outgoing[:PIPE_BUF]
        sz = self._sock.send(data)
        self.outgoing = self.outgoing[sz:]

    def add_space(self, data):
        try:
            parser = (hdcoordinator.parser.space + pyparsing.stringEnd)
            space = parser.parseString(data)[0]
            self._coordinator.add_space(space)
        except ValueError as e:
            return self._fail(str(e))
        except pyparsing.ParseException as e:
            return self._fail(str(e))
        except Coordinator.DuplicateSpace as e:
            return self._fail("Space already exists")
        self.outgoing += 'SUCCESS\n'
        logging.info("created new space \"{0}\"".format(space.name))

    def del_space(self, space):
        try:
            self._coordinator.del_space(space)
        except ValueError as e:
            return self._fail(str(e))
        except Coordinator.UnknownSpace as e:
            return self._fail("Space does not exist")
        self.outgoing += 'SUCCESS\n'
        logging.info("removed space \"{0}\"".format(space))

    def _fail(self, msg):
        error = 'failing control connection {0}: {1}'.format(self._id, msg)
        logging.error(error)
        self.outgoing += msg + '\n'


class CoordinatorServer(object):

    def __init__(self, bindto, control_port, host_port):
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
        self._coord = Coordinator()

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
    args = parser.parse_args(argv)
    level = {'debug': logging.DEBUG
            ,'info': logging.INFO
            ,'warn': logging.WARN
            ,'error': logging.ERROR
            ,'critical': logging.CRITICAL
            }.get(args.logging.lower(), None)
    try:
        logging.basicConfig(level=level)
        cs = CoordinatorServer(args.bindto, args.control_port, args.host_port)
        cs.run()
    except socket.error as se:
        logging.error("%s [%d]" % (se.strerror, se.errno))

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
