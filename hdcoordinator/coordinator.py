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


PIPE_BUF = getattr(select, 'PIPE_BUF', 512)
RESTRICTED_SPACES = set([0, 2**32 - 1, 2**32 - 2, 2**32 - 3, 2**32 - 4])

# Format strings for configuration lines
SPACE_LINE = 'space {name} {id} {dims}\n'
SUBSPACE_LINE = 'subspace {space} {subspace} {hashes}\n'
REGION_LINE = 'region {space} {subspace} {prefix} {mask} {hosts}\n'
HOST_LINE = 'host {id} {ip} {inport} {inver} {outport} {outver}\n'

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


Client = collections.namedtuple('Client', 'addr port')
Instance = collections.namedtuple('Instance', 'addr inport inver outport outver pid token')


class Coordinator(object):

    class DuplicateSpace(Exception): pass
    class UnknownSpace(Exception): pass
    class ExhaustedPorts(Exception): pass
    class ExhaustedSpaces(Exception): pass

    def __init__(self):
        self._portcounters = collections.defaultdict(lambda: 1)
        self._instance_counter = 1
        self._instances_by_addr = {}
        self._instances_by_id = {}
        self._space_counter = 1
        self._spaces_by_name = {}
        self._spaces_by_id = {}
        self._rand = random.Random()
        self._conf_counter = 0
        self._conf_data = ''

    def _compute_port_epoch(self, addr, port):
        ver = self._portcounters[(addr, port)]
        self._portcounters[(addr, port)] += 1
        if ver >= (1 << 16):
            raise Coordinator.ExhaustedPorts()
        return ver

    def register_instance(self, addr, inport, outport, pid, token):
        instid = self._instances_by_addr.get((addr, inport, outport, pid, token), 0)
        if instid != 0:
            return self._instances_by_id[instid]
        inver = self._compute_port_epoch(addr, inport)
        outver = self._compute_port_epoch(addr, outport)
        inst = Instance(addr, inport, inver, outport, outver, pid, token)
        instid = self._instance_counter
        self._instance_counter += 1
        self._instances_by_id[instid] = inst
        self._instances_by_addr[(addr, inport, outport, pid, token)] = instid
        return inst

    def keepalive_instance(self, instance):
        pass

    def list_instances(self):
        return sorted(self._instances_by_id.values())

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

    def _fail_host_cmp(self, lhs, rhs):
        if lhs is None and rhs is None:
            return 0
        elif lhs is None:
            return 1
        elif rhs is None:
            return -1
        return 0

    def fail_host(self, ip, port):
        for spacenum, space in self._spaces_by_id.iteritems():
            for subspacenum, subspace in enumerate(space.subspaces):
                for region in subspace.regions:
                    for replicanum in range(len(region.replicas)):
                        if region.replicas[replicanum] and \
                           region.replicas[replicanum][0] == ip and \
                           (region.replicas[replicanum][1] == port or
                            region.replicas[replicanum][3] == port):
                            region.replicas[replicanum] = None
                    region.replicas.sort(cmp=self._fail_host_cmp)
        self._regenerate()

    def configuration(self):
        return self._conf_counter, self._conf_data

    def _initial_layout(self, space):
        still_assigning = True
        while still_assigning:
            still_assigning = False
            for subspacenum, subspace in enumerate(space.subspaces):
                for region in subspace.regions:
                    for replicanum in range(len(region.replicas)):
                        if region.replicas[replicanum] is None:
                            region.replicas[replicanum] = self._select_replica(region.replicas)
                            if region.replicas[replicanum] is not None:
                                still_assigning = True
                            break

    def _select_replica(self, exclude):
        hosts = dict([(k, 0) for k in self._instances_by_id.keys()])
        for spacenum, space in self._spaces_by_id.iteritems():
            for subspacenum, subspace in enumerate(space.subspaces):
                for region in subspace.regions:
                    for replica in region.replicas:
                        if replica is not None:
                            hosts[replica] += 1
        frequencies = collections.defaultdict(list)
        for host, frequency in hosts.iteritems():
            if host not in exclude:
                frequencies[frequency].append(host)
        if not frequencies:
            return None
        least_loaded = min(frequencies.iteritems())[1]
        return self._rand.choice(least_loaded)

    def _regenerate(self):
        used_hosts = set()
        config = 'version {0}\n'.format(self._conf_counter + 1)
        for spaceid, space in self._spaces_by_id.iteritems():
            spacedims = ' '.join([d.name + ' ' + d.type for d in space.dimensions])
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
                    hosts = [str(r) for r in region.replicas if r is not None]
                    for host in region.replicas:
                        if host is not None:
                            used_hosts.add(host)
                    config += REGION_LINE \
                              .format(space=spaceid, subspace=subspaceid,
                                      prefix=region.prefix,
                                      mask=hex(region.mask).rstrip('L'),
                                      hosts=' '.join(hosts))
        host_lines = []
        for hostid in sorted(used_hosts):
            inst = self._instances_by_id[hostid]
            print hostid, inst, self._instances_by_id
            host_line = HOST_LINE \
                        .format(id=hostid, ip=inst.addr, inport=inst.inport,
                                inver=inst.inver, outport=inst.outport,
                                outver=inst.outver)
            host_lines.append(host_line)
        self._conf_counter += 1
        self._conf_data = ('\n'.join(host_lines) + config).strip()


class HostConnection(object):

    class CLIENT: pass
    class INSTANCE: pass

    def __init__(self, coordinator, sock, conns):
        self._connectime = datetime.datetime.now()
        self._coordinator = coordinator
        self._sock = sock
        self._ibuffer = []
        self.outgoing = ''
        self._identified = None
        self._instance = None # Must be None unless self._identified is INSTANCE
        self._state = 'UNCONFIGURED'
        self._config = None
        self._pending = []
        self._id = 'Unidentified({0}, {1})'.format(self._sock.getpeername()[0], self._sock.getpeername()[1])
        self._conns = conns
        logging.info('new host uses ID ' + self._id)

    def handle_in(self):
        data = self._sock.recv(PIPE_BUF)
        if len(data) == 0:
            raise EndConnection()
        self._ibuffer.append(data)
        while '\n' in data:
            data, rem = ''.join(self._ibuffer).split('\n', 1)
            self._ibuffer = [rem]
            commandline = shlex.split(data)
            if len(commandline) == 1 and commandline[0] == 'client':
                self.identify_as_client()
            elif len(commandline) == 6 and commandline[0] == 'instance':
                self.identify_as_instance(*commandline[1:])
            elif len(commandline) == 1 and commandline[0] == 'ACK':
                self.acknowledge_config()
            elif len(commandline) == 1 and commandline[0] == 'BAD':
                self.reject_config()
            elif len(commandline) == 2 and commandline[0] == 'fail_host':
                self.fail_host(commandline[1])
            else:
                raise KillConnection('Unrecognized command {0}'.format(repr(data)))

    def handle_out(self):
        data = self.outgoing[:PIPE_BUF]
        sz = self._sock.send(data)
        self.outgoing = self.outgoing[sz:]

    def handle_err(self):
        pass

    def handle_hup(self):
        pass

    def handle_nval(self):
        pass

    def identify_as_client(self):
        if self._identified:
            raise KillConnection('Client identifying twice')
        self._identified = HostConnection.CLIENT
        self._state = 'CONFIGURED'
        oldid = self._id
        addr, port = self._sock.getpeername()
        self._id = str(Client(addr, port))
        logging.info('{0} identified by {1}'.format(oldid, self._id))
        self.reconfigure(self._coordinator.configuration())

    def identify_as_instance(self, addr, incoming, outgoing, pid, token):
        if self._identified:
            raise KillConnection('Client identifying twice')
        self._identified = HostConnection.INSTANCE
        addr = normalize_address(addr)
        if not addr:
            raise KillConnection('Client claims to bind to unparseable address')
        try:
            incoming = int(incoming)
        except ValueError:
            raise KillConnection('Client claims to bind to non-numeric incoming port')
        try:
            outgoing = int(outgoing)
        except ValueError:
            raise KillConnection('Client claims to bind to non-numeric outgoing port')
        try:
            pid = int(pid)
        except ValueError:
            raise KillConnection('Client claims to have non-numeric PID')
        self._instance = self._coordinator.register_instance(addr, incoming, outgoing, pid, token)
        self._state = 'CONFIGURED'
        oldid = self._id
        self._id = str(self._instance)
        logging.info('{0} identified by {1}'.format(oldid, self._id))
        self.reconfigure(self._coordinator.configuration())

    def reconfigure(self, newconfig):
        if self._state == 'UNCONFIGURED':
            logging.debug('{0} is UNCONFIGURED; not following reconfiguration'.format(self._id))
            pass
        elif self._state in ('CONFIGURED', 'CONFIGFAILED'):
            self._state = 'CONFIGPENDING'
            if len(self._pending) != 0:
                raise KillConnection('Connection is configured with pending config')
            self._pending.append(newconfig)
            self.send_config(newconfig)
            logging.debug('{0} is {1}; reconfiguring'.format(self._id, self._state))
        elif self._state == 'CONFIGPENDING':
            self._pending.append(newconfig)
            logging.debug('{0} is CONFIGPENDING; appending config'.format(self._id, self._state))
        else:
            raise KillConnection('Connection wandered into state {0}'.format(self._state))

    def acknowledge_config(self):
        logging.debug('{0} acknowledges config'.format(self._id))
        if not self._identified:
            raise KillConnection('Host responds to a configuration before identiying')
        if self._state != 'CONFIGPENDING':
            raise KillConnection('Host acknowledges a new configuration when none were issued.')
        if len(self._pending) < 1:
            raise KillConnection('Connection should have pending config but has none.')
        self._config = self._pending[0]
        self._pending = self._pending[1:]
        self._state = 'CONFIGURED'
        if len(self._pending) > 0:
            self.send_config(self._pending[0])
            self._state = 'CONFIGPENDING'
            logging.debug('{0} has pending configs; moving to CONFIGPENDING'.format(self._id, self._state))

    def reject_config(self):
        logging.debug('{0} rejects config'.format(self._id))
        if not self._identified:
            raise KillConnection('Host responds to a configuration before identiying')
        if self._state != 'CONFIGPENDING':
            raise KillConnection('Host rejects a new configuration when none were issued.')
        if len(self._pending) < 1:
            raise KillConnection('Host should have pending config but has none.')
        logging.warning('{0} rejects configuration'.format(self._id))
        self._pending = self._pending[1:]
        self._state = 'CONFIGURED'
        if len(self._pending) > 0:
            self.send_config(self._pending[0])
            self._state = 'CONFIGPENDING'
            logging.debug('{0} has pending configs; moving to CONFIGPENDING'.format(self._id, self._state))

    def send_config(self, config):
        num, data = config
        self.outgoing += (data + '\nend of line').strip() + '\n'

    def fail_host(self, host):
        if ':' not in host:
            raise KillConnection('Host reports failure of invalid address')
        ip, port = host.rsplit(':', 1)
        ip = normalize_address(ip)
        if ip is None:
            raise KillConnection('Host reports failure of invalid address')
        try:
            port = int(port)
        except ValueError:
            raise KillConnection('Host reports failure of invalid address')
        logging.info("Report of failure for {0}".format(commandline[1]))
        self._coordinator.fail_host(ip, port)
        self._reconfigure_all()

    def _reconfigure_all(self):
        config = self._coordinator.configuration()
        for sock, conn in self._conns.values():
            if hasattr(conn, 'reconfigure'):
                conn.reconfigure(config)

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
                if len(commandline) == 2 and commandline == ['list', 'clients']:
                    self.list_clients()
                elif len(commandline) == 2 and commandline == ['list', 'instances']:
                    self.list_instances()
                elif len(commandline) == 2 and commandline == ['add', 'space']:
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

    def handle_err(self):
        pass

    def handle_hup(self):
        pass

    def handle_nval(self):
        pass

    def list_clients(self):
        ret = []
        for sock, conn in self._conns.values():
            if getattr(conn, '_identified', None) == HostConnection.CLIENT:
                ret.append((conn._connectime, conn._id))
        resp = '\n'.join(['{0} {1}'.format(d, i) for d, i in ret])
        if resp:
            self.outgoing += resp + '\n.\n'
        else:
            self.outgoing += '.\n'

    def list_instances(self):
        local = {}
        for sock, conn in self._conns.values():
            if getattr(conn, '_identified', None) == HostConnection.INSTANCE:
                local[conn._instance] = conn._connectime, conn._state
        resp = ''
        for inst in sorted(self._coordinator.list_instances()):
            addr, incoming, inc_ver, outgoing, out_ver, pid, token = inst
            if (addr, incoming, outgoing, pid, token) in local:
                connectime = local[(addr, incoming, outgoing, pid, token)][0]
                state = local[(addr, incoming, outgoing, pid, token)][1]
            else:
                connectime = ''
                state = ''
            fmt = '{connectime:26} {addr} {incoming}:{inc_ver} {outgoing}:{out_ver} {pid} {token} {state}\n'
            resp += fmt.format(connectime=str(connectime), addr=addr,
                    incoming=incoming, inc_ver=inc_ver, outgoing=outgoing,
                    out_ver=out_ver, pid=pid, token=token, state=state)
        self.outgoing += resp + '.\n'

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
        self._reconfigure_all()
        self.outgoing += 'SUCCESS\n'
        logging.info("created new space \"{0}\"".format(space.name))

    def del_space(self, space):
        try:
            self._coordinator.del_space(space)
        except ValueError as e:
            return self._fail(str(e))
        except Coordinator.UnknownSpace as e:
            return self._fail("Space does not exist")
        self._reconfigure_all()
        self.outgoing += 'SUCCESS\n'
        logging.info("removed space \"{0}\"".format(space))

    def _reconfigure_all(self):
        config = self._coordinator.configuration()
        for sock, conn in self._conns.values():
            if hasattr(conn, 'reconfigure'):
                conn.reconfigure(config)

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
        while True:
            fds = self._p.poll()
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
                        self._conns[sock.fileno()] = sock, HostConnection(self._coord, sock, self._conns)
                if fd in self._conns:
                    sock, conn = self._conns[fd]
                    remove = False
                    try:
                        if ev & select.POLLERR:
                            conn.handle_err()
                            remove = True
                        elif ev & select.POLLHUP:
                            conn.handle_hup()
                            remove = True
                        elif ev & select.POLLNVAL:
                            conn.handle_nval()
                            remove = True
                        if not remove and ev & select.POLLIN:
                            conn.handle_in()
                        if not remove and ev & select.POLLOUT:
                            conn.handle_out()
                    except KillConnection as kc:
                        logging.error('connection closed unexpectedly: ' + str(kc))
                        remove = True
                    except EndConnection as ec:
                        remove = True
                    if remove:
                        del self._conns[fd]
                        self._p.unregister(fd)
                        try:
                            sock.shutdown(socket.SHUT_RDWR)
                            sock.close()
                        except socket.error as e:
                            pass
            for fd, conn in self._conns.iteritems():
                sock, conn = conn
                if len(conn.outgoing) > 0:
                    self._p.modify(fd, select.POLLIN | select.POLLOUT)
                else:
                    self._p.modify(fd, select.POLLIN)


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
    logging.basicConfig(level=level)
    cs = CoordinatorServer(args.bindto, args.control_port, args.host_port)
    cs.run()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
