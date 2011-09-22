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


import asynchat
import asyncore
import collections
import datetime
import logging
import random
import shlex
import signal
import socket
import uuid

import pyparsing

import hdcoordinator.parser

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


# XXX When we move to using ConCoord, we need to ensure that each call to
# add_instance is accompanied by an rm_instance even in the case where this code
# fails.
class ActionsLog(object):

    class HostExists(Exception): pass
    class DuplicateSpace(Exception): pass
    class UnknownSpace(Exception): pass
    class ExhaustedPorts(Exception): pass

    def __init__(self):
        self._portcounters = collections.defaultdict(int)
        self._instances = {}
        self._spaces_by_name = {}
        self._spaces_by_num = {}
        self._spacenum = 1
        self._stableconfig = ''
        self._confignum = 1
        self._rand = random.Random()

    def add_instance(self, addr, incoming, outgoing):
        if (addr, incoming, outgoing) in self._instances:
            raise ActionsLog.HostExists()
        # Get unique numbers for these ports.
        inc_ver = self._portcounters[(addr, incoming)]
        out_ver = self._portcounters[(addr, outgoing)]
        inc_ver += 1
        out_ver += 1
        self._portcounters[(addr, incoming)] = inc_ver
        self._portcounters[(addr, outgoing)] = out_ver
        # Fail if either exceeds the size of a 16-bit int.
        if inc_ver >= 1 << 16 or out_ver >= 1 << 16:
            raise ActionsLog.ExhaustedPorts()
        self._instances[(addr, incoming, outgoing)] = (inc_ver, out_ver)

    def rm_instance(self, addr, incoming, outgoing):
        # XXX rm_instance should not just wipe out the instance.  When we move
        # to ConCoord (where we have multiple coordinators) rm_instance should
        # put the instance "on notice" and wipe it if and only if someone else
        # reports a failure.

        # XXX remove the failed instance from the configs.
        del self._instances[(addr, incoming, outgoing)]

    def list_instances(self):
        return self._instances

    def add_space(self, space):
        if space.name in self._spaces_by_name:
            raise ActionsLog.DuplicateSpace()
        while True:
            spacenum = self._spacenum
            self._spacenum = (self._spacenum + 1) % ((1 << 32) - 4) + 1
            if spacenum not in self._spaces_by_num:
                break
        self._spaces_by_name[space.name] = spacenum
        self._spaces_by_num[spacenum] = space
        self.fill_regions(space)

    def rm_space(self, space):
        if space not in self._spaces_by_name:
            raise ActionsLog.UnknownSpace()
        spacenum = self._spaces_by_name[space]
        del self._spaces_by_name[space]
        del self._spaces_by_num[spacenum]

    def fill_space(self, space):
        if space not in self._spaces_by_name:
            raise ActionsLog.UnknownSpace()
        spacenum = self._spaces_by_name[space]
        self.fill_regions(self._spaces_by_num[spacenum])

    def fill_regions(self, space):
        for subspacenum, subspace in enumerate(space.subspaces):
            for region in subspace.regions:
                for replicanum in range(len(region.replicas)):
                    if not region.replicas[replicanum]:
                        region.replicas[replicanum] = self.select_least_loaded(region.replicas)

    def select_least_loaded(self, exclude):
        hosts = dict([(k, 0) for k in self._instances.keys()])
        for spacenum, space in self._spaces_by_num.iteritems():
            for subspacenum, subspace in enumerate(space.subspaces):
                for region in subspace.regions:
                    for replica in region.replicas:
                        if replica:
                            hosts[replica] += 1
        frequencies = collections.defaultdict(list)
        for host, frequency in hosts.iteritems():
            frequencies[frequency].append(host)
        if not frequencies:
            return None
        least_loaded = list(set(min(frequencies.iteritems())[1]) - set(exclude))
        if not least_loaded:
            return None
        return self._rand.choice(least_loaded)

    def config(self):
        try:
            hosts = {}
            counter = 0
            config = ''
            for spacenum, space in self._spaces_by_num.iteritems():
                spacedims = dict([(d.name, d) for d in space.dimensions])
                space_str = 'space\t{0}\t{1}\t{2}\n'
                space_str = space_str.format(hex(spacenum).rstrip('L'), space.name,
                                             '\t'.join([d.name for d in space.dimensions]))
                for subspacenum, subspace in enumerate(space.subspaces):
                    subdims = dict([(d.name, d) for d in subspace.dimensions])
                    hashes = []
                    for dim in space.dimensions:
                        if dim.name in subdims:
                            repl = subdims[dim.name].replhash or dim.replhash
                            disk = subdims[dim.name].diskhash or dim.diskhash
                        else:
                            repl = dim.replhash
                            disk = dim.diskhash
                        hashes.append((repl, disk))
                    hashes_str = '\t'.join(n + '\t' + h for n, h in hashes)
                    subspace_str = 'subspace\t{0}\t{1}\t{2}\n'
                    subspace_str = subspace_str.format(space.name,
                                                       subspacenum,
                                                       hashes_str)
                    for region in subspace.regions:
                        region_str = 'region\t{0}\t{1}\t{2}\t{3}'
                        region_str = region_str.format(space.name,
                                                       subspacenum,
                                                       region.prefix,
                                                       hex(region.mask).rstrip('L'))
                        for replica in region.replicas:
                            if replica:
                                if replica not in hosts:
                                    hosts[replica] = 'auto_{0}'.format(counter)
                                    counter += 1
                                region_str += '\t{0}'.format(hosts[replica])
                        subspace_str += region_str + '\n'
                    space_str += subspace_str
                config += space_str
            hosts_lines = []
            for host, codename in hosts.iteritems():
                inc_ver, out_ver = self._instances[host]
                host_str = 'host\t{0}\t{1}\t{2}\t{3}\t{4}\t{5}'
                host_str = host_str.format(codename, host[0], host[1], inc_ver,
                                                              host[2], out_ver)
                hosts_lines.append(host_str)
            hosts_lines.sort()
            self._stableconfig = ('\n'.join(hosts_lines) + '\n' + config).strip()
            self._confignum += 1
        except Exception as e:
            # XXX Use the ConCoord logging service.
            logging.error("Unhandled exception {0} in config()".format(e))
        return self.stable_config()

    def stable_config(self):
        return self._stableconfig, self._confignum


class Acceptor(asyncore.dispatcher):

    def __init__(self, actionslog, desc, newinst, bindto, port, map):
        asyncore.dispatcher.__init__(self, map=map)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((bindto, port))
        self.listen(5)
        self._actionslog = actionslog
        self._desc = desc
        self._newinst = newinst

    def handle_accept(self):
        accepted = self.accept()
        if accepted:
            conn, addr = accepted
            self._newinst(self._actionslog, conn, self._map)
            logging.info("{0} connection established from {1}".format(self._desc, addr))


class ControlConnection(asynchat.async_chat):

    def __init__(self, actionslog, sock, map):
        asynchat.async_chat.__init__(self, sock=sock, map=map)
        self.set_terminator("\n")
        self._actionslog = actionslog
        self._ibuffer = []
        self._connectime = datetime.datetime.now()
        self._mode = "COMMAND"
        self._act_on_data = None
        self._id = '%s:%i' % sock.getpeername()

    def handle_close(self):
        try:
            asynchat.async_chat.handle_close(self)
            if self._id:
                logging.info("Control disconnect from {0}".format(self._id))
        except:
            pass

    def handle_error(self):
        '''Provide polite error messages in case of unhandled exception.'''
        nil, t, v, tbinfo = asyncore.compact_traceback()
        error_id = uuid.uuid4()
        logging.error('uncaptured python exception {0} ({1}:{2} {3})'
                      .format(error_id, t, v, tbinfo))
        self.push('Request generated an unhandled exception:  {0}\n'.format(error_id))

    def collect_incoming_data(self, data):
        self._ibuffer.append(data)

    def found_terminator(self):
        if self._mode == "COMMAND":
            commandline = shlex.split("".join(self._ibuffer))
            self._ibuffer = []
            if commandline:
                if commandline[0] == "list" and commandline[1] == "clients":
                    if len(commandline) != 2:
                        return self.fail("Invalid control command {0}".format(commandline))
                    self.list_clients()
                elif commandline[0] == "list" and commandline[1] == "instances":
                    if len(commandline) != 2:
                        return self.fail("Invalid control command {0}".format(commandline))
                    self.list_instances()
                elif commandline[0] == "add" and commandline[1] == "space":
                    if len(commandline) != 2:
                        return self.fail("Invalid control command {0}".format(commandline))
                    self.set_terminator('\n.\n')
                    self._mode = "DATA"
                    self._act_on_data = self.add_space
                elif commandline[0] == "rm" and commandline[1] == "space":
                    if len(commandline) != 3:
                        return self.fail("Invalid control command {0}".format(commandline))
                    self.rm_space(commandline[2])
                elif commandline[0] == "fill" and commandline[1] == "space":
                    if len(commandline) != 3:
                        return self.fail("Invalid control command {0}".format(commandline))
                    self.fill_space(commandline[2])
                else:
                    return self.fail("Unknown commandline {0}".format(commandline))
        elif self._mode == "DATA":
            data = "".join(self._ibuffer)
            self._ibuffer = []
            self._act_on_data(data)
            self.set_terminator('\n')
            self._mode="COMMAND"

    def fail(self, msg):
        logging.warning(msg + " from {0}".format(self._id))
        self.push(msg + '\n')

    def list_clients(self):
        ret = []
        for conn in self._map.values():
            identified = getattr(conn, '_identified', None)
            if identified == HostConnection.CLIENT:
                ret.append((conn._connectime, conn._id))
        resp = "\n".join(["{0} {1}".format(d, i) for d, i in ret])
        if resp:
            resp += "\n.\n"
        else:
            resp += ".\n"
        self.push(resp)

    def list_instances(self):
        local = {}
        instances = self._actionslog.list_instances()
        for conn in self._map.values():
            identified = getattr(conn, '_identified', None)
            if identified == HostConnection.INSTANCE:
                local[conn._instance] = conn._connectime
        resp = ''
        for key, val in sorted(instances.items()):
            time = local.get(key, '')
            resp += "{0:26} {1} {2}v{3} {4}v{5}\n"\
                    .format(str(time), key[0], key[1], val[0], key[2], val[1])
        resp += ".\n"
        self.push(resp)

    def add_space(self, data):
        try:
            parser = (hdcoordinator.parser.space + pyparsing.stringEnd)
            space = parser.parseString(data)[0]
            self._actionslog.add_space(space)
        except ValueError as e:
            return self.fail(str(e))
        except pyparsing.ParseException as e:
            return self.fail(str(e))
        except ActionsLog.DuplicateSpace as e:
            return self.fail("Space already exists")
        config, num = self._actionslog.config()
        for fd, desc in self._map.iteritems():
            if hasattr(desc, 'new_configuration'):
                desc.new_configuration(config, num)
        self.push("SUCCESS\n")

    def rm_space(self, space):
        try:
            self._actionslog.rm_space(space)
        except ValueError as e:
            return self.fail(str(e))
        except ActionsLog.UnknownSpace as e:
            return self.fail("Space does not exist")
        config, num = self._actionslog.config()
        for fd, desc in self._map.iteritems():
            if hasattr(desc, 'new_configuration'):
                desc.new_configuration(config, num)
        self.push("SUCCESS\n")

    def fill_space(self, space):
        try:
            self._actionslog.fill_space(space)
        except ValueError as e:
            return self.fail(str(e))
        except ActionsLog.UnknownSpace as e:
            return self.fail("Space does not exist")
        config, num = self._actionslog.config()
        for fd, desc in self._map.iteritems():
            if hasattr(desc, 'new_configuration'):
                desc.new_configuration(config, num)
        self.push("SUCCESS\n")


class HostConnection(asynchat.async_chat):

    class CLIENT: pass
    class INSTANCE: pass

    def __init__(self, actionslog, sock, map):
        asynchat.async_chat.__init__(self, sock=sock, map=map)
        self.set_terminator("\n")
        self._actionslog = actionslog
        self._ibuffer = []
        self._connectime = datetime.datetime.now()
        self._id = '%s:%i' % sock.getpeername()
        self._identified = None
        self._instance = None
        self._die = False
        config, num = actionslog.stable_config()
        self.new_configuration(config, num)

    def handle_close(self):
        asynchat.async_chat.handle_close(self)
        if self._instance:
            addr, incoming, outgoing = self._instance
            self._actionslog.rm_instance(addr, incoming, outgoing)
            self._instance = None
        if not self._die:
            logging.info("Disconnect from {0}".format(self._id))

    def handle_error(self):
        'This is necessary because of loose exception handling in asyncore.'
        try:
            asynchat.async_chat.handle_error(self)
        except:
            pass

    def collect_incoming_data(self, data):
        self._ibuffer.append(data)

    def found_terminator(self):
        commandline = shlex.split("".join(self._ibuffer))
        self._ibuffer = []
        if commandline:
            if commandline[0] == "client":
                if len(commandline) != 1:
                    return self.die("Invalid client command {0} from {{0}}".format(commandline))
                self.identify_as_client()
            elif commandline[0] == "instance":
                if len(commandline) != 4:
                    return self.die("Invalid instance command {0} from {{0}}".format(commandline))
                self.identify_as_instance(commandline[1], commandline[2], commandline[3])
            elif commandline[0] == "ACK":
                if len(commandline) != 1:
                    return self.die("Invalid ACK command {0} from {{0}}".format(commandline))
                self.handle_config_response(True)
            elif commandline[0] == "BAD":
                if len(commandline) != 1:
                    return self.die("Invalid BAD command {0} from {{0}}".format(commandline))
                self.handle_config_response(False)
            else:
                return self.die("Unknown commandline {0} from {{0}}".format(commandline))

    def die(self, msg):
        logging.error(msg.format(self._id) + ":  dumping connection")
        self.socket.close()
        self._die = True

    def identify_as_client(self):
        if self._identified:
            return self.die("Host {0} is identifying multiple times")
        self._identified = HostConnection.CLIENT
        logging.info("Host {0} identifies as a client.".format(self._id))

    def identify_as_instance(self, addr, incoming, outgoing):
        if self._identified:
            return self.die("Host {0} is identifying multiple times")
        self._identified = HostConnection.INSTANCE
        addr = normalize_address(addr)
        if not addr:
            return self.die("Host {0} identifies as an instance with an unparseable address")
        try:
            incoming = int(incoming)
        except ValueError:
            return self.die("Host {0} identifies as an instance with an unparseable incoming port")
        try:
            outgoing = int(outgoing)
        except ValueError:
            return self.die("Host {0} identifies as an instance with an unparseable incoming port")
        try:
            self._actionslog.add_instance(addr, incoming, outgoing)
            self._instance = (addr, incoming, outgoing)
            logging.info("Host {0} identifies as an instance on {1} {2} {3}"
                         .format(self._id, addr, incoming, outgoing))
        except ActionsLog.HostExists:
            return self.die("Host {{0}} identifies as instance ({0}, {1}, {2}) which is already conected"
                            .format(addr, incoming, outgoing))
        except ActionsLog.ExhaustedPorts:
            return self.die("Host {0} uses a port which has been exhausted")

    def new_configuration(self, config, num):
        # XXX Do something more inteligent.
        self.push((config + '\nend\tof\tline').strip() + '\n')

    def handle_config_response(self, success):
        if not self._identified:
            return self.die("Host {0} is responding to a configuration before identifying")
        if success:
            logging.info("Host {0} acknowledges new configuration".format(self._id))
        else:
            logging.error("Host {0} says new configuration is bad".format(self._id))
        # XXX Do something more inteligent.
        return


class CoordinatorServer(object):

    def __init__(self, bindto, control_port, host_port):
        self._map = {}
        self._actionslog = ActionsLog()
        self._control = Acceptor(self._actionslog, 'Control', ControlConnection, bindto, control_port, self._map)
        logging.info("Control port established on {0}".format(self._control.getsockname()))
        self._host = Acceptor(self._actionslog, 'Host', HostConnection, bindto, host_port, self._map)
        logging.info("Host port established on {0}".format(self._host.getsockname()))

    def run(self):
        try:
            while self._map:
                asyncore.loop(0.1, True, self._map, 1)
        except KeyboardInterrupt:
            logging.critical("KeyboardInterrupt received:  exiting")
            pass


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--bindto', default='')
    parser.add_argument('-c', '--control-port', type=int, default=0)
    parser.add_argument('-p', '--host-port', type=int, default=0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    cs = CoordinatorServer(args.bindto, args.control_port, args.host_port)
    cs.run()
