#!/usr/bin/env python

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


import json
import socket
import sys

import argparse
import pyparsing

import hypercoordinator.parser


def send_msg(host, port, msg, args=''):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
    s.connect((host, port))
    s.sendall(json.dumps({msg:args}) + '\n')
    data = ""
    while '\n' not in data:
        d = s.recv(4096)
        if len(d) == 0:
            sys.stderr.write("Protocol error: server closed connection unexpectedly after sending {0} \n".format(data))
            return 1
        data += d
    try:
        resp = json.loads(data)
    except ValueError as e:
        sys.stderr.write("Protocol error: got non-JSON message {0}\n".format(data))
        return 1
    if type(resp) != dict:
        sys.stderr.write("Protocol error: got invalid JSON message {0}\n".format(data))
        return 1
    if not resp.has_key(msg):
        sys.stderr.write("Protocol error: server message {0} does not contain response to {1}\n".format(data, msg))
        return 1
    rv = resp[msg]
    if rv == "ERROR":
        e = resp['error'] if resp.has_key('error') else ''
        sys.stderr.write("Server responded with failure: {0}\n".format(e))
        return 1
    if rv != "SUCCESS":
        sys.stdout.write(rv+'\n')
    return 0

     
def add_space(args):
    data = sys.stdin.read()
    try:
        parser = (hypercoordinator.parser.space + pyparsing.stringEnd)
        space = parser.parseString(data)[0]
    except ValueError as e:
        print str(e)
        return 1
    except pyparsing.ParseException as e:
        print str(e)
        return 1
    return send_msg(args.host, args.port, 'add-space', data)


def del_space(args):
    return send_msg(args.host, args.port, 'del-space', args.space)


def lst_spaces(args):
    return send_msg(args.host, args.port, 'lst-spaces')


def get_space(args):
    return send_msg(args.host, args.port, 'get-space', args.space)


def quiesce(args):
    return send_msg(args.host, args.port, 'quiesce')

    
def shutdown(args):
    return send_msg(args.host, args.port, 'shutdown')

	
def get_status(args):
    return send_msg(args.host, args.port, 'get-status')


def go_live(args):
    return send_msg(args.host, args.port, 'go-live')


def backup_state(args):
    return send_msg(args.host, args.port, 'backup-state')

    
def validate_space(args):
    data = sys.stdin.read()
    try:
        parser = (hypercoordinator.parser.space + pyparsing.stringEnd)
        space = parser.parseString(data)[0]
    except ValueError as e:
        print str(e)
        return 1
    except pyparsing.ParseException as e:
        print str(e)
        return 1
    return 0


def main(args, name='hyperdex-control'):
    parser = argparse.ArgumentParser(prog=name)
    parser.add_argument('--host', metavar='COORDHOST',
                        help='Address of the coordiantor',
                        default='127.0.0.1')
    parser.add_argument('--port', metavar='COORDPORT', type=int,
                        help='Port for coordinator control connections',
                        default=6970)
    subparsers = parser.add_subparsers(help='sub-command help')
    parser_add_space = subparsers.add_parser('add-space', help='add-space help')
    parser_add_space.set_defaults(func=add_space)
    parser_del_space = subparsers.add_parser('del-space', help='del-space help')
    parser_del_space.add_argument('space', metavar='SPACE', help='the space to delete')
    parser_del_space.set_defaults(func=del_space)
    parser_validate_space = subparsers.add_parser('validate-space', help='validate help')
    parser_validate_space.set_defaults(func=validate_space)
    parser_lst_spaces = subparsers.add_parser('lst-spaces', help='lst-space help')
    parser_lst_spaces.set_defaults(func=lst_spaces)
    parser_get_space = subparsers.add_parser('get-space', help='get-space help')
    parser_get_space.add_argument('space', metavar='SPACE', help='the space to get')
    parser_get_space.set_defaults(func=get_space)
    parser_shutdown = subparsers.add_parser('quiesce', help='quiesce help')
    parser_shutdown.set_defaults(func=quiesce)
    parser_shutdown = subparsers.add_parser('shutdown', help='shutdown help')
    parser_shutdown.set_defaults(func=shutdown)
    parser_get_status = subparsers.add_parser('get-status', help='get-stataus help')
    parser_get_status.set_defaults(func=get_status)
    parser_go_live = subparsers.add_parser('go-live', help='get-stataus help')
    parser_go_live.set_defaults(func=go_live)
    parser_go_live = subparsers.add_parser('backup-state', help='backup-state help')
    parser_go_live.set_defaults(func=backup_state)
    args = parser.parse_args(args)
    return args.func(args)


if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
