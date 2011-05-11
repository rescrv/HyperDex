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


import asyncore
import socket
import threading


class server(asyncore.dispatcher):

    def __init__(self, lock, address, port, clients):
        asyncore.dispatcher.__init__(self)
        self.lock = lock
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((address, port))
        self.listen(8)
        self.clients = clients

    def handle_accept(self):
        print 'accepting'
        pair = self.accept()
        if pair is None:
            return
        sock, addr = pair
        print 'XXX connection from %s' % repr(addr)
        client(self.lock, sock, addr, self.clients)


class client(asyncore.dispatcher_with_send):

    def __init__(self, lock, sock, addr, clients):
        asyncore.dispatcher_with_send.__init__(self, sock)
        self.lock = lock
        self.clients = clients
        self.address = addr
        self.data = ''
        with self.lock:
            self.clients.add(self)

    def handle_close(self):
        print 'XXX closing %s' % repr(self.address)
        self.close()
        with self.lock:
            self.clients.remove(self)
        del self

    def handle_read(self):
        self.data += self.recv(1024)
        while '\n' in self.data:
            data, self.data = self.data.split('\n', 1)
            print 'XXX data from %s: %s' % (repr(self.address), data)

    def send(self, data):
        asyncore.dispatcher_with_send.send(self, data)


class asyncthread(threading.Thread):

    def __init__(self, lock, address, port, clients):
        threading.Thread.__init__(self)
        self.lock = lock
        self.address = address
        self.port = port
        self.clients = clients

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        sock.bind((self.address, self.port))
        sock.listen(1)
        while True:
            pair = sock.accept()
            if not pair:
                continue
            print 'XXX controller from %s' % repr(pair[1])
            controller = pair[0]
            data = ''
            while True:
                newdata = controller.recv(8192)
                if newdata:
                    data += newdata
                else:
                    break
            with self.lock:
                for client in self.clients:
                    client.send(data)
            controller.shutdown(socket.SHUT_RDWR)
            controller.close()


def main(address, client, control):
    clients = set()
    lock = threading.RLock()
    s = server(lock, address, client, clients)
    thread = asyncthread(lock, address, control, clients);
    thread.start()
    while True:
        with thread.lock:
            asyncore.loop(1, count=1)


if __name__ == '__main__':
    main('127.0.0.1', 6970, 6971);
