#!/usr/bin/env python3
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Error
import sys
import os
import _thread
import time


class Worker:
    def __init__(self, addr, jt_addr):
        self.addr = addr
        self.jt = ServerProxy(jt_addr)
        self.jt_addr = jt_addr
        self.hb_timeout = 0.5  # heartbeat timeout in seconds
        self.on = True

    def start(self):
        print('Init worker')
        print('Start sending heartbeats to', self.jt_addr)
        _thread.start_new_thread(self._heartbeat, ())
        print('Server is ready')

    def _heartbeat(self):
        while self.on:
            try:
                self.jt.heartbeat(self.addr)
            except Exception as e:
                pass
            time.sleep(self.hb_timeout)


if __name__ == '__main__':
    if len(sys.argv) == 3:
        host = sys.argv[1]
        port = int(sys.argv[2])
    else:
        host = 'localhost'
        port = 9999

    addr = 'http://' + host + ":" + str(port)
    if not os.getenv('YAMR_JT'):
        ns_addr = 'http://localhost:11111'
    else:
        ns_addr = os.environ['YAMR_JT']

    worker = Worker(addr, ns_addr)
    worker.start()

    server = SimpleXMLRPCServer((host, port))
    server.register_introspection_functions()
    server.register_instance(worker)
    server.serve_forever()