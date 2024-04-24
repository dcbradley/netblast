#!/usr/bin/env python3
import socket
import socketserver
import json
import traceback
import random
import time

KEEPALIVE_TIMEOUT = 120
RETRY_INTERVAL = 10
BLAST_CLIENT_DURATION = 60
TEST_DURATION = 120

class NetBlastHandler(socketserver.BaseRequestHandler):

    def handle(self):
        try:
            data = ""
            while True:
                more_data = str(self.request.recv(1024),"utf-8")
                if len(more_data)==0: break
                data += more_data
            if self.server.debug:
                print("Received from {}:".format(self.client_address[0]))
                print(data)

            req = json.loads(data)

            if not ('ip' in req):
                req['ip'] = self.client_address[0]

            q = req['q']
            if q == 'get_work':
                res = self.server.getWork(self,req)
            elif q == 'register_worker':
                res = self.server.registerWorker(self,req)
            elif q == 'keep_alive':
                res = self.server.keepalive(self,req)
            elif q == 'report_flow':
                res = self.server.reportFlow(self,req)
            else:
                print("Unknown command from {}: {}".format(self.client_address[0],data))
                res = {'success': False, 'message': "Unknown command '" + q + "'"}

            self.request.sendall(bytes(json.dumps(res),"utf-8"))

        except Exception as error:
            print("Error handling request from " + self.client_address[0] + ":",error)
            print("Request string from " + self.client_address[0] + " was: " + repr(data))
            print(traceback.format_exc())

class NetBlastServer(socketserver.TCPServer):
    allow_reuse_address = True
    workers = None
    ids = None
    debug = None
    test_duration = None
    test_started = None

    def __init__(self,addr,handler):
        super().__init__(addr,handler)
        self.test_started = time.time()
        self.workers = {}
        self.ids = set()

    def getNewWorkerID(self):
        while True:
            id = ""
            for i in range(0,8):
                id += "%x" % (random.randint(0,65535))
            if not (id in self.ids):
                self.ids.add(id)
                return id

    def registerWorker(self,handler,req):
        worker_id = self.getNewWorkerID()

        worker = {}
        worker['worker_id'] = worker_id
        worker['ip'] = req['ip']
        if 'blast_port' in req:
            worker['blast_port'] = req['blast_port']
        else:
            worker['blast_port'] = 0
        worker['blast_client'] = None
        worker['last_contact'] = time.time()

        self.workers[worker_id] = worker

        res = {}
        res['success'] = True
        res['worker_id'] = worker_id
        return res

    def keepalive(self,handler,req):
        self.workers[req['worker_id']]['last_contact'] = time.time()

    def getWork(self,handler,req):
        self.keepalive(handler,req)
        req_ip = req['ip']

        # unlink this client from any previous job it may have been doing
        for worker_id,worker in self.workers.items():
            if worker['blast_client'] == req['worker_id']:
                worker['blast_client'] = None
            break

        now = time.time()
        blast_server = None
        for worker_id,worker in self.workers.items():
            worker_ip = worker['ip']
            if worker['blast_client'] and now - self.workers[worker['blast_client']]['last_contact'] < KEEPALIVE_TIMEOUT: continue
            if worker_ip == req_ip: continue
            if not worker['blast_port']: continue
            if now - worker['last_contact'] > KEEPALIVE_TIMEOUT: continue
            blast_server = worker
            break

        res = {}
        if not blast_server:
            res['success'] = False
            if now - self.test_started + RETRY_INTERVAL < self.test_duration:
                res['retry_after'] = RETRY_INTERVAL
                res['error_msg'] = 'No servers found.  Retry in ' + str(res['retry_after']) + ' seconds.'
            else:
                res['error_msg'] = 'Test ended.'
        else:
            blast_server['blast_client'] = req['worker_id']
            res['success'] = True
            res['blast_ip'] = blast_server['ip']
            res['blast_port'] = blast_server['blast_port']
            res['duration'] = BLAST_CLIENT_DURATION
            if now - self.test_started + BLAST_CLIENT_DURATION > self.test_duration:
                res['duration'] = self.test_duration - (now - self.test_started)
                if res['duration'] <= 0:
                    res['success'] = False
                    res['error_msg'] = 'Test ended.'

        return res

    def reportFlow(self,handler,req):
        self.keepalive(handler,req)
        print('FLOW:',req['ip'],req['blast_ip'],req['blast_port'],req['start'],req['duration'],req['bytes'])

def whatsMyIP():
    try:
        # try to discover our IP address by binding a UDP socket to a public address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except:
        pass

def runNetBlastManager(host,port,debug,test_duration):
    with NetBlastServer((host, port), NetBlastHandler) as server:
        server.debug = debug
        server.test_duration = test_duration

        if not host: host = str(server.server_address[0])
        port = str(server.server_address[1])
        if host == "0.0.0.0":
            my_ip = whatsMyIP()
            if my_ip: host = my_ip
        print("Manager network address:",host + ":" + port)

        server.serve_forever()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Manage a swarm of netblast workers.')
    parser.add_argument('--port', default=0, type=int, help='network port to use (default use random port)')
    parser.add_argument('--host', default="", help='IP/hostname to bind to (default all interfaces)')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--duration', default=TEST_DURATION, type=int, help='Stop the test after this many seconds.')

    args = parser.parse_args()
    runNetBlastManager(args.host,args.port,args.debug,args.duration)
