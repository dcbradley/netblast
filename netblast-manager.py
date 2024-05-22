#!/usr/bin/env python3
import socket
import socketserver
import json
import traceback
import random
import time
import ipaddress
import sys
import threading
import signal

KEEPALIVE_TIMEOUT = 120
RETRY_INTERVAL = 10
BLAST_CLIENT_DURATION = 60
TEST_DURATION = 120
MAX_CONNECT_ERRORS = 3

class NetBlastHandler(socketserver.BaseRequestHandler):

    def handle(self):
        try:
            data = ""
            while True:
                more_data = str(self.request.recv(1024),"utf-8")
                if len(more_data)==0: break
                data += more_data
            if self.server.debug:
                print("Received from {}: {}".format(self.client_address[0],data))

            req = json.loads(data)

            if not ('ip' in req):
                req['ip'] = self.client_address[0]

            q = req['q']
            if 'worker_id' in req and req['worker_id'] not in self.server.workers:
                res = {}
                res['success'] = False
                res['reregister'] = True
                res['retry_after'] = 1
                res['error_msg'] = "Worker ID " + str(req['worker_id']) + " not found.  Reregister."
            elif q == 'get_work':
                res = self.server.getWork(self,req)
            elif q == 'register_worker':
                res = self.server.registerWorker(self,req)
            elif q == 'keep_alive':
                res = self.server.keepalive(self,req)
            elif q == 'report_flow':
                res = self.server.reportFlow(self,req)
            elif q == 'connect_failed':
                res = self.server.reportConnectFailed(self,req)
            else:
                print("Unknown command from {}: {}".format(self.client_address[0],data))
                res = {'success': False, 'message': "Unknown command '" + q + "'"}

            if self.server.debug:
                print("Response to {}: {}".format(self.client_address[0],res))
                sys.stdout.flush()

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
    client_networks = None
    server_networks = None
    shutting_down = False

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
        worker['connect_errors'] = 0
        worker['blast_client'] = None
        worker['last_contact'] = time.time()

        worker['in_client_networks'] = ipMatches(worker['ip'],self.client_networks,self.server_networks)
        worker['in_server_networks'] = ipMatches(worker['ip'],self.server_networks,self.client_networks)
        if not worker['in_client_networks'] and not worker['in_server_networks']:
            print("Warning: worker with IP",worker['ip'],"is not in client or server networks, so it will not participate.")

        self.workers[worker_id] = worker

        if self.debug:
            print("Registered worker: " + repr(worker))

        res = {}
        res['success'] = True
        res['worker_id'] = worker_id
        return res

    def keepalive(self,handler,req):
        self.workers[req['worker_id']]['last_contact'] = time.time()

    def getWork(self,handler,req):
        self.keepalive(handler,req)
        client_worker = self.workers[req['worker_id']]
        req_ip = req['ip']
        res = {}
        now = time.time()

        # unlink this client from any previous job it may have been doing
        for worker_id,server_worker in self.workers.items():
            if server_worker['blast_client'] == req['worker_id']:
                server_worker['blast_client'] = None

        if not client_worker['in_client_networks']:
            res['success'] = False
            if now - self.test_started < self.test_duration:
                res['retry_after'] = now - self.test_started
                if res['retry_after'] > KEEPALIVE_TIMEOUT/2:
                    res['retry_after'] = KEEPALIVE_TIMEOUT/2
                if client_worker['in_server_networks']:
                    res['error_msg'] = 'You will only be a server.'
                else:
                    res['error_msg'] = 'You are not in client or server networks, so you will do nothing.'
                res['error_msg'] += '  Check in again in ' + str(round(res['retry_after'],1)) + ' seconds.'
            else:
                res['error_msg'] = 'Test ended.'
            return res

        blast_server = None
        for server_worker_id,server_worker in self.workers.items():
            server_worker_ip = server_worker['ip']
            if not server_worker['in_server_networks']: continue
            if server_worker['blast_client'] and now - self.workers[server_worker['blast_client']]['last_contact'] < KEEPALIVE_TIMEOUT: continue
            if server_worker_ip == req_ip: continue
            if not server_worker['blast_port']: continue
            # avoid simultaneously acting as both a server and client to the same peer
            if client_worker['blast_client'] and client_worker['blast_client'] == server_worker['worker_id']: continue
            if now - server_worker['last_contact'] > KEEPALIVE_TIMEOUT: continue
            if server_worker['connect_errors'] > MAX_CONNECT_ERRORS: continue
            blast_server = server_worker
            break

        if not blast_server:
            res['success'] = False
            if now - self.test_started < self.test_duration:
                res['retry_after'] = RETRY_INTERVAL
                if now - self.test_started + res['retry_after'] > self.test_duration:
                    res['retry_after'] = self.test_duration - (now - self.test_started)
                res['error_msg'] = 'No servers found.  Retry in ' + str(round(res['retry_after'],1)) + ' seconds.'
            else:
                res['error_msg'] = 'Test ended.'
        else:
            blast_server['blast_client'] = req['worker_id']
            res['success'] = True
            res['blast_ip'] = blast_server['ip']
            res['blast_port'] = blast_server['blast_port']
            res['blast_id'] = blast_server['worker_id']
            res['direction'] = self.direction
            res['duration'] = BLAST_CLIENT_DURATION
            if now - self.test_started + BLAST_CLIENT_DURATION > self.test_duration:
                res['duration'] = self.test_duration - (now - self.test_started)
                if res['duration'] < 1:
                    res['success'] = False
                    res['error_msg'] = 'Test ended.'

        return res

    def reportFlow(self,handler,req):
        self.keepalive(handler,req)

        if req['bytes_sent']:
            print('FLOW:',req['ip'],req['blast_ip'],req['blast_port'],req['start'],req['duration'],req['bytes_sent'])
        if req['bytes_received']:
            print('FLOW:',req['blast_ip'],req['ip'],req['blast_port'],req['start'],req['duration'],req['bytes_received'])

        sys.stdout.flush()

    def reportConnectFailed(self,handler,req):
        server = self.workers[req['blast_id']]
        server['connect_errors'] += 1
        if server['connect_errors'] == MAX_CONNECT_ERRORS+1:
            print("Will no longer use failing server at ",server['ip'] + ":" + str(server['blast_port']),": ",req['error'])

    def stopSignal(self,signum,frame):
        self.shutting_down = True
        sys.stderr.write("Received interrupt.  Shutting down.\n")


def whatsMyIP():
    try:
        # try to discover our IP address by binding a UDP socket to a public address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except:
        pass

def considerShutdown(server):
    while not server.shutting_down and (server.test_duration == 0 or time.time() - server.test_started < server.test_duration + 5):
        s = server.test_duration - (time.time() - server.test_started)
        if s < 1: s = 5
        if s > 5: s = 5
        time.sleep(s)
    print("Test ended after " + str(round(time.time() - server.test_started)) + " seconds.")
    sys.stdout.flush()
    server.shutdown()

def runNetBlastManager(host,port,debug,test_duration,client_networks,server_networks,direction):
    server = NetBlastServer((host, port), NetBlastHandler)
    server.debug = debug
    server.test_duration = test_duration
    server.client_networks = client_networks
    server.server_networks = server_networks
    server.direction = direction

    if not host: host = str(server.server_address[0])
    port = str(server.server_address[1])
    if host == "0.0.0.0":
        my_ip = whatsMyIP()
        if my_ip: host = my_ip
    print("Manager network address:",host + ":" + port)
    sys.stdout.flush()

    ender = threading.Thread(target=considerShutdown, args=(server,))
    ender.start()

    signal.signal(signal.SIGINT, server.stopSignal)
    signal.signal(signal.SIGTERM, server.stopSignal)

    server.serve_forever()
    ender.join()

def ipMatches(ip,patterns,other_patterns):
    if patterns is None or len(patterns)==0:
        if other_patterns is None or len(other_patterns)==0: return True
        for other_pattern in other_patterns:
            if other_pattern == ip: return False
            if ipaddress.ip_address(ip) in ipaddress.ip_network(other_pattern): return False
        return True

    for pattern in patterns:
        if pattern == ip: return True
        if ipaddress.ip_address(ip) in ipaddress.ip_network(pattern): return True
    return False

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Manage a swarm of netblast workers.')
    parser.add_argument('--port', default=0, type=int, help='network port to use (default use random port)')
    parser.add_argument('--host', default="", help='IP/hostname to bind to (default all interfaces)')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--duration', default=TEST_DURATION, type=int, help='Stop the test after this many seconds.')
    parser.add_argument('--clients',action='append',help='Network(s) that should send data. (May use option multiple times.)')
    parser.add_argument('--servers',action='append',help='Network(s) that should receive data. (May use option multiple times.)')
    parser.add_argument('--direction',default='s',choices=['s','r','b'],help="Direction of flow from client to server: (s)end, (r)eceive, (b)oth.")

    args = parser.parse_args()
    runNetBlastManager(args.host,args.port,args.debug,args.duration,args.clients,args.servers,args.direction)
