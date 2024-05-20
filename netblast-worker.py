#!/usr/bin/env python3
import traceback
import signal
import socket
import time
import json
import sys
import os

BLAST_BUFSIZE = 2**15

def sendRequest(manager,request,debug):
    request_json = json.dumps(request)
    if debug:
        sys.stderr.write("Sending request to manager " + manager + ": " + request_json + "\n")

    manager_addr = manager.split(':')
    sock = socket.create_connection(manager_addr)
    sock.sendall(bytes(request_json,"utf-8"))
    sock.shutdown(socket.SHUT_WR)
    response = ""
    while True:
        r = str(sock.recv(1024),"utf-8")
        if len(r)==0: break
        response += r

    if debug:
        sys.stderr.write("Received response: " + response + "\n")

    return json.loads(response)

def spawnBlastReceiver(worker_host,worker_port,debug):
    sock = socket.create_server((worker_host,worker_port))
    sock_addr = sock.getsockname()
    blast_port = socket.getnameinfo(sock_addr,socket.NI_NUMERICHOST | socket.NI_NUMERICSERV)[1]
    if debug:
        sys.stderr.write("Blast receiver listening on port " + str(blast_port) + "\n")

    blast_pid = os.fork()
    if blast_pid > 0:
        if debug:
            sys.stderr.write("Blast receiver spawned with pid " + str(blast_pid) + "\n")
        return (blast_port,blast_pid)

    while True:
        (client,client_addr) = sock.accept()
        if debug:
            sys.stderr.write("Blast receiver got connection from " + repr(client_addr) + "\n")

        buf = bytearray(BLAST_BUFSIZE)
        byte_count = 0
        start_time = time.time()
        while True:
            b = client.recv_into(buf)
            if b==0: break
            byte_count += b

        client.shutdown(socket.SHUT_RDWR)
        client.close()
        end_time = time.time()
        elapsed = end_time - start_time
        print("Received",byte_count,"bytes in",elapsed,"seconds")

def blastem(manager,worker_id,blast_ip,blast_port,duration,debug):
    if debug:
        sys.stderr.write("Blasting " + blast_ip + ":" + str(blast_port) + "\n")

    buf = bytearray(BLAST_BUFSIZE)
    for i in range(0,BLAST_BUFSIZE):
        buf[i] = i % 256

    sock = socket.create_connection((blast_ip,blast_port))
    started = time.time()
    bytes_sent = 0
    while time.time() - started < duration:
        sock.sendall(buf)
        bytes_sent += len(buf)

    sock.shutdown(socket.SHUT_RDWR)
    sock.close()
    elapsed = time.time() - started

    req = {}
    req['q'] = 'report_flow'
    req['worker_id'] = worker_id
    req['blast_ip'] = blast_ip
    req['blast_port'] = blast_port
    req['start'] = int(round(started))
    req['duration'] = round(elapsed,2)
    req['bytes'] = bytes_sent
    sendRequest(manager,req,debug)

    print("Sent",bytes_sent,"bytes in",elapsed,"seconds")

def runNetBlastWorker(manager,worker_host,worker_port,debug,worker_duration):
    worker_started = time.time()
    (blast_port,blast_pid) = spawnBlastReceiver(worker_host,worker_port,debug)

    req = {}
    req['q'] = 'register_worker'
    req['blast_port'] = blast_port
    res = sendRequest(manager,req,debug)
    worker_id = res['worker_id']

    while not worker_duration or time.time() - worker_started < worker_duration:
        req = {}
        req['q'] = 'get_work'
        req['worker_id'] = worker_id
        res = sendRequest(manager,req,debug)
        if not res['success']:
            if res['error_msg']:
                sys.stderr.write("Received message from manager: " + res['error_msg'] + "\n")
            if 'retry_after' in res:
                time.sleep(res['retry_after'])
                continue
            break

        try:
            blastem(manager,worker_id,res['blast_ip'],res['blast_port'],res['duration'],debug)
        except Exception as error:
            print("Error blasting " + res['blast_ip'] + ":" + res['blast_port'] + ":",error)
            print(traceback.format_exc())

    os.kill(blast_pid,signal.SIGTERM)
    print("Shutting worker down after",time.time()-worker_started,"seconds")

def daemonize():
    if os.fork():
        sys.exit(0)
    os.setsid()
    if os.fork():
        sys.exit(0)
    os.chdir("/")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Connect to a netblast manager and do assigned tasks.')
    parser.add_argument('--manager', metavar='HOSTNAME:PORT', required=True, help='address of netblast manager')
    parser.add_argument('--worker-port', default=0, type=int, help='network port to listen on (default use random port)')
    parser.add_argument('--worker-host', default="", help='IP/hostname to bind to (default all interfaces)')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--duration', type=int, help='Stop the worker after this many seconds.')
    parser.add_argument('--daemonize',action='store_true')

    args = parser.parse_args()

    if args.daemonize:
        daemonize()

    runNetBlastWorker(args.manager,args.worker_host,args.worker_port,args.debug,args.duration)
