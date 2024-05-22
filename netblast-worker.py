#!/usr/bin/env python3
import traceback
import threading
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

stop_blast_server = False
def stopBlastServer(signum,frame):
    global stop_blast_server
    stop_blast_server = True

def spawnBlastServer(worker_host,worker_port,debug):
    sock = socket.create_server((worker_host,worker_port))
    sock.settimeout(5)
    sock_addr = sock.getsockname()
    blast_port = socket.getnameinfo(sock_addr,socket.NI_NUMERICHOST | socket.NI_NUMERICSERV)[1]
    if debug:
        sys.stderr.write("NetBlast server listening on port " + str(blast_port) + "\n")

    blast_pid = os.fork()
    if blast_pid > 0:
        if debug:
            sys.stderr.write("NetBlast server spawned with pid " + str(blast_pid) + "\n")
        return (blast_port,blast_pid)

    signal.signal(signal.SIGTERM, stopBlastServer)

    while not stop_blast_server:
        try:
            client = sock.accept()
        except socket.timeout:
            continue
        if not client: continue
        (client_sock,client_addr) = client
        if debug:
            sys.stderr.write("NetBlast server received connection from " + repr(client_addr) + "\n")

        blastServerProtocol(client_sock,client_addr)
    sys.exit(0)

def receiveLoop(sock,duration,stats):
    started = time.time()
    buf = bytearray(BLAST_BUFSIZE)

    while duration == 0 or time.time() - started < duration:
        b = sock.recv_into(buf)
        if b==0: break
        stats['bytes_received'] += b
    sock.shutdown(socket.SHUT_RD)

def sendLoop(sock,duration,stats):
    started = time.time()
    buf = bytearray(BLAST_BUFSIZE)
    for i in range(0,BLAST_BUFSIZE):
        buf[i] = i % 256

    while duration == 0 or time.time() - started < duration:
        sock.sendall(buf)
        stats['bytes_sent'] += len(buf)
    sock.shutdown(socket.SHUT_WR)

def blastServerProtocol(sock,sock_addr):
    buf = bytearray(20)

    b = sock.recv_into(buf,1)
    direction = chr(buf[0])

    b = sock.recv_into(buf,20)
    duration = int(buf.decode())

    peer_addr = str(sock_addr[0]) + ":" + str(sock_addr[1])
    print("NetBlast server will",directionDesc(direction),peer_addr,"for",round(duration),"seconds.")
    sys.stdout.flush()

    stats = {}
    stats['bytes_sent'] = 0
    stats['bytes_received'] = 0

    start_time = time.time()
    send_thread = receive_thread = None
    if direction == 's' or direction == 'b':
        send_thread = threading.Thread(target=sendLoop,args=(sock,duration,stats))
        send_thread.start()
    if direction == 'r' or direction == 'b':
        receive_thread = threading.Thread(target=receiveLoop,args=(sock,0,stats))
        receive_thread.start()
    if send_thread:
        send_thread.join()
    if receive_thread:
        receive_thread.join()

    sock.close()

    end_time = time.time()
    elapsed = end_time - start_time
    if stats['bytes_sent']:
        print("NetBlast server sent",stats['bytes_sent'],"bytes to",peer_addr,"in",round(elapsed),"seconds")
    if stats['bytes_received']:
        print("NetBlast server received",stats['bytes_received'],"bytes from",peer_addr,"in",round(elapsed),"seconds")
    sys.stdout.flush()

def directionDesc(d):
    if d == "r": return "receive from"
    if d == "s": return "send to"
    if d == "b": return "send and receive to/from"
    return d

def blastClientProtocol(manager,worker_id,blast_ip,blast_port,blast_id,duration,direction,debug):
    peer_addr = blast_ip + ":" + str(blast_port)
    if debug:
        sys.stderr.write("NetBlast client connecting to " + peer_addr + "\n")

    try:
        sock = socket.create_connection((blast_ip,blast_port))
    except Exception as error:
        print("Failed to connect to " + peer_addr + ": " + str(error))
        req = {}
        req['q'] = 'connect_failed'
        req['blast_ip'] = blast_ip
        req['blast_port'] = blast_port
        req['blast_id'] = blast_id
        req['error'] = str(error)
        sendRequest(manager,req,debug)
        return

    print("NetBlast client will",directionDesc(direction),peer_addr,"for",round(duration),"seconds.")
    sys.stdout.flush()

    other_direction = ""
    if direction == 's':
        other_direction = 'r'
    if direction == 'r':
        other_direction = 's'
    if direction == 'b':
        other_direction = 'b'
    if other_direction == '':
        raise ValueError("Unexpected direction: " + direction)

    sock.sendall(other_direction.encode("utf-8"))

    duration = int(duration)
    if duration == 0: duration = 1
    sock.sendall(("% 20s" % (duration)).encode("utf-8"))

    stats = {}
    stats['bytes_sent'] = 0
    stats['bytes_received'] = 0
    started = time.time()

    send_thread = receive_thread = None
    if direction == 's' or direction == 'b':
        send_thread = threading.Thread(target=sendLoop,args=(sock,duration,stats))
        send_thread.start()
    if direction == 'r' or direction == 'b':
        receive_thread = threading.Thread(target=receiveLoop,args=(sock,0,stats))
        receive_thread.start()
    if send_thread:
        send_thread.join()
    if receive_thread:
        receive_thread.join()

    sock.shutdown(socket.SHUT_RDWR)
    sock.close()
    elapsed = time.time() - started

    req = stats
    req['q'] = 'report_flow'
    req['worker_id'] = worker_id
    req['blast_ip'] = blast_ip
    req['blast_port'] = blast_port
    req['start'] = int(round(started))
    req['duration'] = round(elapsed,2)
    req['direction'] = direction
    sendRequest(manager,req,debug)

    if stats['bytes_sent']:
        print("NetBlast client sent",stats['bytes_sent'],"bytes to",peer_addr,"in",round(elapsed),"seconds")
    if stats['bytes_received']:
        print("NetBlast client received",stats['bytes_received'],"bytes from",peer_addr,"in",round(elapsed),"seconds")
    sys.stdout.flush()

def runNetBlastWorker(manager,worker_host,worker_port,debug,worker_duration):
    worker_started = time.time()
    (blast_port,blast_pid) = spawnBlastServer(worker_host,worker_port,debug)

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
            blastClientProtocol(manager,worker_id,res['blast_ip'],res['blast_port'],res['blast_id'],res['duration'],res['direction'],debug)
        except Exception as error:
            print("Error blasting " + res['blast_ip'] + ":" + res['blast_port'] + ":",error)
            print(traceback.format_exc())

    os.kill(blast_pid,signal.SIGTERM)
    os.waitpid(blast_pid,0)

    print("Shutting worker down after",round(time.time()-worker_started),"seconds")

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
