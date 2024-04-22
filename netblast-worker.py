import socket
import json
import sys

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

def runNetBlastWorker(manager,worker_host,worker_port,debug,worker_duration):
    req = {}
    req['q'] = 'register_worker'
    res = sendRequest(manager,req,debug)
    worker_id = res['worker_id']

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Connect to a netblast manager and do assigned tasks.')
    parser.add_argument('--manager', metavar='HOSTNAME:PORT', required=True, help='address of netblast manager')
    parser.add_argument('--worker-port', default=0, type=int, help='network port to listen on (default use random port)')
    parser.add_argument('--worker-host', default="", help='IP/hostname to bind to (default all interfaces)')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--duration', type=int, help='Stop the worker after this many seconds.')

    args = parser.parse_args()
    runNetBlastWorker(args.manager,args.worker_host,args.worker_port,args.debug,args.duration)
