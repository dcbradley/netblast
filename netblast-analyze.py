#!/usr/bin/env python3
import csv
import ipaddress

def ipMatches(ip,patterns):
    for pattern in patterns:
        if pattern == ip: return True
        if ipaddress.ip_address(ip) in ipaddress.ip_network(pattern): return True
    return False

def netflowMatches(src_ip,dest_ip,src_patterns,dest_patterns):
    if not (src_patterns is None) and len(src_patterns)>0 and not ipMatches(src_ip,src_patterns):
        return False
    if not (dest_patterns is None) and len(dest_patterns)>0 and not ipMatches(dest_ip,dest_patterns):
        return False
    return True

def analyzeNetBlastLog(logfile,outputcsv,src,dest,debug):
    F = open(logfile,"r")
    records = []
    for line in F:
        if not line.startswith("FLOW: "): continue
        parts = line.split()
        rec = {}
        rec['src_ip'] = parts[1]
        rec['dest_ip'] = parts[2]
        rec['dest_port'] = parts[3]
        rec['start_time'] = float(parts[4])
        rec['elapsed'] = float(parts[5])
        rec['end_time'] = rec['start_time'] + rec['elapsed']
        rec['bytes_sent'] = int(parts[6])

        if not netflowMatches(rec['src_ip'],rec['dest_ip'],src,dest):
            if debug:
                print("UNMATCHED: ",repr(rec))
            continue

        if debug:
            print("MATCHED: ",repr(rec))

        records.append(rec)

    min_time = None
    max_time = None
    for rec in records:
        if min_time is None or min_time > rec['start_time']:
            min_time = rec['start_time']
        if max_time is None or max_time < rec['end_time']:
            max_time = rec['end_time']
    if min_time is None:
        min_time = 0
        max_time = 0

    OF = open(outputcsv,"w")
    csvout = csv.writer(OF)

    csvout.writerow(["t","bps","bytes","duration"])

    dt = 30
    for t in range(int(min_time),int(max_time),dt):
        bytes_sent = 0
        for rec in records:
            if rec['start_time'] <= t and rec['end_time'] > t:
                delta = dt
                if t + delta > rec['end_time']:
                    delta = rec['end_time'] - t
                bytes_sent += rec['bytes_sent']/(1.0*rec['elapsed'])*dt
        flow = bytes_sent/dt*8

        csvout.writerow([round(t-min_time),round(flow),round(bytes_sent),dt])

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Analyze the log of a netblast manager to summarize network flows.')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--src',action='append',help='filter by IP address of source')
    parser.add_argument('--dest',action='append',help='filter by IP address of destination')
    parser.add_argument('logfile')
    parser.add_argument('outputcsv')

    args = parser.parse_args()
    analyzeNetBlastLog(args.logfile,args.outputcsv,args.src,args.dest,args.debug)
