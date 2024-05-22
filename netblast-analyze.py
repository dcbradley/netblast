#!/usr/bin/env python3
import csv
import ipaddress

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

def netflowMatches(src_ip,dest_ip,src_patterns,dest_patterns):
    if not ipMatches(src_ip,src_patterns,dest_patterns):
        return False
    if not ipMatches(dest_ip,dest_patterns,src_patterns):
        return False
    return True

def analyzeNetBlastLog(logfile,outputcsv,src,dest,dt,debug):
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

    csvout.writerow(["t","duration","bps","bytes","tx_IPs","txrx_IPs",])

    for t in range(int(min_time),int(max_time),dt):
        bytes_sent = 0
        src_ips = {}
        dest_ips = {}
        for rec in records:
            if rec['start_time'] < t+dt and rec['end_time'] > t:
                delta = min(rec['end_time'],t+dt) - max(rec['start_time'],t)
                bytes_sent += rec['bytes_sent']/(1.0*rec['elapsed'])*delta

                if not rec['src_ip'] in src_ips:
                    src_ips[rec['src_ip']] = 0
                src_ips[rec['src_ip']] += delta

                if not rec['dest_ip'] in dest_ips:
                    dest_ips[rec['dest_ip']] = 0
                dest_ips[rec['dest_ip']] += delta

        flow = bytes_sent/dt*8

        num_src_ips = 0
        num_src_and_dest_ips = 0
        for src_ip in src_ips:
            if src_ips[src_ip] > dt:
                # if there are multiple workers on the same computer, only count them as 1
                src_ips[src_ip] = dt
            num_src_ips += src_ips[src_ip]*1.0/dt

            if src_ip in dest_ips:
                d = src_ips[src_ip]
                if dest_ips[src_ip] < d:
                    d = dest_ips[src_ip]
                num_src_and_dest_ips += d*1.0/dt

        csvout.writerow([round(t-min_time),dt,round(flow),round(bytes_sent),round(num_src_ips),round(num_src_and_dest_ips)])

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Analyze the log of a netblast manager to summarize network flows.')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--src',action='append',help='filter by IP address of source')
    parser.add_argument('--dest',action='append',help='filter by IP address of destination')
    parser.add_argument('--dt',default=30,type=int,help='time delta between output records')
    parser.add_argument('logfile')
    parser.add_argument('outputcsv')

    args = parser.parse_args()
    analyzeNetBlastLog(args.logfile,args.outputcsv,args.src,args.dest,args.dt,args.debug)
