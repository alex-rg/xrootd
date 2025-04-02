#!/usr/bin/env python3

import argparse
import logging
import pathlib
import psutil
import statistics as stats
import sys
import re
import time
import threading
import socket
import subprocess

from collections import deque, namedtuple
from datetime import datetime
from logging.handlers import RotatingFileHandler
from math import ceil

Load = namedtuple("Load", ['system', 'cpu', 'mem', 'paging', 'net'])


def bound_int(value, min_val=0, max_val=100):
    """Round a value to int, and ensure is bounded in a range"""
    return max(min(ceil(round(value, 0)), max_val), min_val)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Load reporter for xrootd servers')
    parser.add_argument('-i', '--interval', help='Interval of reporting [s]', type=int, default=5)
    #parser.add_argument('--config', default='config.ini', help='Configuration file')
    parser.add_argument('-d', '--debug', action='store_true', help='Set verbose logging')
    parser.add_argument('--loadfile', default='/etc/nolb')
    parser.add_argument('--pingtime', default='/etc/pingtime')
    parser.add_argument('--nconns', default='/etc/nconns')
    parser.add_argument('--logfile', default=None)
    parser.add_argument('--logfilelevel', default='INFO')
    parser.add_argument('--logfilemaxMB', default=100)

    args = parser.parse_args()
    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)
    # log.name = "PERF"

    handlers = []
    log_formatter = logging.Formatter('STATS-%(asctime)s-%(process)d-%(levelname)s-%(message)s')
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.DEBUG if args.debug else logging.CRITICAL)
    console_handler.setFormatter(log_formatter)
    handlers.append(console_handler)

    if args.logfile is not None:
        handler = RotatingFileHandler(args.logfile, maxBytes=args.logfilemaxMB*1024**2,
                                      backupCount=1,
                                      )
        handler.setFormatter(log_formatter)
        handler.setLevel(args.logfilelevel.upper())
        handlers.append(handler)
#                    format='STATS-%(asctime)s-%(process)d-%(levelname)s-%(message)s',
    # logging.basicConfig(level= logging.DEBUG if args.debug else logging.INFO,
    #                 format=log_formatter,
    #                 handlers=handlers)
                    # filename=None if args.logfile is None else args.logfile,
    for h in handlers:
        log.addHandler(h)
    log.propagate = False

    return args


def report(load):
    output = "{} {} {} {} {}\n".format(load.system, load.cpu, load.mem, load.paging, load.net)
    log.info(f'Reporting: {load}')
    sys.stdout.write(output)
    sys.stdout.flush()


def get_system_load(loadint=5):
    load_vals = (1, 5, 15)
    if loadint not in load_vals:
        raise ValueError("Load average only reports for 1,5,15 min intervals")

    num_log_cpus = psutil.cpu_count()
    percent_tuple = []
    for load in psutil.getloadavg():
        percent = (load / num_log_cpus) * 100
        percent_tuple.append(percent)
    log.debug(f'System load: {percent_tuple}')
    el = load_vals.index(loadint)
    return bound_int(percent_tuple[el], 0, 100)


def get_memory_percent():
    mem = psutil.virtual_memory()
    log.debug(f"Memory: {mem}")
    return bound_int(mem.percent, 0, 100)


def get_cpu_percent(interval=1):
    cpus = psutil.cpu_percent(interval=interval, percpu=True)
    cpu_avg = stats.mean(cpus)
    log.debug(f"Cput: mean: {cpu_avg}%, percpu: {cpus}")
    return bound_int(cpu_avg, 0, 100)


def get_if_and_speed():
    """return tuple of interface name, and card speed [Bytes/s]"""
    addrs = {k: v for k, v in psutil.net_if_addrs().items() if k != 'lo'}
    log.debug(f'found interfaces {addrs.keys()}')
    stats = psutil.net_if_stats()

    candidates = []
    for ifname in addrs.keys():
        # check has stats
        if not ifname in stats:
            continue
        # check as ipv4
        if not any([x.family == socket.AddressFamily.AF_INET for x in addrs[ifname]]):
            continue

        candidates.append(ifname)
    log.debug(f'Candidate interfaces {candidates}')
    if len(candidates) == 0:
        raise RuntimeError("No Network devices passed selection")

    selected_ifname = None

    if len(candidates) == 1:
        log.debug(f'Candidate interface selected {candidates[0]}')
        selected_ifname = candidates[0]

    # now try and pick best if multiple:
    if len(candidates) > 1:
        selected_ifname = max(candidates, key=lambda x: stats[x].speed)
        log.debug(f'Candidate interface selected {selected_ifname} from multiple')

    speed = stats[selected_ifname].speed * 1024*1024
    log.debug(f'Candidate interface speed {speed}')

    return selected_ifname, speed


def ema(data, alpha=0.):
    """
    alpha = 1 - exp(-dT /tau), if dT << tau, alpha ~ dT / tau
    if dT << average age, then average age ~ tau
    """
    # FIXME - very inefficient
    if len(data) == 0:
        return [0]
    ema = deque(maxlen=len(data))
    ema.appendleft(data[-1])  # Initialize the EMA with the first data point
    for i in range(1, len(data)):
        ema_val = alpha * data[-i] + (1 - alpha) * ema[0]
        ema.appendleft(ema_val)
    return ema


class NetStats:
    def __init__(self, ifname, interval=2, max_time_s=15*60):
        self._interval = interval
        self._n_elements = ceil(max_time_s / interval)
        self._bytes_sent = deque(maxlen=self._n_elements)
        self._bytes_recv = deque(maxlen=self._n_elements)
        self._ifname = ifname
        self._last_stats = None
        self._running = False
        self._th_looper = None
        self.start()  # start the thread

    def _update(self):
        tx, stats = datetime.utcnow(), psutil.net_io_counters(pernic=True, nowrap=True)[self._ifname]
        if self._last_stats is None:
            self._last_stats = (tx, stats)
        else:
            dt = (tx - self._last_stats[0]).total_seconds()
            delta_bytes_sent = stats.bytes_sent - self._last_stats[1].bytes_sent
            delta_bytes_recv = stats.bytes_recv - self._last_stats[1].bytes_recv
            self._bytes_sent.appendleft((dt, delta_bytes_sent))
            self._bytes_recv.appendleft((dt, delta_bytes_recv))
            self._last_stats = (tx, stats)

    def _loop(self):
        while self._running:
            self._update()
            if not self._running:
                break
            time.sleep(self._interval)

    def get_ema(self, alpha=0.):
        ema_sent = ema([x[1] for x in self._bytes_sent], alpha)
        ema_recv = ema([x[1] for x in self._bytes_recv], alpha)
        return (ema_sent[0], ema_recv[0])

    def isrunning(self):
        return self._running

    def results(self, time_average_s, card_speed):
        if not self._running:
            raise RuntimeError("Not running")
        if time_average_s <= 0:
            raise ValueError("time_average_s must be positive number")

        log.debug(f"EMA sent [Gb/s]: {[('%.01f'%(x[1]/x[0]*8/1024**3)) for x in self._bytes_sent]}")
        log.debug(f"EMA recv [Gb/s]: {[('%.01f'%(x[1]/x[0]*8/1024**3)) for x in self._bytes_recv]}")

        alpha = self._interval / time_average_s
        vals = self.get_ema(alpha=alpha)
        v_sent = vals[0] / self._interval * 8. / card_speed * 100.
        v_recv = vals[1] / self._interval * 8. / card_speed * 100.
        log.debug(f"Net pct of card: sent:{v_sent:.01f}, recv:{v_recv:.01f} [%]")
        return bound_int(max(v_sent, v_recv), 0, 100)

    def __str__(self):
        return f'NetStat {self._ifname}, {self._interval}'

    def start(self):
        if self._running:
            return  # already started
        # run this once, to set in internal stuff going
        psutil.cpu_percent(interval=None, percpu=True)

        self._running = True
        self._th_looper = threading.Thread(target=self._loop, daemon=True)
        self._th_looper.start()

    def stop(self):
        if not self._running:
            return
        self._running = False


class CpuStats:
    def __init__(self, interval=2, max_time_s=15*60):
        self._interval = interval
        self._n_elements = ceil(max_time_s / interval)
        self._stats = deque(maxlen=self._n_elements)

        self._running = False
        self._th_looper = None
        self.start()  # start the thread

    def _update(self):
        tx, _stats = datetime.utcnow(), psutil.cpu_percent(interval=None, percpu=True)
        self._stats.appendleft((tx, stats.mean(_stats)))

    def _loop(self):
        while self._running:
            self._update()
            if not self._running:
                break
            time.sleep(self._interval)

    def get_ema(self, alpha=0.):
        ema_cpu_pct = ema([x[1] for x in self._stats], alpha)
        return ema_cpu_pct

    def isrunning(self):
        return self._running

    def results(self, time_average_s):
        if not self._running:
            raise RuntimeError("Not running")
        if time_average_s <= 0:
            raise ValueError("time_average_s must be positive number")

        log.debug(f"EMA cpu[%]: {[('%.01f'%(x[1])) for x in self._stats]}")

        alpha = self._interval / time_average_s
        vals = self.get_ema(alpha=alpha)[0]
        log.debug(f"CPU: {vals:.01f}[%]")
        return bound_int(vals, 0, 100)

    def __str__(self):
        return f'CPUStat {self._interval}'

    def start(self):
        if self._running:
            return  # already started
        self._running = True
        self._th_looper = threading.Thread(target=self._loop, daemon=True)
        self._th_looper.start()

    def stop(self):
        if not self._running:
            return
        self._running = False


class MemStats:
    def __init__(self, interval=2, max_time_s=15*60):
        self._interval = interval
        self._n_elements = ceil(max_time_s / interval)
        self._stats = deque(maxlen=self._n_elements)

        self._running = False
        self._th_looper = None
        self.start()  # start the thread

    def _update(self):
        tx, _stats = datetime.utcnow(), psutil.virtual_memory().percent
        self._stats.appendleft((tx, _stats))

    def _loop(self):
        while self._running:
            self._update()
            if not self._running:
                break
            time.sleep(self._interval)

    def get_ema(self, alpha=0.):
        ema_pct = ema([x[1] for x in self._stats], alpha)
        return ema_pct

    def isrunning(self):
        return self._running

    def results(self, time_average_s):
        if not self._running:
            raise RuntimeError("Not running")
        if time_average_s <= 0:
            raise ValueError("time_average_s must be positive number")

        log.debug(f"EMA mem[%]: {[('%.01f'%(x[1])) for x in self._stats]}")

        alpha = self._interval / time_average_s
        vals = self.get_ema(alpha=alpha)[0]
        log.debug(f"Mem: {vals:.01f}[%]")
        return bound_int(vals, 0, 100)

    def __str__(self):
        return f'MemStat {self._interval}'

    def start(self):
        if self._running:
            return  # already started
        self._running = True
        self._th_looper = threading.Thread(target=self._loop, daemon=True)
        self._th_looper.start()

    def stop(self):
        if not self._running:
            return
        self._running = False


def has_override_file(fname, default=Load(100, 100, 100, 100, 100)):
    if not pathlib.Path(fname).exists():
        return (False, Load(0, 0, 0, 0, 0))
    val = default
    with open(fname) as fii:
        for line in fii.readlines():
            ll = line.strip()
            if len(ll) == 0:
                continue
            if ll[0] == '#':
                continue
            rg = re.search('(\d+)\w+(\d+)\w+(\d+)\w+(\d+)\w+(\d+)', ll)
            if rg is None:
                continue
            val = Load(bound_int(rg.group(1), 0, 100),
                       bound_int(rg.group(2), 0, 100),
                       bound_int(rg.group(3), 0, 100),
                       bound_int(rg.group(4), 0, 100),
                       bound_int(rg.group(5), 0, 100)
                       )
    log.debug(f"Override file found; returning: {val}")
    return (True, val)


def disk_space_pct(path='/'):
    val_pct = psutil.disk_usage(path).percent
    return bound_int(val_pct, 0, 100)


log = logging.getLogger(__name__)


def run():
    # Parse command line arguments
    args = parse_arguments()
    # trigger initial call, to start the internal timers
    # psutil.cpu_percent(None, False)
    hostname = socket.getfqdn()
    log.info(f"{'='*20}Starting Load reporting on: {str(hostname)}")

    net_card, net_speed = get_if_and_speed()
    # old_if_stats = (datetime.utcnow(), psutil.net_io_counters(pernic=True, nowrap=True)[net_card])
    netstats = NetStats(net_card, interval=min(args.interval, 5), max_time_s=15*60)
    cpustats = CpuStats(interval=min(args.interval, 5), max_time_s=15*60)
    memstats = MemStats(interval=min(args.interval, 5), max_time_s=15*60)
    max_seen = 0
    min_seen = 10000
    try:
        while True:
            time.sleep(args.interval)
            sys_load = get_system_load(5)
            mem = memstats.results(time_average_s=3*60)
            cpu = cpustats.results(time_average_s=3*60)
            # was historically used, but now is needed as an entry only for backwards compatibility
            # perfect round robin value
            paging = 50

            net = netstats.results(time_average_s=3*60, card_speed=net_speed)
            disk = disk_space_pct('/')
            disk_threshold = 95
            override, l = has_override_file(args.loadfile)
            nconns, l = has_override_file(args.nconns)
            man_ping, l = has_override_file(args.pingtime)
            if nconns:
                if max_seen != 1500:
                    max_seen = 1500
                with open('/tmp/connectionLog.txt', 'r') as f:
                    conns = f.read()
                conns = conns.split('\n')
                conns = [i for i in conns if 'ceph-sn' not in i]
                if len(conns) < min_seen:
                    min_seen = len(conns)
                if len(conns) > max_seen:
                    paging = 100
                    max_seen = len(conns)
                else:
                    if max_seen == min_seen:
                        paging = 50
                    else:
                        paging = 100-int((len(conns)-min_seen)*100/(max_seen-min_seen))
            if man_ping:
                start = time.time()
                p = subprocess.Popen(["ping", '-c', "2", "rdr.echo.stfc.ac.uk"], stdout=subprocess.PIPE)
                pingr = p.communicate()[0]
                pingtime = time.time() - start
                if pingtime < min_seen:
                    min_seen = pingtime
                if pingtime > max_seen:
                    max_seen = pingtime
                    paging = 70
                else:
                    if max_seen == min_seen:
                        paging = 50
                    else:
                        paging = int((pingtime-min_seen)*100/(max_seen-min_seen))
            if sys_load > 97:
                paging = 100
            if override:
                load = l
            elif disk > disk_threshold:
                log.debug(f"Disk threshold exceeded on path /: {disk} > {disk_threshold}")
                load = Load(100, 100, 100, 100, 100)
            else:
                load = Load(sys_load, cpu, mem, paging, net)
            # finally, send the report to stdout
            report(load)
    except KeyboardInterrupt:
        log.info("Keyboard interupt. Terminating ... ")
        pass


if __name__ == "__main__":
    run()
