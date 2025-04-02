#!/bin/sh
# /usr/share/xrootd/utils/XrdOlbMonPerf 10
# interval: the reporting frequency via stdout of load information
# This script will report 100% load if the local disk usage exceeds 95%.
/etc/xrootd/xrdload.py --interval 3 --logfile /var/log/xrootd/xrdload/xrdload.log --logfilelevel=info
