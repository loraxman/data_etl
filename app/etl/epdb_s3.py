from job import StepQueueEntry
import s3util
import urllib2
import json
import datetime as dt
import imp
import datetime as dt
import os
import psycopg2
from datetime import timedelta

def execstep(queue=None,step=None,step_pids=None):
    step.active=True
    for wstep in step.wait_steps:
        for wpid in step_pids[wstep]:
            wpid.join()


    py_mod = imp.load_source("wmbackupaws","./wmbackupaws.py")
    bucket='wellmatch-healthline-provider-data'

    files = [ 
        "provlang.dat.gz",
        "provsrvlocspec.dat.gz",
        "provsrvloc.dat.gz",
        "hospaff.dat.gz",
        "provlddelta.dat.gz",
        "provsrvlocflg.dat.gz",
        "srvgrpprovass.dat.gz",]
    for filepattern in files:
      s3util.get_s3_file(filepattern,bucket,py_mod.WMEPDB_AWS_ID,py_mod.WMEPDB_AWS_KEY,named_pipe=None, destination_dir="./")

    sq = StepQueueEntry (step,"PASS")
    queue.put(sq)
