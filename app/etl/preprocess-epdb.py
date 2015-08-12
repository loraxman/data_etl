from job import StepQueueEntry
import s3util
import urllib2
import json
import datetime as dt
import imp
import datetime as dt
import os
import psycopg2
import gzip
import popen2
from datetime import timedelta

def execstep(queue=None,step=None,step_pids=None):
    step.active=True
    for wstep in step.wait_steps:
        for wpid in step_pids[wstep]:
            wpid.join()

    #read each file
    #strip out whitespace and cleanup
    #for big ones call java cleaner

    files = [ 
        "provlang.dat.gz",
        "provsrvlocspec.dat.gz",
        "provsrvloc.dat.gz",
        "hospaff.dat.gz",
        "provlddelta.dat.gz",
        "provsrvlocflg.dat.gz",
        "srvgrpprovass.dat.gz",]
    for filepattern in files:
      filename = './' + filepattern
      load_gz_file(filename)
      break
    sq = StepQueueEntry (step,"PASS")
    queue.put(sq)

def load_gz_file(filename):
  cmd =  "psql -Urogerk -h192.168.1.20 -p9000 OPDB"
  r, w, e = popen2.popen3(cmd)
  print get_table_name_from_file(filename)
  sql = "COPY staging.%s FROM STDIN  USING DELIMITERS '|' ;" % get_table_name_from_file(filename)
  print sql
  w.write(sql+";\n")
  w.flush()
  rec = 0
  with gzip.open(filename) as gz:
    inline = gz.readline()
    while inline != "":
      inline=inline.encode('utf-8')
      items = [i.strip().replace(";","~").replace("\\"," ").strip()+"|" for i in inline.split("|")]
      inline = "".join(items) 
      w.write(inline.strip("|")+"\n")
      w.flush()
      rec +=1
      if rec % 10000 == 0:
        print rec
      inline=gz.readline()
  w.close()

def get_table_name_from_file(filename):
  paths = filename.split("/")
  print paths[-1][0:paths[-1].index(".")]
  return paths[-1][0:paths[-1].index(".")]

