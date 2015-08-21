from job import StepQueueEntry
import s3util
import urllib2
import json
import datetime as dt
import imp
import gc
import datetime as dt
import cStringIO
import os
import copy

try:
  import psycopg2
except:
  import psycopg2cffi

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
    try:
      conn = psycopg2.connect(step.connectdb)
    except:
      conn = psycopg2cffi.connect(step.connectdb)

    files = [
        #"provlang.dat.gz",
        #"provsrvlocspec.dat.gz",
        #"provsrvloc.dat.gz",
        #"hospaff.dat.gz",
        #"provlddelta.dat.gz",
        #"provsrvlocflg.dat.gz",
        "srvgrpprovass.dat.gz",]

    for filepattern in files:
      if filepattern.find('srvgrpprovass') != -1:
          preload_srvgrp_file_filter("/Volumes/My Passport/WM/EPDB-JUL26/srvgrpprovass.dat.gz", "/Volumes/My Passport/WM/EPDB-JUL26/srvgrppabbrev.dat.gz", "/Volumes/My Passport/WM/EPDB-JUL26/provsrvloc.dat.gz")
          pg_copy_direct("/Volumes/My Passport/WM/EPDB-JUL26/provntwkloc.dat.gz","OPDB_DEV")
          pg_copy_direct("/Volumes/My Passport/WM/EPDB-JUL26/provntwkspec.dat.gz","OPDB_DEV")
          pg_copy_direct("/Volumes/My Passport/WM/EPDB-JUL26/srvgrppabbrev.dat.gz","OPDB_DEV")
          pg_copy_direct("/Volumes/My Passport/WM/EPDB-JUL26/provsrvloc.dat.gz","OPDB_DEV")
#          build_srvgrp_index  NOTE THIs is done as part of a SQL step now!
      else:
        filename = '/Volumes/My Passport/WM/EPDB-JUL26/' + filepattern
        load_gz_file(filename)


    sq = StepQueueEntry (step,"PASS")
    queue.put(sq)

def load_gz_file(filename):
  cmd =  "psql -UA727200 -h192.168.1.20 -p5432 opdb_dev"
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

def preload_srvgrp_file_filter(infilename,outfilename,providerfile):
  pmap = get_provider_loc_map(providerfile)
  #pass thru the srv grop file dedupping and attaching provider id
  curpin = ""
  prevpin = ""
  grpstack = []
  grpmap = {}
  reccnt = 0
  srvgrpid=1
  gout = gzip.open(outfilename,"wb")
  with gzip.open(infilename) as gz:
    inline = gz.readline()
    while inline != "":
      net = inline.strip().split("|")
      stringio = cStringIO.StringIO()
      stringio.write(net[0])
      stringio.write(";")
      stringio.write(str(int(net[3])))
      curpin = stringio.getvalue()
      if prevpin == "":
        prevpin = curpin
      net_row = create_network_row(net)
      #grpstack.append(net_row)
      del stringio
      stringio = cStringIO.StringIO()
      stringio.write(curpin)
      stringio.write("~")
      stringio.write(net_row)
      grpmap[stringio.getvalue()] = net_row
      del stringio
      if prevpin != curpin or prevpin>curpin:
          #do group processsing
          #get everything from the grpmap where the pin = prevpin and delete them
          grpmap,srvgrpid=write_group(prevpin,grpmap,gout,pmap,srvgrpid)
      reccnt += 1
      if reccnt % 10000 == 0:
          print reccnt
          print len(grpmap)
      if reccnt % 10000 == 0:
        newmap = copy.deepcopy(grpmap)
        del (grpmap)
        gc.collect()
        grpmap = newmap
      prevpin = curpin
      inline = gz.readline()

def write_group(prevpin,grpmap,gout,pmap,srvgrpid):
  del_rows = []
  #lookup the provider service loc id in map
  if pmap.has_key(prevpin):
    provid=pmap.get(prevpin)
    idx = 0
    for k,val in grpmap.iteritems():
      k2=k[0:k.find('~')]
      if idx == 0:
        idx=1
      if k2 == prevpin:
          gout.write(val)
          gout.write("|")
          gout.write(str(provid))
          gout.write("|")
          gout.write(str(srvgrpid))
          gout.write("\n")
          srvgrpid +=1
          del_rows.append(k)
  else:  # remove everything with this pin/servloc none found in provsrvloc
    for k,val in grpmap.iteritems():
      k2=k[0:k.find('~')]
      if k2 == prevpin:
        del_rows.append(k)

  for d in del_rows:
    del(grpmap[d])
  #print prevpin, grpmap
  #print
 # print
  return grpmap,srvgrpid




def create_network_row(net):
          buf =  cStringIO.StringIO();
          buf.write(net[0].strip());
          buf.write("|");
          buf.write(net[1].strip());
          buf.write("|");
          buf.write(net[3].strip());
          buf.write("|");
          buf.write(net[4].strip());
          buf.write("|");
          buf.write(net[5].strip());
          buf.write("|");
          buf.write(net[8].strip());
          buf.write("|");
          buf.write(net[13].strip());
          return  buf.getvalue();


def get_provider_loc_map(provlocfile):
  pin_map = {}
  cnt = 1
  with gzip.open(provlocfile) as gz:
    inline = gz.readline()
    while inline != "":
      row = inline.split("|")
      stringio = cStringIO.StringIO()
      stringio.write(row[0])
      stringio.write(";")
      stringio.write(str(int(row[18])))
      pin_map[stringio.getvalue()] = cnt
      cnt += 1
      if cnt % 100000 == 0:
        print cnt
      inline = gz.readline()
  return pin_map

def pg_copy_direct(srcfile, targetdatasource):
  pcopy_cmd = "COPY staging.%s FROM PROGRAM 'zcat %s ' USING DELIMITERS '|'" % (tablename, srcfile)
  cmd = "psql -UA727200 -h192.168.1.20 -p5432 opdb_dev  -c \"%s\" " % (pcopy_cmd)
  os.system(cmd)

