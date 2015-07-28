from job import StepQueueEntry
import s3util
import urllib2
import json
import datetime as dt
import imp
import datetime as dt
import os
from datetime import timedelta

def execstep(queue=None,step=None,step_pids=None):
    step.active=True
    for wstep in step.wait_steps:
        for wpid in step_pids[wstep]:
            wpid.join()

    py_mod = imp.load_source("wmbackupaws","../../wmbackupaws.py")
    #wellmatch_production/wellmatch_production_digest_lookup/2015.07.12.00.00.04/wellmatch_production_digest_lookup.tar 
    #get yesterdays file
    #download
    #untar and load up using inserts/copy
    yesterday = dt.datetime.today()+timedelta(days=-1)
    filepattern = "wellmatch_production/wellmatch_production_digest_lookup/%d.%02d.%02d" % (yesterday.year, yesterday.month,yesterday.day)
    s3util.get_s3_file(filepattern,'wm-database-backups-production',py_mod.WMBACKUP_AWS_ID,py_mod.WMBACKUP_AWS_KEY,named_pipe=None, dirct='wellmatch_production')
    #find our file
    items = os.listdir(".")
    fname = None
    for item in items:
      print item, filepattern[filepattern.find("/")+1:].replace("/","."), filepattern.replace("/",".")
      if item.find(filepattern.replace("/",".")) != -1:
        fname=item
        break
    if fname:
      cmd = "tar -xvf %s " % fname
      print cmd
      os.system(cmd)
      cmd = "tar -xvf wellmatch_production_digest_lookup/archives/digest_lookup.tar.gz"
      os.system(cmd)
      #digest_to_member_id_mapping.txt  now we have this filename to read and load
      #remove the directory and old file
      cmd = "rm %s " % fname
      os.system(cmd)
      cmd = "rm -rf wellmatch_production_digest_lookup"
      os.system(cmd)
