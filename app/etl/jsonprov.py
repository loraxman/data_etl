#make provider object at top.
import json
import threading
import multiprocessing
import psycopg2
import yaml
import Queue
from time import sleep
import zlib

import sys

queue =  Queue.Queue()
threads = []
def gettabcols(meta,tabname,notcols=None):
    retcols = {}
    idx = 0
    for col in meta:
        if col.name.find(tabname) != -1:
            if notcols and col.name.find(notcols) == -1:
                retcols[idx] = col.name
            elif notcols == None :
                retcols[idx] = col.name

        idx += 1
    return retcols



def start_consumers(num_consume):
    for i in range(0, num_consume):
        p = threading.Thread(target=service_single_provider, args=(queue,))
        p.setDaemon(True)
        threads.append(p)
        p.start()

def make_json_providers():
    
    #start threads
    start_consumers(3)
    conn3 = psycopg2.connect("dbname='sandbox_rk' user='rogerk' port='9000' host='192.168.1.20' password='1yamadx7'")
    cur=conn3.cursor()
    cur.execute("Select provdrkey from  h_provdr d  limit 5")
    
    while True:
        row = cur.fetchone()
        if row == None:
            break
        queue.put(row[0])
    #wait for queue to empty
    print "waiting..."
    queue.join()
    #at end of queue put -99 on queue to flush the commit before each thread dies
    print "flush trx"
    for i in range(0,30):
        queue.put(-99)
    queue.join()
       
def service_single_provider(svcque): 
    conn = psycopg2.connect("dbname='sandbox_rk' user='rogerk' port='9000' host='192.168.1.20' password='1yamadx7'")
    cur5=conn.cursor()
    cnt = 0
    start_trx = True
    while True:
        try:
            provdrkey  = svcque.get()
            print "provdkey:%d" % provdrkey
            if provdrkey == -99:
                try:
                     curjson.execute("commit")
                     print "commit final"
                     svcque.task_done()
                     continue
                except:
                    svcque.task_done()
                    continue
                    
        except:
            print "end of queue"
            svcque.task_done()
            continue
        cur5.execute("Select * from  vcprovdr where provdrkey = %d" % (provdrkey))
        row = cur5.fetchone()
     #   print cur5.description
        cols = gettabcols(cur5.description,"provdr","provdrlocn")
        #go thru qualified cols and grab from row
        provider = {}
        providerlocns = []
        for k,v in cols.items():
            try:
                provider[v] = row[k].strip()
            except:
                provider[v] = row[k]
              #provider languages
        sql = """
            select  langname from h_provdr a, vault.h_lang e, vault.s_lang d
          , s_provdr b,  vault.l_provdr_lang c
          where a.provdrkey = b.provdrkey 
          and a.provdrkey = c.provdrkey
          and e.langkey = c.langkey
          and d.langkey  = e.langkey
          and a.provdrkey = %s  
        """
        cur2=conn.cursor()
        cur2.execute(sql % (provider['provdrkey']))
        rowsloc = cur2.fetchall()
        providerlangs={}
        for rowloc in rowsloc:
            cols = gettabcols(cur2.description,"lang")            
            for k,v in cols.items():
                try:
                    providerlangs[v] = rowloc[k].strip()
                except:
                    providerlangs[v] = rowloc[k]
                    
        
        provider['providerlangs'] = providerlangs
        #provider locns            
        cur2=conn.cursor()
        cur2.execute("Select * from mgeo_vcprovdrlocn b where b.provdrkey = %s " % (provider['provdrkey']))
        rowsloc = cur2.fetchall()
        providerlocns = []
        
        for rowloc in rowsloc:
            cols = gettabcols(cur2.description,"provdrlocn")
        
            providerlocn = {}
            for k,v in cols.items():
                    providerlocn[v] = rowloc[k]
            providerlocn['practspecl'] = []
            cur3=conn.cursor()
            cur3.execute("Select * from vcpractspecl c,m_vcprovdrlocnpractspecl e where e.provdrlocnkey = %s and e.practspeclkey = c.practspeclkey" % (providerlocn['provdrlocnkey']))
            rowspract = cur3.fetchall()
            for rowpract in rowspract:
                cols = gettabcols(cur3.description,"practspecl")
    
                practspecl = {}
                for k,v in cols.items():
                    try :
                        practspecl[v] = rowpract[k].strip()
                    except:
                        practspecl[v] = rowpract[k]
                        
    
                providerlocn['practspecl'].append(practspecl)   
                    
                    
            providerlocns.append(providerlocn)
        
        provider['providerlocns'] = providerlocns    
        
        sql = """
            select  h.provdrname, h.provdrisfacilityflag from vault.h_provdrhospital a,
             vault.l_provdrhospital_provdr_provdr b,
             vault.l_provdrhospital_provdr_hospital c,
             h_provdr d,
             s_provdr e,
             h_provdr g,
             s_provdr h
             where a.provdrhospitalkey = b.provdrhospitalkey
             and b.provdrkey_provdr = d.provdrkey
             and e.provdrkey = d.provdrkey
             and a.provdrhospitalkey = c.provdrhospitalkey
             and c.provdrkey_hospital = g.provdrkey
             and h.provdrkey = g.provdrkey
             and d.provdrkey = %d
         """  
        cur2.close()
        cur2=conn.cursor()
        cur2.execute(sql % (provider['provdrkey']))
        rowshosp = cur2.fetchall()
        providerhospitals={}
        for rowloc in rowshosp:
            cols = gettabcols(cur2.description,"provdr")            
            for k,v in cols.items():
                try:
                    providerhospitals[v] = rowloc[k].strip()
                except:
                    providerhospitals[v] = rowloc[k]
                    
        provider['providerhospitals'] = providerhospitals

        sql = """
        select distinct c.netwkcategorycode, netwkcategorydescr
        from h_provdr a, 
        s_provdr b,
        h_provdrlocn e,
        vault.h_netwkcategory c,
        vault.h_provdrlocnnetwkcategory d,
        vault.l_provdrlocnnetwkcategory_provdrlocn f,
        vault.l_provdrlocnnetwkcategory_netwkcategory g,
        l_provdrlocn_provdr h,
        vault.s_netwkcategory i
        where  f.provdrlocnkey = e.provdrlocnkey
        and g.netwkcategorykey = c.netwkcategorykey
        and d.provdrlocnnetwkcategorykey = f.provdrlocnnetwkcategorykey
        and d.provdrlocnnetwkcategorykey = g.provdrlocnnetwkcategorykey
        and h.provdrkey = a.provdrkey
        and h.provdrlocnkey = e.provdrlocnkey
        and a.provdrkey = b.provdrkey
        and i.netwkcategorykey = c.netwkcategorykey
        and a.provdrkey = %d
        """
        
        cur2.close()
        cur2=conn.cursor()
        cur2.execute(sql % (provider['provdrkey']))
        rowsnet = cur2.fetchall()
        providernetworks={}
        for rowloc in rowsnet:
            cols = gettabcols(cur2.description,"net")            
            for k,v in cols.items():
                try:
                    providernetworks[v] = rowloc[k].strip()
                except:
                    providernetworks[v] = rowloc[k]
                    
        provider['providernetworks'] = providernetworks
        


   #     print len(json.dumps(provider))
        jsoncompressed = zlib.compress(json.dumps(provider))
        curjson = conn.cursor()
        provdrjson = json.dumps(provider)
        provdrjson = provdrjson.replace("'","''")
        if start_trx:
            curjson.execute("begin") 
            start_trx = False
        sql = "insert into provider_json (provdrid, provdrkey,provdrjson) values ('%s','%s','%s') " % (provider['provdrid'],provider['provdrkey'],provdrjson)
        curjson.execute (sql)
        curjson.execute("commit")
        sql = "insert into provider_json_compress (provdrid, provdrkey,provider_json_ztext) values ('%s','%s',%s) " % (provider['provdrid'],provider['provdrkey'],psycopg2.Binary(jsoncompressed))
        curjson.execute (sql)
        svcque.task_done()
        curjson.execute("commit")
        cnt += 1
        print cnt
        if cnt % 100 == 0:
                curjson.execute("commit")
                start_trx = True
                print "thread  at: %d" % (cnt)
   
    
make_json_providers()
    
