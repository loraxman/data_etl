#make provider object at top.
import json
import threading
import multiprocessing
import psycopg2cffi
import psycopg2

import Queue
from time import sleep
import zlib
import requests

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
        p = threading.Thread(target=service_single_provider, args=(queue,i))
        p.setDaemon(True)
        threads.append(p)
        p.start()

def make_json_providers():
    
    #start threads
    start_consumers(10)
    conn3 = psycopg2cffi.connect("dbname='sandbox_rk' user='rogerk' port='9000' host='192.168.1.20' password='1yamadx7'")
    cur=conn3.cursor()
    cur.execute("Select provdrkey from  h_provdr d  limit 50")
    
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
       
def service_single_provider(svcque,threadno): 
    if not sqlQ:
        fjson = open(dpath + "/jsonout"+str(threadno)+".dat" , "w")
    conn = psycopg2cffi.connect("dbname='sandbox_rk' user='rogerk' port='9000' host='192.168.1.20' password='1yamadx7'")
    cur5=conn.cursor()
    cnt = 0
    start_trx = True
    while True:
        try:
            provdrkey  = svcque.get()
            if provdrkey == -99:
                try:
                    #issue commit for last incomplete batch
                     if sqlQ:
                        curjson.execute("commit")
                     else:
                        fjson.close()
                     print "commit final ... thread %d  at: %d" % (threadno,cnt)
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
              select  distinct langname from h_provdr a, vault.h_lang e, vault.s_lang d
              ,   vault.l_provdr_lang c
              where 
              a.provdrkey = c.provdrkey
              and e.langkey = c.langkey
              and d.langkey  = e.langkey
             and a.provdrkey = %s  
        """
        cur2=conn.cursor()
        cur2.execute(sql % (provider['provdrkey']))
        rowsloc = cur2.fetchall()
        provider['languages'] = []
        for rowloc in rowsloc:
            providerlangs={}
            cols = gettabcols(cur2.description,"lang")            
            for k,v in cols.items():
                try:
                    providerlangs[v] = rowloc[k].strip()
                except:
                    providerlangs[v] = rowloc[k]
                    
        
            provider['languages'].append(providerlangs)
            
        #provider locns            
        cur2=conn.cursor()
        cur2.execute("Select * from mgeo_vcprovdrlocn b where b.provdrkey = %s " % (provider['provdrkey']))
        rowsloc = cur2.fetchall()
        providerlocns = []
        
        for rowloc in rowsloc:
            cols = gettabcols(cur2.description,"provdrlocn")
        
            providerlocn = {}
            for k,v in cols.items():
                try:
                    providerlocn[v] = rowloc[k].strip()
                except:
                    providerlocn[v] = rowloc[k]
                    
                  
            providerlocns.append(providerlocn)
        
        provider['locations'] = providerlocns   
        
        
         
        provider['specialities'] = []
        sql = """
             select distinct a.amaspeclgroupcode as specialty
             from m_vcamaspeclgrouppractspecl a,
             mgeo_vcprovdrlocn b,
             m_vcpractspecl c,
             l_provdrlocnpractspecl_practspecl e,
             l_provdrlocnpractspecl_provdrlocn f,
             h_provdrlocnpractspecl g
             where 
             a.practspeclkey = c.practspeclkey
             and e.practspeclkey = c.practspeclkey
             and f.provdrlocnkey = b.provdrlocnkey  
             and g.provdrlocnpractspeclkey = e.provdrlocnpractspeclkey
             and g.provdrlocnpractspeclkey = f.provdrlocnpractspeclkey
             and b.provdrkey = %d
    
       """
        cur3=conn.cursor()
#            cur3.execute("Select * from vcpractspecl c,m_vcprovdrlocnpractspecl e where e.provdrlocnkey = %s and e.practspeclkey = c.practspeclkey" % (providerlocn['provdrlocnkey']))
        cur3.execute(sql % (provider['provdrkey']))
        rowspract = cur3.fetchall()
        for rowpract in rowspract:
            cols = gettabcols(cur3.description,"specialty")

            practspecl = {}
            for k,v in cols.items():
                try :
                    practspecl[v] = rowpract[k].strip()
                except:
                    practspecl[v] = rowpract[k]
                    

            provider['specialities'].append(practspecl)   
            
            
        
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
        provider['hospitals'] = []
        for rowloc in rowshosp:
            providerhospitals={}
            cols = gettabcols(cur2.description,"provdr")            
            for k,v in cols.items():
                try:
                    providerhospitals[v] = rowloc[k].strip()
                except:
                    providerhospitals[v] = rowloc[k]
                    
            provider['hospitals'].append(providerhospitals)

        sql = """
        select distinct c.netwkcategorycode, netwkcategorydescr
        from 
 
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
        and h.provdrlocnkey = e.provdrlocnkey
 
        and i.netwkcategorykey = c.netwkcategorykey
        and h.provdrkey = %d
        """
        
        cur2.close()
        cur2=conn.cursor()
        cur2.execute(sql % (provider['provdrkey']))
        rowsnet = cur2.fetchall()
        provider['networks'] = []
        for rowloc in rowsnet:
            providernetworks={}
            cols = gettabcols(cur2.description,"net")            
            for k,v in cols.items():
                try:
                    providernetworks[v] = rowloc[k].strip()
                except:
                    providernetworks[v] = rowloc[k]
                    
            provider['networks'].append(providernetworks)

        #provder quality program
        sql = """
        select qualityprogramdescr,qualityprogramcode from vault.h_qualityprogram a, 
        vault.s_qualityprogram b,
        vault.l_provdrlocn_qualityprogram c,
        h_provdrlocn d,
        s_provdrlocn e,
        h_provdr f,
        s_provdr g,
        l_provdrlocn_provdr h
        where a.qualityprogramkey = b.qualityprogramkey
        and a.qualityprogramkey = c.qualityprogramkey
        and d.provdrlocnkey = c.provdrlocnkey 
        and e.provdrlocnkey = d.provdrlocnkey
        and h.provdrkey = f.provdrkey
        and h.provdrlocnkey = d.provdrlocnkey
        and f.provdrkey = g.provdrkey 
        and f.provdrkey = %d
        """  
        
             
        cur2.close()
        cur2=conn.cursor()
        cur2.execute(sql % (provider['provdrkey']))
        rowsnet = cur2.fetchall()
        provider['programs'] = []
        for rowloc in rowsnet:
            providerprograms={}
            cols = gettabcols(cur2.description,"quality")            
            for k,v in cols.items():
                try:
                    providerprograms[v] = rowloc[k].strip()
                except:
                    providerprograms[v] = rowloc[k]
                    
            provider['programs'].append(providerprograms)


   #     print len(json.dumps(provider))
        jsoncompressed = zlib.compress(json.dumps(provider))
        provdrjson = json.dumps(provider)
        provdrjson = provdrjson.replace("'","''")
        if sqlQ:
            curjson = conn.cursor()
            if start_trx:
                curjson.execute("begin") 
                start_trx = False
            sql = "update provider_json set provdrjson = '%s' where provdrkey = %s" % (provdrjson,provider['provdrkey'])
            curjson.execute (sql)
            if curjson.rowcount == 0:
                sql = "insert into provider_json (provdrid, provdrkey,provdrjson) values ('%s','%s','%s') " % (provider['provdrid'],provider['provdrkey'],provdrjson)
                curjson.execute (sql)
         #   curjson.execute("commit")
#            sql = "insert into provider_json_compress (provdrid, provdrkey,provider_json_ztext) values ('%s','%s',%s) " % (provider['provdrid'],provider['provdrkey'],psycopg2cffi.Binary(jsoncompressed))
  #          curjson.execute (sql)
            svcque.task_done()
            cnt += 1
            if cnt % 100 == 0:
                    curjson.execute("commit")
                    start_trx = True
                    print "thread %d  at: %d" % (threadno,cnt)
        else:
            fjson.write("%s|%s|%s\n" % (provider['provdrid'],provider['provdrkey'],provdrjson))
            cnt += 1
            if cnt % 100 == 0:
                print "thread %d  at: %d" % (threadno,cnt)
            svcque.task_done()


def push_to_solr():

    #provdrkey, locations geocodes, hospital names, specialties,provdr is facility,major clasification,name,lastname,firstname
    conn = psycopg2cffi.connect("dbname='sandbox_rk' user='rogerk' port='9000' host='192.168.1.20' password='1yamadx7'")
    cur=conn.cursor(name='scrollit')
    sql = "select provdrjson from provider_json"
    cur.execute(sql)
    idx = 0
    while True:
        row = cur.fetchone()
        #print row
        if row == None:
            break
        solrhash = {}
        solrhash= json.loads(row[0])
        
        idx = 0;
        solrhash['geos'] = []
        for locn in solrhash['locations']:
            geopair = str(locn['provdrlocnlatitude'])  + "," + str(locn['provdrlocnlongitude'])
            solrhash['geos'].append(geopair)
   
        del(solrhash['locations'])
        
        specialities = []
        for locn in solrhash['specialities']:
            specialities.append(locn['specialty'])
            
        solrhash['specialities'] = specialities
      #  print solrhash['specialities']
        del solrhash['networks']           
        del solrhash['programs']
        del solrhash['languages']           
        
        tosolr =   json.dumps(solrhash) 
    #    print tosolr
        headers = {'Content-type': 'application/json'}
     #   r = requests.post('http://172.22.101.104:8983/solr/provider/update?commit=true', data="[" + json.dumps(py) +"]", headers=headers)
        r = requests.post('http://localhost:8984/solr/provider/update?commit=true&overwrite=true', data="[" + json.dumps(solrhash) +"]", headers=headers)
        #print response.read()
        idx += 1
        if idx%1000 == 0:
            print idx
    
    
sqlQ = True 
dpath = "."         
#make_json_providers()
push_to_solr()


    