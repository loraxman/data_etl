#make provider object at top.
import json
import threading
import multiprocessing
from multiprocessing import Process, Queue
import psycopg2cffi
#import psycopg2

from time import sleep
import zlib
import requests
import time
import sys

queue =  Queue()
threads = []
def gettabcols(meta,tabname,notcols=None):
    retcols = {}
    idx = 0
    for col in meta:
        if col.name.find(tabname) != -1 or tabname=="*":
            if notcols and col.name.find(notcols) == -1:
                retcols[idx] = col.name
            elif notcols == None :
                retcols[idx] = col.name

        idx += 1
    return retcols



def start_consumers(num_consume):
    for i in range(0, num_consume):
        p = Process(target=service_single_provider, args=(queue,i))
        threads.append(p)
        p.start()

def make_json_providers():
    
    #start threads
    start_consumers(15)
    conn3 = psycopg2cffi.connect("dbname='sandbox_rk' user='rogerk' port='5432' host='localhost' password='1yamadx7'")
    cur=conn3.cursor()
    cur.execute("Select provdrkey from  h_provdr d  ")
    
    while True:
        row = cur.fetchone()
        if row == None:
            break
        queue.put(row[0])
    #wait for queue to empty
    print "waiting..."
    #at end of queue put -99 on queue to flush the commit before each thread dies
    print "flush trx"
    for i in range(0,30):
        queue.put(-99)
    queue.close()
    queue.join_thread()
       
def service_single_provider(svcque,threadno): 
    if not sqlQ:
        fjson = open(dpath + "/jsonout"+str(threadno)+".dat" , "w")
    conn = psycopg2cffi.connect("dbname='sandbox_rk' user='rogerk' port='5432' host='localhost' password='1yamadx7'")
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
                     #svcque.task_done()
                     break 
                except:
                    #svcque.task_done()
                    break 
        except:
            print "end of queue"
            #svcque.task_done()
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
        sql = """
        select distinct language_description from provlang 
        where pin = '%s' 
        """ 
        cur2=conn.cursor()
        cur2.execute(sql % (provider['provdrid']))
        rowsloc = cur2.fetchall()
        provider['languages'] = []
        cols = gettabcols(cur2.description,"lang")            
        for rowloc in rowsloc:
            providerlangs={}
            for k,v in cols.items():
                try:
                    providerlangs[v] = rowloc[k].strip()
                except:
                    providerlangs[v] = rowloc[k]
                    
        
            provider['languages'].append(providerlangs)
            
        #provider locns            
        cur2=conn.cursor()
        start_time = time.time()
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
        
        
        #print("--- %s provddrlocn seconds ---" % (time.time() - start_time)) 
         
        provider['specialities'] = []
        sql = """
            select distinct c.practspecldescr as specialty, practspeclcode as specialtyid,provdrlocnpractspeclprimspeclflag as isPrimary
            from
            mgeo_vcprovdrlocn b,
            m_vcpractspecl c,
            l_provdrlocnpractspecl_practspecl e,
            l_provdrlocnpractspecl_provdrlocn f,
            h_provdrlocnpractspecl g,
            s_provdrlocnpractspecl h
            where 
            
            e.practspeclkey = c.practspeclkey
            and f.provdrlocnkey = b.provdrlocnkey  
            and g.provdrlocnpractspeclkey = e.provdrlocnpractspeclkey
            and g.provdrlocnpractspeclkey = f.provdrlocnpractspeclkey
            and h.provdrlocnpractspeclkey = g.provdrlocnpractspeclkey             
            and b.provdrkey = %d
     
        """
        sql = """
        

        select distinct  
        '{' || '"specialty":' || to_json(trim(practice_description) ) ||','
                 '"specialtyid":' || to_json(trim(practice_code)) || ',"isPrimary":' || to_json(trim(prim_spec_ind)) || '}'
                 from provsrvlocspec
                 where pin = '%s'

         """

        cur3=conn.cursor()
#            cur3.execute("Select * from vcpractspecl c,m_vcprovdrlocnpractspecl e where e.provdrlocnkey = %s and e.practspeclkey = c.practspeclkey" % (providerlocn['provdrlocnkey']))
        start_time = time.time() 
        cur3.execute(sql % (provider['provdrid']))
        #print("--- %s Specialty seconds ---" % (time.time() - start_time)) 
        rowspract = cur3.fetchall()
        for rowpract in rowspract:

            practspecl = json.loads(rowpract[0])
            provider['specialities'].append(practspecl)   
            
        cur3.close()
            
        #print("--- %s Specialty seconds ---" % (time.time() - start_time)) 
         #bundle sql    
        sql = """   
           select distinct bundleid      from   
           mgeo_vcprovdrlocn b,
           m_vcpractspecl c,
           l_provdrlocnpractspecl_practspecl e,
           l_provdrlocnpractspecl_provdrlocn f,
           h_provdrlocnpractspecl g,
           CBOR i
           where 
           
           e.practspeclkey = c.practspeclkey
           and f.provdrlocnkey = b.provdrlocnkey  
           and g.provdrlocnpractspeclkey = e.provdrlocnpractspeclkey
           and g.provdrlocnpractspeclkey = f.provdrlocnpractspeclkey
           and c.practspeclcode = i.practicecode
           and b.provdrkey = %s
           union 
             select distinct bundleid      from   
             m_vcprovdr a,
             CBOR b
             where 
             b.practicecode is null
           and trim(a.provdrtype) = b.phtype
           and a.provdrkey = %s
        """
        sql = """
          select distinct bundleid      from   
           provsrvlocspec b,
         
           CBOR i
           where   
        b.practice_code = i.practicecode
           and b.pin = '%s'
           union 
             select distinct bundleid      from   
            provsrvlocspec b,
        provsrvloc a,
        CBOR i
             where 
             b.practice_code is null
           and trim(a.provider_type) = i.phtype
           and b.pin = '%s'
           and a.pin = b.pin
           and a.pin = '%s'     
        """
        cur3=conn.cursor()
        start_time=time.time()
        cur3.execute(sql % (provider['provdrid'],provider['provdrid'], (provider['provdrid'])))

        rowspract = cur3.fetchall()
        bundles = []        
        for rowpract in rowspract:
            cols = gettabcols(cur3.description,"bundleid")

            for k,v in cols.items():
               try :
                   bundles.append( rowpract[k].strip())
               except:
                   bundles.append( rowpract[k])
           

        provider['bundles'] =  bundles  
        #print("--- %s Bundles seconds ---" % (time.time() - start_time)) 
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
        start_time = time.time()
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

#        print("--- %s Hosp seconds ---" % (time.time() - start_time))
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
        #Note replace below to extend out for par/Non par
  #      sql = "select distinct category_code, current_tier, master_category_description, master_category_code, cast(cast (base_net_id_no as integer) as varchar) from staging.srvgrpprovass where pin = '%s'"
        sql = """
        select  distinct '{' ||  '"category_code":' || to_json(trim(category_code)) || ',' || 
        '"current_tier":' || to_json(current_tier)  || ',' ||
        '"master_category_code":' || to_json(trim(master_category_code)) || ',' || 
        case 
        when trim(master_category_description)='AEXCELP' then '"mstr_type":'||'"M"'
        when trim(master_category_description)='AEXCEL'  then '"mstr_type":'||'"C"'
        when strpos(master_category_description,'MULTI') > 0  then '"mstr_type":'||'"M"'
        when strpos(master_category_description,'CONCENTRIC') > 0  then '"mstr_type":'||'"C"'
        when strpos(master_category_description,'NATIONAL') > 0  then '"mstr_type":'||'"C"'
        else '"mstr_type":'||'"U"'
        end  ||  ',' 
        '"base_net_id_no":' || '"' || to_json(cast (base_net_id_no as integer) ) || '" }'
        from staging.srvgrpprovass where pin = '%s';        """
        cur2.close()
        cur2=conn.cursor()
        start_time = time.time()
        cur2.execute(sql % (provider['provdrid']))
        rowsnet = cur2.fetchall()
  #      cols = gettabcols(cur2.description,"*")               
        provider['networks'] = []
        for rowloc in rowsnet:
           # providernetworks={}
            providernetworks=json.loads(rowloc[0])
            provider['networks'].append(providernetworks)

        #print("--- %s Network seconds ---" % (time.time() - start_time))
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
        start_time = time.time()
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
        #jsoncompressed = zlib.compress(json.dumps(provider))
        #print("--- %s Quality seconds ---" % (time.time() - start_time))
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
            #curjson.execute("commit")
#            sql = "insert into provider_json_compress (provdrid, provdrkey,provider_json_ztext) values ('%s','%s',%s) " % (provider['provdrid'],provider['provdrkey'],psycopg2cffi.Binary(jsoncompressed))
  #          curjson.execute (sql)
            #svcque.task_done()
            cnt += 1
            if cnt%10 == 0:
                    curjson.execute("commit")
                    start_trx = True
                    print "thread %d  at: %d " % (threadno,cnt)
        else:
            fjson.write("%s|%s|%s\n" % (provider['provdrid'],provider['provdrkey'],provdrjson))
            cnt += 1
            if cnt % 100 == 0:
                print "thread %d  at: %d" % (threadno,cnt)
            #svcque.task_done()

def bundle_search():
    conn = psycopg2cffi.connect("dbname='sandbox_rk' user='rogerk' port='9000' host='192.168.1.20' password='1yamadx7'")
    sql = """
    select bundleid,bundlename,practice_descr as specialty, procedure_main as dse_term from staging.proceduremapping a,
    CBOR b
    where a.practice_code = b.practicecode
    order by bundleid
    """    
    sqltaxo = """
    select distinct \"QUERY\" from 
    condition_specialty a,
    staging.proceduremapping b
    where b.practice_descr = a."DISPLAY"
    and b.practice_descr = '%s'
    """
    
    cur = conn.cursor()
    curtaxo = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    prevbundleid  = -1
    bundle = {}
    for row in rows:
        if prevbundleid != row[0] or prevbundleid==-1:
            #print bundle
            #reduce hashes to arrays
            dse = []
            specl = []
            if bundle.has_key('dse_terms'):
                for k,v in bundle['dse_terms'].iteritems():
                    dse.append(v)
                bundle['dse_terms'] = dse
                for k,v in bundle['specialties'].iteritems():
                    specl.append(v)
                bundle['specialties'] = specl
                #zip through specialties and attach taxonmy illness terms to this bundle
                terms = []
                for sp in dse:
                    curtaxo.execute(sqltaxo % sp)
                    termrows = curtaxo.fetchall()
                    print len(termrows)
               
                    for t in termrows:
                        terms.append(t[0])
                bundle['terms'] = terms
                headers = {'Content-type': 'application/json'}
                  #   r = requests.post('http://172.22.101.104:8983/solr/provider/update?commit=true', data="[" + json.dumps(py) +"]", headers=headers)
                r = requests.post('http://localhost:8984/solr/gettingstarted_shard1_replica2/update?commit=true&overwrite=true', data="[" + json.dumps(bundle) +"]", headers=headers)
                       
                print bundle
                
            bundle = {}
            prevbundleid = row[0]
            bundle['bundleid'] = row[0]
            bundle['name'] = row[1]
            bundle['dse_terms'] = {}
            bundle['specialties'] = {}
        bundle['dse_terms'][row[2]] = row[2]
        bundle['specialties'][row[3]] = row[3]
        
   

def push_to_solr():

    #provdrkey, locations geocodes, hospital names, specialties,provdr is facility,major clasification,name,lastname,firstname
    conn = psycopg2cffi.connect("dbname='sandbox_rk' user='rogerk' port='5432' host='localhost' password='1yamadx7'")
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
make_json_providers()
#push_to_solr()
#bundle_search()


    
