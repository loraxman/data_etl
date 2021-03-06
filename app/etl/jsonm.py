#make provider object at top.
import json
import threading
import multiprocessing
from multiprocessing import Process, Queue
import psycopg2cffi as pypg

#from nltk import bigrams
#import pandas as pd
#import psycopg2 as pypg

from time import sleep
import zlib
import requests
import time
import sys

from elasticsearch import Elasticsearch

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
        p = Process(target=service_single_provider_staging, args=(queue,i))
        threads.append(p)
        p.start()

def make_json_providers(etl_type='full'):
    
    #start threads
    start_consumers(5)
    conn3 = pypg.connect("dbname='sandbox_rk' user='rogerk' port='5432' host='localhost' password='1yamadx7'")
    #conn3 = pypg.connect("dbname='sandbox_rk' user='rogerk' port='9000' host='192.168.1.20' password='1yamadx7'")
    cur=conn3.cursor()
    #for full
    if etl_type == 'full':
        sql = " select distinct pin from provsrvloc " #   where pin = '0005938467'"
    else:
        sql = " select distinct pin, change_type from provlddelta"
  #  cur.execute("Select provdrkey from  h_provdr d  ")
    cur.execute(sql)
    
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
    
    #below should run at end to populate geo search table
    #need to change provider json and replace provdrkey with pin
    sql = """
    insert into psearch_vcprovdrlocn3
    select pin as 
    provdrkey ,
    svcl_longitude as provdrlocnlongitude ,
    svcl_latitude as provdrlocnlatitude ,
    ST_GeomFromText('POINT(' ||  cast(cast(svcl_longitude as float) *-1 as varchar) || ' ' || svcl_latitude || ')',4326),
    provdrjson->'specialities' ,
    provdrjson->'bundles'
    from provsrvloc a, provider_json b
    where a.pin = b.provdrid
    """
       
def service_single_provider(svcque,threadno): 
    if not sqlQ:
        fjson = open(dpath + "/jsonout"+str(threadno)+".dat" , "w")
    conn = pypg.connect("dbname='sandbox_rk' user='rogerk' port='5432' host='localhost0' password='1yamadx7'")
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
        '"base_net_id_no":' || '"' || to_json(cast (base_net_id_no as integer) ) || '" }',
        md5('{' ||  '"category_code":' || to_json(trim(category_code)) || ',' || 
        '"current_tier":' || to_json(current_tier)  || ',' ||
         case 
        when trim(master_category_description)='AEXCELP' then '"mstr_type":'||'"M"'
        when trim(master_category_description)='AEXCEL'  then '"mstr_type":'||'"C"'
        when strpos(master_category_description,'MULTI') > 0  then '"mstr_type":'||'"M"'
        when strpos(master_category_description,'CONCENTRIC') > 0  then '"mstr_type":'||'"C"'
        when strpos(master_category_description,'NATIONAL') > 0  then '"mstr_type":'||'"C"'
        else '"mstr_type":'||'"U"'
        end  ||  ',' 
        '"base_net_id_no":' || '"' || to_json(cast (base_net_id_no as integer) ) || '" }'),
                md5(
        '"base_net_id_no":' || '"' || to_json(cast (base_net_id_no as integer) ) || '" }')
        from staging.srvgrpprovass where pin = '%s';        """
        cur2.close()
        cur2=conn.cursor()
        start_time = time.time()
        cur2.execute(sql % (provider['provdrid']))
        rowsnet = cur2.fetchall()
  #      cols = gettabcols(cur2.description,"*")               
        provider['networks'] = []
        provider['netwkhashes'] = []
        provider['netwkcathashes'] = []
        for rowloc in rowsnet:
           # providernetworks={}
            providernetworks=json.loads(rowloc[0])
            provider['networks'].append(providernetworks)
            provider['netwkhashes'].append(rowloc[1])
            provider['netwkcathashes'].append(rowloc[2])
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
#            sql = "insert into provider_json_compress (provdrid, provdrkey,provider_json_ztext) values ('%s','%s',%s) " % (provider['provdrid'],provider['provdrkey'],pypg.Binary(jsoncompressed))
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

#-------------------USE ONLY STAGING!
def service_single_provider_staging(svcque,threadno): 
    if not sqlQ:
        fjson = open(dpath + "/jsonout"+str(threadno)+".dat" , "w")
    conn = pypg.connect("dbname='sandbox_rk' user='rogerk' port='5432' host='localhost' password='1yamadx7'")
    #conn = pypg.connect("dbname='sandbox_rk' user='rogerk' port='9000' host='192.168.1.20' password='1yamadx7'")

       
        
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
        cur5=conn.cursor()
        
        sql = """
         SELECT distinct pin as provdrkey,
         pin as provdrid,
         name as provdrname,
         null as provdrtin,
         salutation as provdrsalutation,
         last_name as provdrlastname,
         first_name as provdrfirstname,
         middle_name as provdrmiddlename,
         gender_description as provdrgender,
         birth_date as provdrbirthdate,
         primary_degree_desc as provdrprimarydegreedescr,
         secondary_degree_desc as provdrsecondarydegreedescr,
         provider_type as provdrtype,
         case when c."TYPE_CLASS_CD" = 'F' then 'Y'
            else 'N'
            end as provdrisfacilityflag,
            major_classification as provdrmajorclassification,
            med_dent_ind as provdrmeddentalind,
            designation_code as provdrdesignationcode
            from provsrvloc a , provider_type c where trim(provider_type) = trim(c."PROVIDER_TYPE_CD")
            and pin = '%s' 
        """
        #above query produced memory error at some point
        #instead of outer join. If we get no rows just fill in 'N' ot facility
        sqlalt = """
         SELECT distinct pin as provdrkey,
         pin as provdrid,
         name as provdrname,
         null as provdrtin,
         salutation as provdrsalutation,
         last_name as provdrlastname,
         first_name as provdrfirstname,
         middle_name as provdrmiddlename,
         gender_description as provdrgender,
         birth_date as provdrbirthdate,
         primary_degree_desc as provdrprimarydegreedescr,
         secondary_degree_desc as provdrsecondarydegreedescr,
         provider_type as provdrtype,
         'N' as provdrisfacilityflag,
            major_classification as provdrmajorclassification,
            med_dent_ind as provdrmeddentalind,
            designation_code as provdrdesignationcode
         
            from provsrvloc a 
            where pin = '%s'
        """
        # for non update run do below
        sqlcheck = " select count(*) from provider_loc_search where provdrkey = '%s' "
        cur5.execute(sqlcheck % provdrkey)
        row  = cur5.fetchone()
        if row[0] > 0:
            continue
        try : 
            cur5.execute(sql % (provdrkey))
        except:
            print sql
        try:
            row = cur5.fetchone()
        except:        
            print "none to fetch"
        if row == None:
            cur5.execute(sqlalt  % (provdrkey) )
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
                try:
                    provider[v] = row[k]
                except:
                    print "skpped error...row was probably null"

              #provider languages
        sql = """
        select distinct language_description as langname, cast (service_location_number as int) from provlang 
        where pin = '%s' 
        """ 
        cur2=conn.cursor()
        cur2.execute(sql % (provider['provdrid']))
        rowsloc = cur2.fetchall()
        provider['languages'] = []
        cols = gettabcols(cur2.description,"lang")       
        provider_langs = {}     
        for rowloc in rowsloc:
            if not provider_langs.has_key(rowloc[1]):
                provider_langs[rowloc[1]] = []
            provider_langs[rowloc[1]].append(rowloc[0].strip())
            providerlangs={}
            for k,v in cols.items():
                try:
                    providerlangs[v] = rowloc[k].strip()
                except:
                    providerlangs[v] = rowloc[k]
                    
        
            provider['languages'].append(providerlangs)

        provider['specialities'] = []
        sql = """
        

       select 
        '{' || '"name":' || to_json(trim(practice_description) ) ||','
                 '"specialtyId":' || to_json(trim(practice_code)) || ',"isPrimary":' || to_json(trim(prim_spec_ind)) || ',"typeCode":' 
                 || to_json(trim(practice_type)) 
                 || ',"serviceLocationNumber":' || to_json(service_location_no) ||
                 '}'
                 ,
                 trim(practice_code)
                , cast(service_location_no as integer)
                from provsrvlocspec 
                 where pin = '%s'

         """
        
        cur3=conn.cursor()
#            cur3.execute("Select * from vcpractspecl c,m_vcprovdrlocnpractspecl e where e.provdrlocnkey = %s and e.practspeclkey = c.practspeclkey" % (providerlocn['provdrlocnkey']))
        start_time = time.time() 
        cur3.execute(sql % (provider['provdrid']))
        #print("--- %s Specialty seconds ---" % (time.time() - start_time)) 
        rowspract = cur3.fetchall()
        provider_specl= {}
        for rowpract in rowspract:
            
            if not provider_specl.has_key(rowpract[2]):
                provider_specl[rowpract[2]] = []
                provider_specl[rowpract[2]].append([])  #the json ret array
                provider_specl[rowpract[2]].append([])  #the khash array
                
            provider_specl[rowpract[2]][0].append(json.loads(rowpract[0]))
            provider_specl[rowpract[2]][1].append((rowpract[1]))

            practspecl = json.loads(rowpract[0])
            provider['specialities'].append(practspecl)   
            
        cur3.close()
        
        #print("--- %s Specialty seconds ---" % (time.time() - start_time)) 
        #bundle sql    

        sql = """
          select distinct bundleid,cast(service_location_no as integer)      from   
           provsrvlocspec b,
         
           CBOR i
           where   
        b.practice_code = i.practicecode
           and b.pin = '%s'
           union 
             select distinct bundleid ,cast(service_location_no as integer)       from   
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
        curcbor = conn.cursor()
        start_time=time.time()
        cur3.execute(sql % (provider['provdrid'],provider['provdrid'], (provider['provdrid'])))

        rowspract = cur3.fetchall()
        bundles = []   
        provider_bundle= {}
     
        for rowpract in rowspract:
            cols = gettabcols(cur3.description,"bundleid")
            
            if not provider_bundle.has_key(rowpract[1]):
                provider_bundle[rowpract[1]] = []
                
            bundlehash = {}
            bundlehash[rowpract[0]] = []           
            #create the items for doing "Procedures" in payload. Takes the bundle Id and gets all CBOR rows
            cborsql = "select * from CBOR where bundleid=%s"
            curcbor.execute(cborsql % rowpract[0])  
            colscbor = gettabcols(curcbor.description,"*")
            for crow in curcbor.fetchall():
                cbormap = {}
                for k,v in colscbor.items():
                    try:
                        cbormap[v] = crow[k].strip()
                    except:
                        cbormap[v] = crow[k]
                
                bundlehash[rowpract[0]].append(cbormap)
                
            provider_bundle[rowpract[1]].append(bundlehash)

            for k,v in cols.items():
                try :
                    bundles.append( rowpract[k].strip())
                except:
                    bundles.append( rowpract[k])
           

        provider['bundles'] =  bundles  
        #print("--- %s Bundles seconds ---" % (time.time() - start_time)) 
            
        #provider locns            
        cur2=conn.cursor()
        start_time = time.time()
        sql = """
        SELECT 999 as provdrlocnkey,
        service_location_number as provdrlocnid,
        primary_serv_loc_ind as provdrlocnprimarylocnflag,
        addr_print_ind as provdrlocnaddressprintflag,
        addr_nap_only_ind as provdrlocnaddressnaponlyflag,
        addr_cust_only_ind provdrlocnaddresscustonlyflag,
        primary_phone_no as provdrlocnprimaryphone,
        secondary_phone_no as provdrlocnsecondaryphone,
        fax_no as provdrlocnfaxphone,
        npi as provdrlocnnpi,
        hearing_product_code as provdrlocnhearingprodcode,
        nabp_number as provdrlocnnabpnumber,
        svcl_building as provdrlocnbuilding,
        svcl_street1 as provdrlocnstreet1,
        svcl_street2 as provdrlocnstreet2,
        svcl_street3 as provdrlocnstreet3,
        svcl_street4 as provdrlocnstreet4,
        svcl_street5 as provdrlocnstreet5,
        svcl_city as provdrlocncity,
        svcl_state as provdrlocnstate,
        svcl_zip as provdrlocnzip,
        svcl_xzip as provdrlocnzipext,
        svcl_county as provdrlocncounty,
        svcl_country_cd as provdrlocncountrycode,
        svcl_handicap as provdrlocnhandicapflag,
        svcl_latitude as provdrlocnlatitude,
        svcl_longitude as provdrlocnlongitude,
        addr_print_ind as provdrlocndisplayflag,
        accept_new_patients_ind as provdrlocnacceptingnewpatients
        from provsrvloc 
        where pin = '%s'
        """
      #  cur2.execute("Select * from mgeo_vcprovdrlocn b where b.provdrkey = %s " % (provider['provdrkey']))
        cur2.execute(sql % (provider['provdrkey']))
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
                    
                sqlupd = """
                 update provider_loc_search
                 set provdrkey = %s ,
                 provdrlocnlongitude = %s, 
                 provdrlocnlatitude  = %s, 
                 geom = ST_GeomFromText('POINT(' ||  cast(cast('%s' as float) *-1 as varchar) || ' ' || %s || ')',4326),
                 specialties = '%s',
                 bundles = '%s'
                 where pin = '%s'
                 and service_location_number = '%s'
                """
                
               # curloc=conn.cursor()
                #curloc.execute(sql % (provider['provdrkey'], providerlocn['provdrlocnlongitude'], providerlocn['provdrlocnlatitude'],\
                #      providerlocn['provdrlocnlongitude'], providerlocn['provdrlocnlatitude'], json.dumps(provider['specialities']),\
                # json.dumps(provider['bundles']), provider['provdrkey'],  providerlocn['provdrlocnid']))
                sqlins = """
                 insert into provider_loc_search (provdrkey,,service_location_number, provdrlocnlongitude,provdrlocnlatitude,
                 geom, specialties, bundles) values 
                 ('%s','%s', %s,%s, ST_GeomFromText('POINT(' ||  cast(cast('%s' as float) *-1 as varchar) || ' ' || %s || ')',4326),
                 '%s,'%s')"
                 
              
                """
                  
            providerlocns.append(providerlocn)
        
        provider['locations'] = providerlocns   
        
        
        #print("--- %s provddrlocn seconds ---" % (time.time() - start_time)) 
        
        sql = """
        select b.name as facilityName, 
        admit_privileges as admitPrvileges ,
        affil_status_code as affilStatusCode,
        cast(a.service_location_number as int),
        '' as affilFlags,
        a.hospital_pin as hospitalPin
        from hospaff a, provsrvloc b
        where a.pin = '%s'
        and a.hospital_pin = b.pin
        """
        cur2.close()
        cur2=conn.cursor()
        start_time = time.time()
        cur2.execute(sql % (provider['provdrid']))
        rowshosp = cur2.fetchall()
        provider['hospitals'] = []
        provider_hosp = {}
        for rowloc in rowshosp:
            providerhospitals={}
            
           
            cols = gettabcols(cur2.description,"a")            
            for k,v in cols.items():
                try:
                    providerhospitals[v] = rowloc[k].strip()
                except:
                    providerhospitals[v] = rowloc[k]
                    
            provider['hospitals'].append(providerhospitals)
            #initialize network serviceloc hash
            if not provider_hosp.has_key(rowloc[3]):
                provider_hosp[rowloc[3]] = []
                
            provider_hosp[rowloc[3]].append(providerhospitals)

#        print("--- %s Hosp seconds ---" % (time.time() - start_time))

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
        '"base_net_id_no":' || '"' || to_json(cast (base_net_id_no as integer) ) || '" }',
        md5('{' ||  '"category_code":' || to_json(trim(category_code)) || ',' || 
        '"current_tier":' || to_json(current_tier)  || ',' ||
         case 
        when trim(master_category_description)='AEXCELP' then '"mstr_type":'||'"M"'
        when trim(master_category_description)='AEXCEL'  then '"mstr_type":'||'"C"'
        when strpos(master_category_description,'MULTI') > 0  then '"mstr_type":'||'"M"'
        when strpos(master_category_description,'CONCENTRIC') > 0  then '"mstr_type":'||'"C"'
        when strpos(master_category_description,'NATIONAL') > 0  then '"mstr_type":'||'"C"'
        else '"mstr_type":'||'"U"'
        end  ||  ',' 
        '"base_net_id_no":' || '"' || to_json(cast (base_net_id_no as integer) ) || '" }'),
                md5(
        '"base_net_id_no":' || '"' || to_json(cast (base_net_id_no as integer) ) || '" }'),
        cast(service_location_no as integer)
        from staging.srvgrpprovass where pin = '%s';        """
        cur2.close()
        cur2=conn.cursor()
        start_time = time.time()
        cur2.execute(sql % (provider['provdrid']))
        rowsnet = cur2.fetchall()
  #      cols = gettabcols(cur2.description,"*")               
        provider['networks'] = []
        provider['netwkhashes'] = []
        provider['netwkcathashes'] = []
        provider_networkloc = {}
        for rowloc in rowsnet:
            # providernetworks={}
            netwks = json.loads(rowloc[0])
            providernetworks=netwks

            #initialize network serviceloc hash
            if not provider_networkloc.has_key(rowloc[3]):
                 provider_networkloc[rowloc[3]] = []
                 provider_networkloc[rowloc[3]].append([])  #the network array
                 provider_networkloc[rowloc[3]].append([])  #the networkhash array
                 provider_networkloc[rowloc[3]].append([])  #the networkcathash array
                 
            provider_networkloc[rowloc[3]][0].append(netwks)
            provider_networkloc[rowloc[3]][1].append((rowloc[1]))
            provider_networkloc[rowloc[3]][2].append((rowloc[2]))
       
            
            provider['networks'].append(providernetworks)
            provider['netwkhashes'].append(rowloc[1])
            provider['netwkcathashes'].append(rowloc[2])
        #print("--- %s Network seconds ---" % (time.time() - start_time))
        #provder quality program
        sql = """
        select distinct flag_code as flagCode, flag_description as qualityprogramdescr,cast (service_location_no as int)
        from provsrvlocflg where pin = '%s'
        """
        cur2.close()
        cur2=conn.cursor()
        start_time = time.time()
        cur2.execute(sql % (provider['provdrkey']))
        rowsnet = cur2.fetchall()
        provider['programs'] = []
        provider_flags = {}
        for rowloc in rowsnet:
            providerprograms={}
            cols = gettabcols(cur2.description,"a")            
            for k,v in cols.items():
                try:
                    providerprograms[v] = rowloc[k].strip()
                except:
                    providerprograms[v] = rowloc[k]
                if not provider_flags.has_key(rowloc[2]):
                     provider_flags[rowloc[2]] = []
                provider_flags[rowloc[2]].append(rowloc[0].strip())
                   
            provider['programs'].append(providerprograms)


        #call HL mapping
        hl_locs = map_to_HLformat(provider)
        
        #her we could no map each new hl_loc to a single row in our geo search table
        
        add_provdr_loc_table(conn, hl_locs,provider,provider_networkloc,provider_specl,provider_langs,provider_hosp,provider_flags,provider_bundle)
        
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
            sql = "update provider_json3 set provdrjson = '%s' where provdrkey = %s" % (provdrjson,provider['provdrkey'])
         #   curjson.execute (sql)
            if curjson.rowcount == 0:
                sql = "insert into provider_json3 (provdrid, provdrkey,provdrjson) values ('%s','%s','%s') " % (provider['provdrid'],provider['provdrkey'],provdrjson)
        #        curjson.execute (sql)
           #curjson.execute("commit")
#            sql = "insert into provider_json_compress (provdrid, provdrkey,provider_json_ztext) values ('%s','%s',%s) " % (provider['provdrid'],provider['provdrkey'],pypg.Binary(jsoncompressed))
  #          curjson.execute (sql)
            #svcque.task_done()
            cnt += 1
            if cnt%10 == 0:
                curjson.execute("commit")
                start_trx = True
                if cnt%100 == 0:
                    print "thread %d  at: %d " % (threadno,cnt)
                #lets try to drop and redo connect every 1000 times
                if True:
                   conn.close()
                   #conn = pypg.connect("dbname='sandbox_rk' user='rogerk' port='9000' host='192.168.1.20' password='1yamadx7'")
                   conn = pypg.connect("dbname='sandbox_rk' user='rogerk' port='5432' host='localhost' password='1yamadx7'")
                   curjson = conn.cursor()
        else:
            fjson.write("%s|%s|%s\n" % (provider['provdrid'],provider['provdrkey'],provdrjson))
            cnt += 1
            if cnt % 100 == 0:
                print "thread %d  at: %d" % (threadno,cnt)
            #svcque.task_done()




def add_provdr_loc_table(conn, hl_locs,provider,provider_networkloc,provider_specl, provider_langs,provider_hosp,provider_flags,provider_bundle):
    sqlupd = """
     update provider_loc_search
     set provdrkey = %s ,
     service_location_number = '%s',
     provdrlocnlongitude = %s, 
     provdrlocnlatitude  = %s, 
     geom = ST_GeomFromText('POINT(' ||  cast(cast('%s' as float) *-1 as varchar) || ' ' || %s || ')',4326),
     bundles = '%s',
     networks = '%s',
     netwkhashes = '%s',
     netwkcathashes = '%s',
     specialties = '%s',
     provdrjson = '%s',
     procedure_zip = pgp_sym_encrypt('%s','abc','compress-algo=1, cipher-algo=aes128')
     where provdrkey = '%s'
     and service_location_number = '%s'
    """
    sqlins = """
     insert into provider_loc_search (provdrkey,service_location_number, provdrlocnlongitude,provdrlocnlatitude,
     geom,  bundles,networks,netwkhashes,netwkcathashes,specialties,provdrjson,procedure_zip) values 
     ('%s','%s', %s,%s, ST_GeomFromText('POINT(' ||  cast(cast('%s' as float) *-1 as varchar) || ' ' || %s || ')',4326),
     '%s','%s','%s','%s','%s','%s',pgp_sym_encrypt('%s','abc','compress-algo=1, cipher-algo=aes128'))
     
  
    """
    curloc=conn.cursor()

    idx = 0
    
    for loc in hl_locs:
        provdrjson = loc
        netkey = provider['locations'][idx]['provdrlocnid']
        #might not have a service grouping for every location -- handle that and default empty arrays
        if provider_networkloc.has_key(int(netkey)):
            netwksjson = json.dumps(provider_networkloc[int(provider['locations'][idx]['provdrlocnid'])][0])
            netwkshashjson =  json.dumps(provider_networkloc[int(provider['locations'][idx]['provdrlocnid'])][1])
            netwkscathashjson =  json.dumps(provider_networkloc[int(provider['locations'][idx]['provdrlocnid'])][2])
            #build up the networks,categories and tier strings
            netwks = set([n['base_net_id_no'] for n in provider_networkloc[int(provider['locations'][idx]['provdrlocnid'])][0]])
            cats = set([n['category_code'] for n in provider_networkloc[int(provider['locations'][idx]['provdrlocnid'])][0]])
            tiers = set([n['current_tier'] for n in provider_networkloc[int(provider['locations'][idx]['provdrlocnid'])][0]])
            netwkstr =  ",".join(netwks)
            catsstr =  ",".join(cats)
            tiersstr =  ",".join(tiers)

        else:
            netwksjson = "[]"
            netwkshashjson = "[]"
            netwkscathashjson = "[]"
            netwkstr = ""
            catsstr = ""
            tiersstr = ""
            
        if provider_specl.has_key(int(netkey)):
            specljson = json.dumps(provider_specl[int(provider['locations'][idx]['provdrlocnid'])][0])
            speclhashjson =  json.dumps(provider_specl[int(provider['locations'][idx]['provdrlocnid'])][1])
            provdrjson['specialties'] = provider_specl[int(provider['locations'][idx]['provdrlocnid'])][0]
        else:
            specljson = ""
            speclhashjson = "[]"
            
        if provider_langs.has_key(int(netkey)):            
            langstr = ''
            for l in provider_langs[int(netkey)]:
                langstr +=  l.strip() + ','
            langstr = langstr.strip(",") 
        else:
            langstr = ''
        provdrjson['languageDescription'] = langstr
      
      
        if provider_hosp.has_key(int(netkey)):
            provdrjson['hospitalAffiliations'] = provider_hosp[int(netkey)]
            
        if provider_flags.has_key(int(netkey)):
            pflags = ",".join(set(provider_flags[int(netkey)]))
            provdrjson['flagCode'] = pflags
 
        if provider_bundle.has_key(int(netkey)):
            #get keys of bundle has
            bid = []
            for bn in provider_bundle[int(netkey)]:
                bid.append(bn.keys()[0])
            bundlstr = json.dumps(bid)
        else:
            bundlstr = "[]"   
            
        procedures = generate_procedure_json(provider_bundle, int(netkey))
            
        provdrjson['categoryCode'] = catsstr
        provdrjson['network'] = netwkstr
        provdrjson['tier'] = tiersstr
        provdrjson['serviceLocationNumber'] = netkey
        provdrjson['ioe'] = False
        provdrjson['ioq'] = False
        #commented out for zip storage
      #  provdrjson['procedureCode'] = procedures
         
         
        curloc.execute(sqlupd % (provider['provdrkey'],provider['locations'][idx]['provdrlocnid'], provider['locations'][idx]['provdrlocnlongitude'], provider['locations'][idx]['provdrlocnlatitude'],\
                provider['locations'][idx]['provdrlocnlongitude'], provider['locations'][idx]['provdrlocnlatitude'], \
                
                bundlstr,  \
                netwksjson,\
                netwkshashjson,\
                netwkscathashjson ,\
                speclhashjson,\
                json.dumps(provdrjson).replace("'","''"),\
                json.dumps(procedures),\
                provider['provdrkey'],provider['locations'][idx]['provdrlocnid']))
        
        if curloc.rowcount == 0:   
            try :     
                curloc.execute(sqlins % (provider['provdrkey'],provider['locations'][idx]['provdrlocnid'], provider['locations'][idx]['provdrlocnlongitude'], provider['locations'][idx]['provdrlocnlatitude'],\
                    provider['locations'][idx]['provdrlocnlongitude'], provider['locations'][idx]['provdrlocnlatitude'], 
                  
                    bundlstr,  \
                    netwksjson,\
                    netwkshashjson,\
                    netwkscathashjson, \
                    speclhashjson, \
                    json.dumps(provdrjson).replace("'","''"),\
                    json.dumps(procedures),\
                    ))
            except:
                print sqlins % (provider['provdrkey'],provider['locations'][idx]['provdrlocnid'], provider['locations'][idx]['provdrlocnlongitude'], provider['locations'][idx]['provdrlocnlatitude'],\
                        provider['locations'][idx]['provdrlocnlongitude'], provider['locations'][idx]['provdrlocnlatitude'], 
                        
                        json.dumps(provider['bundles']),  \
                                netwksjson,\
                                netwkshashjson,\
                                netwkscathashjson, \
                                speclhashjson, \
                                json.dumps(provdrjson).replace("'","''"),\
                                 json.dumps(procedures)\
                                )
        idx += 1
       # print idx
        if idx % 100  == 0:
            print "Locs:%d" % idx
            curloc.execute("commit")
 
 
def generate_procedure_json(provider_bundle,lockey):
    procedures = []
    if not provider_bundle.has_key(lockey):
        return procedures

    for bundles in provider_bundle[lockey]:
        for k,blist in bundles.iteritems():
            for b in blist:
                procedure = {}
                procedure['code'] = b['bundleid']
                procedure['description'] = b['bundlename']
                procedure['minAge'] = int(b['uk1'][0:4])
                procedure['maxAge'] = int(b['uk1'][4:8])
                procedure['gender'] = b['uk1'][9:10]
                procedure['constitude_code'] = b['uk1'][10:11]
                procedures.append(procedure)
    return procedures
        
        
def map_to_HLformat(provider):
    pass
    #transform 'locations' array and denormalize provider
    hl_locs = map_to_HLlocations(provider)
    #denormalize provider
    hl_locs = map_to_HLprovider(hl_locs,provider)
    return hl_locs
    
    
def map_to_HLprovider(hl_locs,provider):
    translate = {"provdrprimarydegreedescr":"primaryDegreeDesc",
        "provdrfirstname":"firstName",
        "provdrlastname":"lastName",
        "provdrgender":"gender",
        "provdrpin":"pin",
        "provdrmiddlename":"middleName",
        "provdrdesignationcode":"designationCode",
        "provdrname" : "name",
        "provdrisfacilityflag":"provdrisfacilityflag"
        
    }
        
    sql = """
         SELECT distinct pin as provdrkey,
         pin as provdrid,
         name as provdrname,
         null as provdrtin,
         salutation as provdrsalutation,
         last_name as provdrlastname,
         first_name as provdrfirstname,
         middle_name as provdrmiddlename,
         gender_description as provdrgender,
         birth_date as provdrbirthdate,
         primary_degree_desc as provdrprimarydegreedescr,
         secondary_degree_desc as provdrsecondarydegreedescr,
         provider_type as provdrtype,
         case when c."TYPE_CLASS_CD" = 'F' then 'Y'
            else 'N'
            end as provdrisfacilityflag,
            major_classification as provdrmajorclassification,
            med_dent_ind as provdrmeddentalind,
            designation_code as provdrdesignationcode
            from provsrvloc a, provider_type c
            where pin = '%s'
            and trim(a.provider_type) = trim(c."PROVIDER_TYPE_CD");
    """
    hl_newlocs = []
    for loc in hl_locs:
        for k,v in provider.iteritems():
           
            if translate.has_key(k):
                mapper = translate[k]
                loc[mapper] = v
         
        if loc['provdrisfacilityflag'] == 'Y':          
            loc['facilityName']  = loc['name'] 
            del loc['gender']
            del loc['firstName']
            del loc['lastName']
            del loc['middleName']
        del loc['name']
        hl_newlocs.append(loc)
     
    return hl_newlocs


def map_to_HLlocations(provider):
    """
     addr_nap_only_ind as provdrlocnaddressnaponlyflag,
     addr_cust_only_ind provdrlocnaddresscustonlyflag,
     primary_phone_no as provdrlocnprimaryphone,
     secondary_phone_no as provdrlocnsecondaryphone,
     fax_no as provdrlocnfaxphone,
     npi as provdrlocnnpi,
     hearing_product_code as provdrlocnhearingprodcode,
     nabp_number as provdrlocnnabpnumber,
     svcl_building as provdrlocnbuilding,
     svcl_street1 as provdrlocnstreet1,
     svcl_street2 as provdrlocnstreet2,
     svcl_street3 as provdrlocnstreet3,
     svcl_street4 as provdrlocnstreet4,
     svcl_street5 as provdrlocnstreet5,
     svcl_city as provdrlocncity,
     svcl_state as provdrlocnstate,
     svcl_zip as provdrlocnzip,
     svcl_xzip as provdrlocnzipext,
     svcl_county as provdrlocncounty,
     svcl_country_cd as provdrlocncountrycode,
     svcl_handicap as provdrlocnhandicapflag,
     svcl_latitude as provdrlocnlatitude,
     svcl_longitude as provdrlocnlongitude,
     addr_print_ind as provdrlocndisplayflag
     from provsrvloc 
     """
     
    translate = {"service_location_number":"serviceLocationNumber",
       "provdrlocncounty":"county",
       "provdrlocnprimaryphone":"primaryPhone",
       "provdrlocncity":"city",
       "provdrlocnstate":"state",
       "provdrlocnaddressnaponlyflag":"nap",
       "provdrlocnaddressprintflag":"print",
       "provdrlocnfaxphone":"fax",
       "provdrlocnnpi":"npi",
       "provdrlocnstreet1":"street1",
       "provdrlocnstreet2":"street2",
       "provdrlocnstreet3":"street3",
       "provdrlocnstreet4":"street4",
       "provdrlocnstreet5":"street5",
       "provdrlocnzip":"zipCode",
       "provdrlocnprimaryphone":"primaryPhone",
       "provdrlocnacceptingnewpatients":"acceptingNewPatients",
       "provdrdesignationcode":"designationCode"
       }
       
    hl_locs = []
    for locn in provider['locations']:
        hl_loc = {}
        for k,v in locn.iteritems():
            if translate.has_key(k):
                mapper = translate[k]
                hl_loc[mapper] = v
        #have to build lat/lng hl string
        #"latLong": "36.083586,-115.10092800000001",
        hl_loc['latLong'] = "%s,%s" % (locn['provdrlocnlatitude'] ,locn['provdrlocnlongitude'])
        #always set 'Par' to 'Y' since we don't bring back any non-par
        hl_loc['par'] = 'Y'
        #distance is undefined yet put placemarker
        hl_loc["distance"] =  "0.0"
        hl_loc['pin'] = provider['provdrid']
        hl_locs.append(hl_loc)
        
    return hl_locs


    
sqlQ = True 
dpath = "."         
make_json_providers()


    
