#make provider object at top.
import json
import threading
import multiprocessing
from multiprocessing import Process, Queue
import psycopg2
#from nltk import bigrams
import pandas as pd
#import psycopg2

from time import sleep
import zlib
import requests
import time
import sys

from elasticsearch import Elasticsearch



def generate_cpt_bigrams(conn):
    sqlcpt = """
    select bundleid, bundlename
    from CBOR a
    where cpt_code = '%s'
    """
    
    cpt = pd.read_sql('select "PRCDR_CD","PRCDR_DSCRPTN" from cpt_codes',conn)
   
    for index,item in cpt.iterrows():
        #index is the 
        #    print item['PRCDR_DSCRPTN']
        if item['PRCDR_DSCRPTN'] :
            istr = item['PRCDR_DSCRPTN'].strip().lower()
            descrbigrams = bigrams(istr.split(" "))
            for gram in descrbigrams:
                print gram[0] + ' ' + gram[1]
                break


def bundle_search():
    conn = psycopg2.connect("dbname='sandbox_rk' user='rogerk' port='5432' host='localhost' password='1yamadx7'")
    sql = """
    select bundleid,bundlename,practice_descr as specialty, procedure_main as dse_term ,cpt_code from CBOR b
    LEFT OUTER JOIN  staging.proceduremapping  a  on a.practice_code = b.practicecode
    order by bundleid
    """    
    sqltaxo = """
    select distinct \"QUERY\" from 
    condition_specialty a,
    staging.proceduremapping b
    where b.practice_descr = a."DISPLAY"
    and translate(b.practice_descr,'''','') = '%s'
    """
    sqlcpt = """
    select lower(\"PRCDR_DSCRPTN\") from 
    cpt_codes where \"PRCDR_CD\" = '%s'
    """
    
    cur = conn.cursor()
    curtaxo = conn.cursor()
    curcpt = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    prevbundleid  = -1
    bundle = {}
    for row in rows:
        if prevbundleid != row[0] or prevbundleid==-1:
            #print bundle
            #reduce hashes to arrays
            dse = []
            dse_lower = []
            specl = []
            specl_lower=[]
            if bundle.has_key('dse_terms'):
                for k,v in bundle['dse_terms'].iteritems():
                    if not v:
                        continue
                    dse.append(v)
                    dse_lower.append(v.lower())
                bundle['dse_terms'] = dse
                bundle['dse_terms_lower'] = dse_lower
                for k,v in bundle['specialties'].iteritems():
                    if not v:
                        continue
                    specl.append(v)
                    specl_lower.append(v.lower())
                bundle['specialties'] = specl
                bundle['specialties_lower'] = specl_lower
                #zip through specialties and attach taxonmy illness terms to this bundle
                terms = []
                for sp in dse:
                    curtaxo.execute(sqltaxo % sp.replace("'",""))
                    termrows = curtaxo.fetchall()
                    print len(termrows)
               
                    for t in termrows:
                        terms.append(t[0])
                bundle['terms'] = terms
                curcpt.execute(sqlcpt % row[4])
                rowcpt = curcpt.fetchone()
                if rowcpt:
                    bundle['cpt_descr'] = rowcpt[0]
                headers = {'Content-type': 'application/json'}
                #   r = requests.post('http://172.22.101.104:8983/solr/provider/update?commit=true', data="[" + json.dumps(py) +"]", headers=headers)
     #           r = requests.post('http://172.22.100.88:8983/solr/gettingstarted_shard1_replica2/update?commit=true&overwrite=true', data="[" + json.dumps(bundle) +"]", headers=headers)
                r = requests.post('http://localhost:8983/solr/gettingstarted_shard1_replica2/update?commit=true&overwrite=true', data="[" + json.dumps(bundle) +"]", headers=headers)
                
                print bundle
                
            bundle = {}
            prevbundleid = row[0]
            bundle['bundleid'] = row[0]
            bundle['name'] = row[1]
            bundle['lowered_name'] = row[1].lower() 
            bundle['dse_terms'] = {}
            bundle['specialties'] = {}
            bundle['dse_terms_lower'] = {}
            bundle['specialties_lower'] = {}
        bundle['dse_terms'][row[3]] = row[3]
        bundle['specialties'][row[2]] = row[2]
        if row[3]:
            bundle['dse_terms_lower'][row[3]] = row[3].lower()
        if row[2]:
            bundle['specialties_lower'][row[2]] = row[2].lower()
        
   

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
        r = requests.post('http://172.22.101.104:8983/solr/provider/update?commit=true', data="[" + json.dumps(py) +"]", headers=headers)
        #r = requests.post('http://localhost:8984/solr/provider/update?commit=true&overwrite=true', data="[" + json.dumps(solrhash) +"]", headers=headers)
        #print response.read()
        idx += 1
        if idx%1000 == 0:
            print idx
    
    
def elastic_bundle():
    es = Elasticsearch(hosts = ['172.22.100.88'])
    if not ( es.indices.exists('typeahead')):
        es.indices.exists('typeahead')
        request_body = {
        "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
        }
        }
        res = es.indices.create(index = 'typeahead', body=request_body)    
    
  
    bulk_data = [] 
    conn = psycopg2cffi.connect("dbname='sandbox_rk' user='rogerk' port='5432' host='localhost' password='1yamadx7'")
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
    docid = 1
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
                
               # print bundle
                res = es.index(index="typeahead", doc_type='bundle', id=docid, body=json.dumps(bundle))
                docid += 1
                print res
                
            bundle = {}
            prevbundleid = row[0]
            bundle['bundleid'] = row[0]
            bundle['name'] = row[1]
            bundle['dse_terms'] = {}
            bundle['specialties'] = {}
        bundle['dse_terms'][row[2]] = row[2]
        bundle['specialties'][row[3]] = row[3]
    es.indices.refresh(index="typeahead")
    
    
    
sqlQ = True 
dpath = "."         

#push_to_solr()
bundle_search()
#elastic_bundle()



