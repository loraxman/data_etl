from job import StepQueueEntry
import s3util
import psycopg2
import urllib2
import json
import datetime as dt

def execstep(queue=None,step=None,step_pids=None):
    step.active=True
    
    for wstep in step.wait_steps:
        for wpid in step_pids[wstep]:
            wpid.join()


    conn = psycopg2.connect(step.connectdb)
    cur = conn.cursor()

    keenitems = \
        [{"url" : "http://api.keen.io/3.0/projects/526542b136bf5a71fb00000a/queries/extraction?api_key=932166a5a64da6dc30f31290104893bd4b732c25f062ae8f08cfbc19f54af7c9d18e0c4c43bd67ba80a09f5824131e64d8ef6d8820cb6a441bedea43c6dde069976b3d30c1c35a65f805bb388d3ea0a8c0c30a2e4ed51a7a1fb08dd8a999409157cb1c52f29582c68bd8dfd6a4c4a0ce&event_collection=procedure_searched&latest=20000"
         , "tablename" : "staging.keen_json_procedure_searched" },
        {"url" : "http://api.keen.io/3.0/projects/526542b136bf5a71fb00000a/queries/extraction?api_key=932166a5a64da6dc30f31290104893bd4b732c25f062ae8f08cfbc19f54af7c9d18e0c4c43bd67ba80a09f5824131e64d8ef6d8820cb6a441bedea43c6dde069976b3d30c1c35a65f805bb388d3ea0a8c0c30a2e4ed51a7a1fb08dd8a999409157cb1c52f29582c68bd8dfd6a4c4a0ce&event_collection=Signed_in&latest=20000"
        , "tablename" : "staging.keen_json_signed_up" },
        {"url" : "http://api.keen.io/3.0/projects/526542b136bf5a71fb00000a/queries/extraction?api_key=932166a5a64da6dc30f31290104893bd4b732c25f062ae8f08cfbc19f54af7c9d18e0c4c43bd67ba80a09f5824131e64d8ef6d8820cb6a441bedea43c6dde069976b3d30c1c35a65f805bb388d3ea0a8c0c30a2e4ed51a7a1fb08dd8a999409157cb1c52f29582c68bd8dfd6a4c4a0ce&event_collection=Signed_up&latest=20000"
        , "tablename" : "staging.keen_json_signed_in" },
        {"url" : "http://api.keen.io/3.0/projects/526542b136bf5a71fb00000a/queries/extraction?api_key=932166a5a64da6dc30f31290104893bd4b732c25f062ae8f08cfbc19f54af7c9d18e0c4c43bd67ba80a09f5824131e64d8ef6d8820cb6a441bedea43c6dde069976b3d30c1c35a65f805bb388d3ea0a8c0c30a2e4ed51a7a1fb08dd8a999409157cb1c52f29582c68bd8dfd6a4c4a0ce&event_collection=User_completes_provider_review&latest=20000"
        , "tablename" : "staging.keen_json_user_completes_provider_review" }
        ]
    
    idx=0
    for row in keenitems:       
        jsons = get_keen_content(row['url'])
        jsonresult = filter_by_date(conn,jsons,idx)
        add_keen_table(conn, row['tablename'],jsonresult)
        idx+=1
        
    sq = StepQueueEntry (step,"PASS")
    queue.put(sq) 
    
def get_keen_content(url):   
    rsp = urllib2.urlopen(url )
    raw =  rsp.read()
    raw = raw.encode('utf-8')
    raw = raw.replace("\t"," ")
    return json.loads(raw)


def filter_by_date(conn,jsons,idx):
    cur = conn.cursor()
    jsonresult = {}
    jsonresult['result'] = []
    qry=[]
    qry.append("select max(search_time)  from edw.search_fact")
    qry.append('select max(registration_time) from edw.registration_fact')
    qry.append('select max(return_time) from edw.return_fact')
    qry.append("select cast('1900-01-01 00:00:00' as timestamp)")
    
    cur.execute(qry[idx])
    maxdate = cur.fetchone()[0]
    for item in jsons['result']:
        trxdate = dt.datetime.strptime(item['keen']['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        if trxdate > maxdate:
            jsonresult['result'].append(item)
            
    return jsonresult


def add_keen_table(conn, tablename, j):
    sql = "insert into %s (payload) values ('%s')"
    sql = sql %  (tablename,json.dumps(j).replace("'",""))
    cur = conn.cursor()
    try:
        cur.execute(sql )
        conn.commit()
    except Exception as e:
        print e
        
