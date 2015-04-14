from job import StepQueueEntry
import s3util
import psycopg2
import urllib2
import json

def execstep(queue=None,step=None,step_pids=None):
    step.active=True
    
    for wstep in step.wait_steps:
        for wpid in step_pids[wstep]:
            wpid.join()


    conn = psycopg2.connect(step.connectdb)
    cur = conn.cursor()
    rsp = urllib2.urlopen("http://api.keen.io/3.0/projects/526542b136bf5a71fb00000a/queries/extraction?api_key=932166a5a64da6dc30f31290104893bd4b732c25f062ae8f08cfbc19f54af7c9d18e0c4c43bd67ba80a09f5824131e64d8ef6d8820cb6a441bedea43c6dde069976b3d30c1c35a65f805bb388d3ea0a8c0c30a2e4ed51a7a1fb08dd8a999409157cb1c52f29582c68bd8dfd6a4c4a0ce&event_collection=Signed_up" )
    raw =  rsp.read()
    raw = raw.encode('utf-8')
    raw = raw.replace("\t"," ")
    j = json.loads(raw)
    sql = "insert into testkeen (recval) values ('%s')"
    sql = sql %  json.dumps(j).replace("'","")
    cur = conn.cursor()
    try:
        cur.execute(sql )
        conn.commit()
    except Exception as e:
        print e

    sq = StepQueueEntry (step,"PASS")
    queue.put(sq)   
