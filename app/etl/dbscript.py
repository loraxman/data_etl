import psycopg2
from job import StepQueueEntry
import multiprocessing


def execstep(queue=None, script=None,step=None):

	sql = open(script).read()
	conn = psycopg2.connect("dbname='claims' user='roger@wellmatchhealth.com' port='5439' host='dw-nonprod.healthagen.com' password='S6JB3ZjG7FMN'")
	cur = conn.cursor()
	
	cur.execute(sql)
	rowcnt = cur.rowcount
	sq = StepQueueEntry (step,rowcnt)
	queue.put(sq)