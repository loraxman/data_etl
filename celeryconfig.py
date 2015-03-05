CELERY_REDIS_SCHEDULER_URL = "redis://localhost:6379/1"
CELERY_REDIS_SCHEDULER_KEY_PREFIX = 'tasks:meta:'


CELERY_RESULT_BACKEND = "redis://localhost:6379/1"
BROKER_URL=broker="redis://localhost:6379/1"



#CELERY_RESULT_BACKEND = "db+postgres://A727200:s@127.0.0.1/claimdata"
#BROKER_URL=broker='sqla+postgres://A727200:s@127.0.0.1/claimdata'

import redis
import json
#from app import tasks
def get_redis_schedule():
	r = redis.Redis()
	list = r.hgetall("taskhash")
	i=0
	joblist = []
	for item in list:
		print item
		job = r.hget("taskhash",item)
		print job
	#	json.loads(job)
		joblist.append(job)
		i+=1
	return joblist

CELERY_TIMEZONE = 'America/Chicago'
CELERY_IMPORTS=("app.tasks")
from celery.schedules import crontab

#why not just read from redis here!!
joblist = get_redis_schedule()
print "_____________________"

print "_____________________"
#print json.loads(str(tmp))
#tmp = json.loads(str(tmp))
job=None
for j in joblist:
	job = json.loads(j)
	print job
	key = job.keys()[0]
	print job[key]["schedule"]
	job[key]["schedule"] = eval(job[key]["schedule"])
	print "eval1"
	job[key]["args"] = eval(job[key]["args"])
	print "eval2"
	print job[key]["args"]
	print job[key]
print "00000000000000000000000"
CELERYBEAT_SCHEDULE = job
#print CELERYBEAT_SCHEDULE 
CELERYBEAT_SCHEDULE2 = {
	"every-minute": {
	"task": "app.tasks.add",
	 "schedule": crontab(minute='*/1'),
	"args": (1,2),
	},
}


	
