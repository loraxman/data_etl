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

#print json.loads(str(tmp))
#tmp = json.loads(str(tmp))
job=None
alljobs = {}
for j in joblist:
	job = json.loads(j)
	key = job.keys()[0]
	job[key]["schedule"] = eval(job[key]["schedule"])
	job[key]["args"] = eval(job[key]["args"])
	alljobs[key] = job[key]
print "-----------------------------------------"
print alljobs
CELERYBEAT_SCHEDULE = alljobs
print CELERYBEAT_SCHEDULE 
CELERYBEAT_SCHEDULE2 = {
	"every-minute": {
	"task": "app.tasks.add",
	 "schedule": crontab(minute='*/1'),
	"args": (1,2),
	},
   "every-minute2": {
   "task": "app.tasks.add",
   "schedule": crontab(minute='*/1'),
   "args": (1,2),
   },

}


	
