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
	list = r.llen("tasks")
	print "size %d\n" % list
	i = 0
	joblist = ''
	for item in range(0,list):
		job = r.lindex("tasks",i)
		print job
		#json.loads(job)
		joblist += job 
		i+=1
	return joblist 

CELERY_IMPORTS=("app.tasks")
from celery.schedules import crontab

#why not just read from redis here!!
tmp = get_redis_schedule()
print tmp
tmp = json.loads(str(tmp))
for key in tmp.keys():
	tmp[key]["schedule"] = eval(tmp[key]["schedule"])
	tmp[key]["args"] = eval(tmp[key]["args"])
	print tmp[key]
	
CELERYBEAT_SCHEDULE = tmp
#print CELERYBEAT_SCHEDULE 
CELERYBEAT_SCHEDULE2 = {
	"every-minute": {
	"task": "app.tasks.add",
	 "schedule": crontab(minute='*/1'),
	"args": (1,2),
	},
}


	