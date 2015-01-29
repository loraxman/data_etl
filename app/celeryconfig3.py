#CELERY_REDIS_SCHEDULER_URL = "redis://localhost:6379/1"
#CELERY_REDIS_SCHEDULER_KEY_PREFIX = 'tasks:meta:'
#BROKER_URL=broker='sqla+postgres://dbuser:dbpass@dbhost/celery'
	
from celery.schedules import crontab
from app import *

CELERY_IMPORTS=("app.tasks")

CELERYBEAT_SCHEDULE = {
'every-minute': {
'task': 'app.tasks.add',
'schedule': crontab(minute='*/1'),
'args': (1,2),
},
