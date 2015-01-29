
from app.etl import job
import os
import flask
from flask.ext.celery import Celery
import redis

#from .run import get_celery


from celery import Celery

celery = Celery('tasks', broker='redis://localhost:6379/0')

celery.config_from_object('celeryconfig')

@celery.task
def add(x, y):
	return x + y


@celery.task()
def startjob(jobfile):
	redisconn = redis.StrictRedis(host='localhost', port=6379, db=0)
	j = job.Job(redisconn)
	j.loadyaml(jobfile)
	print "started job:" + j.name
	j.execute()
	
def make_celery(app):
	celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
	celery.conf.update(app.config)
	TaskBase = celery.Task
	class ContextTask(TaskBase):
		abstract = True
		def __call__(self, *args, **kwargs):
			with app.app_context():
				return TaskBase.__call__(self, *args, **kwargs)
	celery.Task = ContextTask
	return celery

