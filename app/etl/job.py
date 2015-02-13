
import threading
import multiprocessing
import psycopg2
import yaml
import Queue
import jsonpickle
import sys
import time


#Job class is a group of steps that can be executed in order as defined
#externally.
#Implements the threaded version
class Job:
	def __init__(self,redis):
		
		self.steps = []
		#below reference to mulitprocessing is
		#for a version that would use multi-processes to work
		#currently not used, has an issue regarding child steps waiting on other child steps
		#threads addresses this issue fine, but child procs can't wait on other child procs
		self.queue =  multiprocessing.Queue()
		#Thread queue for returning results back to main controller
		self.queue =  Queue.Queue()
		self.pids = []
		self.step_pids = {}
		self.redis = redis
		self.monitor_alive = True
		
	#keyed by filename then list of running jobs
	def monitor_persist(self):
		if self.redis == None:
			return
		while self.monitor_alive:
			jobsteps = []
			active = self.active_steps()
			print self.job_id, active
			self.redis.hset(self.yamlfile,  str(self.job_id), active)

			time.sleep(5)
		self.redis.delete(self.yamlfile, str(self.job_id))
		
	def active_steps(self):
		active = []
		for step in self.steps:
			if step.active:
				active.append(step.name)
		return active
	
	def json(self):
		return jsonpickle.encode(self)
		
	def loadyaml(self,yamlf):
		self.yamlfile = yamlf
		content = open(yamlf).read()
#		print content
		ymlhash = yaml.load(content)
		self.name = ymlhash['job']['name'] 
		stepdict = ymlhash['job']['steps']
		self.description = ymlhash['job']['description']
		self.jobtype = ymlhash['job']['jobtype']
		#sort yaml by step name so to preserve sequence
		d1 = sorted(stepdict)
		for step in d1:
			item = stepdict[step]
			s = Step(item['name'], item['type'],item['file'], item['async'], item['err'],item['description'],connectdb=item['connectdb'])
			if item.has_key('waits_on'):
				for wait in item['waits_on']:
					s.wait_on(wait.strip())
			s.job = self
			self.steps.append(s)
		for step in self.steps:
			step.waiting_on_step()
				
	#execute the Job
	def execute(self):
		#job id
		self.job_id = int(time.time()) 
		pmon = threading.Thread(target=self.monitor_persist, args=())
		pmon.start();
		#start monitor thread
		for step in self.steps:
			#note below loop only used for Process based
			#currenlty does nothing!
			for wstep in step.wait_steps:
				#if there is no pid for this wait step
				#it probably was done Sync
				if self.step_pids.has_key(wstep):
					for wpid in self.step_pids[wstep]:
						pass
#						wpid = self.step_pids[wstep]
		#				print "Step %s waits on %s" % (step.name,wstep)
					#	wpid.join()
				else:
#					print "missing pid %s" % (wstep,)
					pass
				
			if step.steptype == "SQL":
				if step.async:
					p = threading.Thread(target=execstep, args=(self.queue,step.file,step,self.step_pids))
#					p = multiprocessing.Process(target=execstep, args=(self.queue,step.file,step,self.step_pids))
					self.pids.append(p)
					if not (self.step_pids.has_key(step.name)):
						self.step_pids[step.name] = []
					self.step_pids[step.name].append(p)
					p.start()
#					print "started step %s" % (step.name)
				else:
			#		print "started step %s" % (step.name,self.step_pids)
					execstep(self.queue,step.file,step)
			elif step.steptype == 'python':
				#find path of file
				paths=step.file.split("/")
				endpath=paths[len(paths)-1]
				subpath=step.file[0:step.file.index(endpath)]
		#		print subpath
				sys.path.append(subpath)

				pymod = __import__(endpath)

				if step.async:
					p = threading.Thread(target=pymod.execstep, args=(self.queue,step,self.step_pids))
#					p = multiprocessing.Process(target=unittest.execstep, args=(self.queue,step,))
					self.pids.append(p)
					p.start()
#					print "started step %s" % (step.name)
				else:
				#	print "started step %s" % (step.name,self.step_pids)
					pymod.exestep(self.queue, step)

		pidlen = len(self.pids)
		deque = 0		
		job_status = []
		while(deque < pidlen):
			#print "wait on pid %d " %(pid.pid,)
			#pid.join()
			sq  = self.queue.get()
			if (sq != None):
#				print "\n\t\tReturned %s,%s" % (sq.step.name,sq.return_value)
				sq.step.active=False
				job_status.append(sq)
				deque += 1
		self.monitor_alive = False
		return job_status
		
	
#Step is an executable unit of work
#typically will be a sql script
#can also be a python script
#all assume an interface call "execstep"	
class Step:
	def __init__(self,name,steptype, file,async=False,error=None,description=None,job=None,connectdb=None):
		self.name = name
		self.steptype = steptype
		self.file = file
		self.async = async
		self.error = error
		self.wait_pids = []
		self.wait_steps = []
		self.description = description
		self.job=job
		self.waiting_on = []
		self.active = False
		self.connectdb = connectdb

		
	def wait_on(self,step_name):
		self.wait_steps.append(step_name)
		
	def waiting_on_step(self):
		self.waiting_on = []
		#who waits on me?
		for step in self.job.steps:
			if (step.name != self.name):
				for wait in step.wait_steps:
					if wait == self.name:
						self.waiting_on.append(step.name)
		
		
		
	
class StepQueueEntry:
	def __init__(self, step, return_value):
		self.step = step
		self.return_value = return_value
		
		
		
		
def execstep(queue=None, script=None,step=None,step_pids=None,passed_conn=None):
	#below for threading only
	#wait on another thread(s) to finish
	step.active=True
	for wstep in step.wait_steps:
		for wpid in step_pids[wstep]:
		#	print "........Step %s waits on %s" % ( step.name, wstep)
			wpid.join()
	#print "\nstarted step %s" % (step.name)
	
	status = "FAIL"
	try:
		sql = open(script).read()
		if passed_conn == None:
			conn = psycopg2.connect(step.connectdb)
		else:
			conn = passed_conn
		cur = conn.cursor()
	#	print sql
		cur.execute(sql)
		#fetch all
		#we will assume the first row/col is the result "PASS"/"FAIL"
		rows = cur.fetchall()
		status = rows[0][0]
	except Exception as inst:
		print "Error:%s" % inst
		status = "FAIL"
		
	sq = StepQueueEntry (step,status)
	queue.put(sq)	

