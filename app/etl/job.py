
import threading
import multiprocessing
import psycopg2
import yaml
import Queue
import jsonpickle


#Job class is a group of steps that can be executed in order as defined
#externally.
#Implements the threaded version
class Job:
	def __init__(self):
		
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
	
	def json(self):
		return jsonpickle.encode(self)
		
	def loadyaml(self,yamlf):
		content = open(yamlf).read()
		
		ymlhash = yaml.load(content)
		self.name = ymlhash['job']['name'] 
		stepdict = ymlhash['job']['steps']
		
		#sort yaml by step name so to preserve sequence
		d1 = sorted(stepdict)
		for step in d1:
			item = stepdict[step]
			s = Step(item['name'], item['type'],item['file'], item['async'], item['err'],item['description'])
			if item.has_key('waits_on'):
				for wait in item['waits_on']:
					s.wait_on(wait.strip())
			s.job = self
			self.steps.append(s)
		for step in self.steps:
			step.waiting_on_step()
				
	#execute the Job
	def execute(self):
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
					print "started step %s" % (step.name,self.step_pids)
					execstep(self.queue,step.file,step)
			elif step.steptype == 'python':
				pymod = __import__(step.file)

				if step.async:
					p = threading.Thread(target=pymod.execstep, args=(self.queue,step,self.step_pids))
#					p = multiprocessing.Process(target=unittest.execstep, args=(self.queue,step,))
					self.pids.append(p)
					p.start()
#					print "started step %s" % (step.name)
				else:
					print "started step %s" % (step.name,self.step_pids)
					pymod.exestep(self.queue, step)

				
		for pid in self.pids:
			#print "wait on pid %d " %(pid.pid,)
			pid.join()
			sq  = self.queue.get()
			print "\n\t\tReturned %s,%d" % (sq.step.name,sq.return_value)
			
	
#Step is an executable unit of work
#typically will be a sql script
#can also be a python script
#all assume an interface call "execstep"	
class Step:
	def __init__(self,name,steptype, file,async=False,error=None,description=None,job=None):
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
		
		
		
		
def execstep(queue=None, script=None,step=None,step_pids=None):
	#below for threading only
	#wait on another thread(s) to finish
	for wstep in step.wait_steps:
		for wpid in step_pids[wstep]:
			print "........Step %s waits on %s" % ( step.name, wstep)
			wpid.join()
	print "\nstarted step %s" % (step.name)
	
	sql = open(script).read()
	conn = psycopg2.connect("dbname='claims' user='roger@wellmatchhealth.com' port='5439' host='dw-nonprod.healthagen.com' password='S6JB3ZjG7FMN'")
	cur = conn.cursor()
	
	cur.execute(sql)

	rowcnt = cur.rowcount
	sq = StepQueueEntry (step,rowcnt)
	queue.put(sq)	

