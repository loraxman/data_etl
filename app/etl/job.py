
import threading
import multiprocessing
import psycopg2
import yaml
 

class Job:
	def __init__(self):
		
		self.steps = []
		self.queue =  multiprocessing.Queue()
		self.pids = []
		self.step_pids = {}
	
		
	def loadyaml(self,yamlf):
		content = open(yamlf).read()
		
		ymlhash = yaml.load(content)
		self.name = ymlhash['job']['name'] 
		stepdict = ymlhash['job']['steps']
		
		#sort yaml by step name so to preserve sequence
		d1 = sorted(stepdict)
		for step in d1:
			item = stepdict[step]
			s = Step(item['name'], item['type'],item['file'], item['async'], item['err'])
			if item.has_key('waits_on'):
				for wait in item['waits_on']:
					s.wait_on(wait.strip())
			self.steps.append(s)
	
		
	def execute(self):
		for step in self.steps:
			print "sequencing...%s" % (step.name)
			for wstep in step.wait_steps:
				#if there is no pid for this wait step
				#it probably was done Sync
				if self.step_pids.has_key(wstep):
					for wpid in self.step_pids[wstep]:
#						wpid = self.step_pids[wstep]
						print "Step %s waits on %s, %d" % (step.name,wstep, wpid.pid,)
						wpid.join()
				else:
					print "missing pid %s" % (wstep,)
				
			if step.steptype == "SQL":
				if step.async:
					p = multiprocessing.Process(target=execstep, args=(self.queue,step.file,step,self.step_pids))
					self.pids.append(p)
					if not (self.step_pids.has_key(step.name)):
						self.step_pids[step.name] = []
					self.step_pids[step.name].append(p)
					p.start()
					print "started step %s" % (step.file)
				else:
					print "started step %s" % (step.file)
					execstep(self.queue,step.file,step)
			elif step.steptype == 'python':
				unittest = __import__(step.file)

				if step.async:
					p = multiprocessing.Process(target=unittest.execstep, args=(self.queue,step,))
					self.pids.append(p)
					p.start()
					print "started step %s" % (step.file)
				else:
					print "started step %s" % (step.file)
					unittest.exestep(self.queue, step)

				
		for pid in self.pids:
			#print "wait on pid %d " %(pid.pid,)
			pid.join()
			sq  = self.queue.get()
			print "Returned %s,%d" % (sq.step.name,sq.return_value)
			
		
class Step:
	def __init__(self,name,steptype, file,async=False,error=None):
		self.name = name
		self.steptype = steptype
		self.file = file
		self.async = async
		self.error = error
		self.wait_pids = []
		self.wait_steps = []
		
	def wait_on(self,step_name):
		self.wait_steps.append(step_name)
	
class StepQueueEntry:
	def __init__(self, step, return_value):
		self.step = step
		self.return_value = return_value
		
		
		
		
def execstep(queue=None, script=None,step=None,step_pids=None):
	print step_pids
	for wpid in step_pids[step.name]:
		print wpid
		#wpid.join()
		
	sql = open(script).read()
	conn = psycopg2.connect("dbname='claims' user='roger@wellmatchhealth.com' port='5439' host='dw-nonprod.healthagen.com' password='S6JB3ZjG7FMN'")
	cur = conn.cursor()
	
	cur.execute(sql)

	rowcnt = cur.rowcount
	sq = StepQueueEntry (step,rowcnt)
	queue.put(sq)	

