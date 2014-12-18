
import threading
import multiprocessing
import psycopg2
 

class Job:
	def __init__(self,name):
		self.name = name
		self.steps = []
		self.queue =  multiprocessing.Queue()
		self.pids = []
		self.step_pids = {}
		
	def execute(self):
		for step in self.steps:
			for wstep in step.wait_steps:
				wpid = self.step_pids[wstep]
				print "Step %s waits on %d" % (step.name,wpid.pid,)
				wpid.join()
				
			if step.steptype == "SQL":
				if step.async:
					p = multiprocessing.Process(target=execstep, args=(self.queue,step.file,step,))
					self.pids.append(p)
					self.step_pids[step.name] = p
					p.start()
					print "started step %s" % (step.file)
				else:
					dbscript.execstep(step.sqlfile)
			elif step.steptype == 'python':
				unittest = __import__(step.file)

				if step.async:
					p = multiprocessing.Process(target=unittest.execstep, args=(self.queue,step,))
					self.pids.append(p)
					p.start()
					print "started step %s" % (step.file)
				else:
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
		
		
		
		
def execstep(queue=None, script=None,step=None):

	sql = open(script).read()
	conn = psycopg2.connect("dbname='claims' user='roger@wellmatchhealth.com' port='5439' host='dw-nonprod.healthagen.com' password='S6JB3ZjG7FMN'")
	cur = conn.cursor()
	
	cur.execute(sql)

	rowcnt = cur.rowcount
	sq = StepQueueEntry (step,rowcnt)
	queue.put(sq)	

