from job import StepQueueEntry
import time

def execstep(queue=None,step=None,step_pids=None):
	step.active=True

	for wstep in step.wait_steps:
		for wpid in step_pids[wstep]:
#			print "........Step %s waits on %s" % ( step.name, wstep)
			wpid.join()
	#print "started steppy %s" % (step.name)
	time.sleep(10)
	sq = StepQueueEntry (step,"PASS")
	queue.put(sq)	

