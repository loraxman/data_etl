from job import StepQueueEntry


def execstep(queue=None,step=None,step_pids=None):

	for wstep in step.wait_steps:
		for wpid in step_pids[wstep]:
			print "........Step %s waits on %s" % ( step.name, wstep)
			wpid.join()
	
	sq = StepQueueEntry (step,1)
	queue.put(sq)	

