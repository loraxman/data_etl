from job import StepQueueEntry

def execstep(queue=None,step=None):
	sq = StepQueueEntry (step,1)
	queue.put(sq)	

