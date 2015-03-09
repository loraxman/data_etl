from job import StepQueueEntry
import data_quality

def execstep(queue=None,step=None,step_pids=None):
	step.active=True
	for wstep in step.wait_steps:
		for wpid in step_pids[wstep]:
			wpid.join()


	data_quality.gzipped_to_pipe("/Volumes/My Passport/WM/EPDB-HEALTHLINE.MULTI-EXTRACT.20150202180939/provntwkloc.dat.gz",named_pipe="provntwkloc.dat")	
	sq = StepQueueEntry (step,"PASS")
	queue.put(sq)	
