from job import StepQueueEntry
import s3util

def execstep(queue=None,step=None,step_pids=None):
	step.active=True
	
	for wstep in step.wait_steps:
		for wpid in step_pids[wstep]:
			wpid.join()


    conn = psycopg2.connect(step.connectdb)
    cur = conn.cursor()

	s3util.get_s3_file("00",named_pipe="claimraw.dat")	
	sq = StepQueueEntry (step,"PASS")
	queue.put(sq)	
