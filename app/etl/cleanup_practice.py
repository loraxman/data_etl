from job import StepQueueEntry
import time
import data_quality

def execstep(queue=None,step=None,step_pids=None):
	step.active=True
	for wstep in step.wait_steps:
		for wpid in step_pids[wstep]:
			wpid.join()

	#read thru the file
	#stip out "
	inbuf = open("/Users/A727200/proj/ipynotes/work/provider/practice.csv").read()
	cleanbuf = data_quality.strip_doublequotes(inbuf)
	#strip off extra trailing '-' causing description unique issues
	lines = cleanbuf.split("\n")
	cleanbuf = ""
	for line in lines:
		if line.strip() == "":
			break
		#need to cleanup practice description
		#collapse spaces and reconstruct so that the descriptions are unique
		cleanbuf += line.strip().strip("-").strip() + "\n"
	fout = open("/Users/A727200/proj/ipynotes/work/provider/cleanpract.csv","w")
	fout.write(cleanbuf)
	fout.close()
	sq = StepQueueEntry (step,"PASS")
	queue.put(sq)	

