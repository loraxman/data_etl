from job import StepQueueEntry
import time
import data_quality
import pandas as pd

def execstep(queue=None,step=None,step_pids=None):
	step.active=True
	for wstep in step.wait_steps:
		for wpid in step_pids[wstep]:
			wpid.join()

	#read thru the file
	#stip out "
	practice_from_excel()
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
		cleanbuf = cleanbuf + line.strip().strip("-").strip().strip(" ") + ";" +  "\n"
	fout = open("/Users/A727200/proj/ipynotes/work/provider/cleanpract.csv","w")
	fout.write(cleanbuf)
	fout.close()
	sq = StepQueueEntry (step,"PASS")
	queue.put(sq)	


def practice_from_excel():
	#read in from excel
	#fix 'blank' assumed repeat cells so that we have real Db rows
	practice = pd.io.excel.read_excel("/Users/A727200/proj/ipynotes/work/provider/AMA_Specialty_Mapping.xlsx")
	#drop all entirely null rows
	practice = practice.dropna(subset=practice.columns[0:], how='all')
	prevrow = None
	ix = 0
	for item in practice.iterrows():
		# print item[1]['Codes']
		if str(item[1]['Codes']).strip() == "nan":
			item[1]['Codes'] = prevrow[1]['Codes']
			item[1]['AMA Specialty Group'] = prevrow[1]['AMA Specialty Group']
	        
		else:
			prevrow = item
		ix += 1
	practice.to_csv("/Users/A727200/proj/ipynotes/work/provider/practice.csv",sep="|",index=False)
	
	