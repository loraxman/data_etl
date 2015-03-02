from  job import Job

import os
import sys

class TestRunner:
	@staticmethod
	def execute_job(jobfile):
		j = Job(None)
		j.loadyaml(jobfile)
		if j.jobtype == "unittest":
			print "--------------------------------------------------"
			print "started test:\t" + j.name
			ret_status = j.execute()
			return j,ret_status
		else:
			return j, []
	
	@staticmethod
	def runtests(directory):
		#get all unit type files
		#execute each job noting pass/fail
		jobfiles = os.listdir(directory)
		runjobs = []
		for job in jobfiles:
			runjob, ret_status = TestRunner.execute_job(directory+"/"+job)
			jobres = {job:runjob, "status":ret_status}
			if runjob.jobtype=="unittest":
				runjobs.append(jobres)
#				print "-------------------------------------------\n\n"
			for stat in ret_status:
				print "%s\t%s\n" % (stat.return_value,stat.step.description)
			print "\n"
		TestRunner.print_results(runjobs)
		return runjobs
	
	@staticmethod
	def print_results(jobs):
		print "\n\n\n=====================================SUMMARY============================================"
		jobcnt =  len(jobs)
		fails = 0
		
		for job in jobs:
			job["testpass"] = True
			for stat in job["status"]:
#				print "%s\t%s\n" % (stat.return_value,stat.step.description)
				if stat.return_value != "PASS":
					job["testpass"] = False
		
		for job in jobs:
			for key in job.keys():
				if key != "status" and  key != "testpass":
					break
				
			if  job["testpass"] == False :
				print "job:%20s%40s\t\t FAILED" % (job[key].name,job[key].description)
				fails += 1
			else:
				print "job:%20s%40s\t\t PASS" % (job[key].name,job[key].description)
				
				
		print "Total tests failed:%d" % fails
		print "Total test passeed:%d" % (jobcnt-fails)
		if fails == 0 :
			print "\nTESTS ALL PASSED"
		else:
			print "\nTEST FAILURES - Correct"
	
if __name__ == "__main__":
	TestRunner.runtests(sys.argv[1])