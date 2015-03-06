import boto
import os
import uuid
import sys
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import sys
from boto.s3.connection import S3Connection
import smart_open
import re

#grab a file from an S3 bucket stream as std out

def get_s3_file(s3filename,named_pipe=None):
	#rgex +name+ 
	rgex = re.compile("%s+" % (s3filename,))
	AWS_ACCESS_KEY_ID=open("awsid.txt").read().strip()
	AWS_SECRET_ACCESS_KEY=open("awskey.txt").read().strip()
	s3 = boto.connect_s3(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
	#s3 = boto.connect_s3()
	bucket = s3.get_bucket('wellmatch-healthline-provider-data')
	
	listall = bucket.list()
	fout = None
	for item in listall:
		fname = str(item).split(",")[1].strip(">").strip()
		if (not rgex.match(fname)):
			continue
		sz = bucket.lookup(fname).size
		print fname + ' size->' + str(sz)
		totbytes = 0
		if named_pipe:
			infile = named_pipe
			try:
				if (not fout):
					print 'make pipe'
					os.system('mkfifo ' + infile)
					fout = open(str(infile),"w")
			except Exception as e:
				print e
				pass # if exists don't care
		else:
			infile = fname
			fout = open(str(infile),"w")

		print 'transferring %s...' % fname
		incrbytes = 0
		eof = False
		with smart_open.smart_open('s3://wellmatch-healthline-provider-data/' + fname) as fin:
			#read 50M in
			#then read do a single readline to make sure we got complete records before sending to pipe
			while not eof:
				inbuf = fin.read(50000000)
				if inbuf=="":
					eof = True
					break
				#read til we find a complete record
				for i in range (1,30000):
					inchar = fin.read(1)
					if inchar == "":
						eof = True
						break
					inbuf += inchar
					if inchar == "\n":
						break
				totbytes += len(inbuf)
				print "Bytes transferred %d of %d" % (totbytes, sz)
				if eof:
					if (not named_pipe):  #leave 
						fout.close()
					totbytes = 0
					break
				fout.write(inbuf)
				fout.flush()
	
	
