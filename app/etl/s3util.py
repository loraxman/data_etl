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


def get_s3_file(s3filename,bucketname,awsid,awskey,named_pipe=None,dirct=None):
    #rgex +name+ 
    #  [.*\Q+\E.*]/Test/
    rgex = re.compile("/%s/" % (s3filename,))
   # rgex = re.compile("%s+" % (s3filename,))
    print awsid +";" +  awskey
    AWS_ACCESS_KEY_ID=awsid
    AWS_SECRET_ACCESS_KEY=awskey
    s3 = boto.connect_s3(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    #s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucketname)
    print bucket
    if dir != None:
        print dirct
        listall = bucket.list(dirct)
    else:
        listall = bucket.list()
    fout = None
    for item in listall:
        fname = str(item).split(",")[1].strip(">").strip()
        if fname.find(s3filename) == -1:
            continue
        print fname
        infile = fname.replace("/",".")
        key = bucket.get_key(fname)
        key.get_contents_to_filename(infile)

    
    
    
#try to stream
def get_s3_file_stream(s3filename,bucketname,awsid,awskey,named_pipe=None,dirct=None):
	#rgex +name+ 
  #  [.*\Q+\E.*]/Test/
	rgex = re.compile("/%s/" % (s3filename,))
   # rgex = re.compile("%s+" % (s3filename,))
	print awsid +";" +  awskey
	AWS_ACCESS_KEY_ID=awsid
	AWS_SECRET_ACCESS_KEY=awskey
	s3 = boto.connect_s3(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
	#s3 = boto.connect_s3()
	bucket = s3.get_bucket(bucketname)
	print bucket
	if dir != None:
	   print dirct
	   listall = bucket.list(dirct)
	else:
	   listall = bucket.list()
	fout = None
	for item in listall:
		fname = str(item).split(",")[1].strip(">").strip()
		#print fname
		if fname.find(s3filename) == -1:
			continue
		print fname
		key = bucket.get_key(fname)
		key.get_contents_to_filename('test.txt')
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
			infile = fname.replace("/",".")
			fout = open(str(infile),"w")

		print 'transferring %s...' % fname
		incrbytes = 0
		eof = False
		#retry logic is for getting back Incomplete read from S3.
		#retry 10x until give up
		retry_http = 0
		sz = bucket.lookup(fname).size
		print 's3://'+AWS_ACCESS_KEY_ID+':' + AWS_SECRET_ACCESS_KEY + '@'+bucketname+'/' + fname
		with smart_open.smart_open('s3://'+AWS_ACCESS_KEY_ID+':' + AWS_SECRET_ACCESS_KEY + '@'+bucketname+'/' + fname) as fin:
			#read 50M in
			#then read do a single readline to make sure we got complete records before sending to pipe
			while not eof:
				try:
					inbuf = fin.read(50000000)
					if inbuf=="":
						print 'EOF'
						eof = True
						retry_http=111
						break
					retry_http = 111
					#read til we find a complete record
					for i in range (1,30000):
						inchar = fin.read(1)
						if inchar == "":
							eof = True
							print 'EOF'
							retry_http=111
							break
						inbuf += inchar
						if inchar == "\n":
							break
				except Exception as e:
					print e
				totbytes += len(inbuf)
				print "Bytes transferred %d of %d" % (totbytes, sz)
				if eof:
					if (not named_pipe):  #leave 
						fout.close()
					totbytes = 0
					break
				fout.write(inbuf)
				fout.flush()
	
	
