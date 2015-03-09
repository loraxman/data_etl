import os
import gzip

def strip_doublequotes(instr):
	return instr.replace('"', '')


def gzipped_to_pipe(fname,named_pipe=None):
		fout = None
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

			print 'transferring %s...' % fname
			incrbytes = 0
			eof = False
			#retry logic is for getting back Incomplete read from S3.
			#retry 10x until give up
			retry_http = 0
			with gzip.open( fname) as fin:
				#read 50M in
				#then read do a single readline to make sure we got complete records before sending to pipe
				sz = os.fstat(fin.fileno()).st_size
				while not eof:
					try:
						inbuf = fin.read(50000000)
						print inbuf[0:30]
						if inbuf=="":
							print 'EOF'
							eof = True
							break
						#read til we find a complete record
						for i in range (1,30000):
							inchar = fin.read(1)
							if inchar == "":
								eof = True
								print 'EOF'
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
