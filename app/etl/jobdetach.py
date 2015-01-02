from job import *
import sys

j = Job()
j.loadyaml(sys.argv[1])
j.execute()


