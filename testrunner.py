import sys
from app.etl import testrunner
if __name__ == "__main__":
	testrunner.TestRunner.runtests(sys.argv[1])
	