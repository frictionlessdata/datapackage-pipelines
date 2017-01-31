import os
import coverage

os.environ['COVERAGE_PROCESS_START']= os.path.join(os.environ["PWD"], 'tox.ini')
coverage.process_startup()  

