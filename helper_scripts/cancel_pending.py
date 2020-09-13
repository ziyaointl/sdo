import sys
sys.path.append("..")
from stages import *
from util import run_command

sentinel = SentinelStage()
stage = RunbrickPyStage(sys.argv[1], sentinel, 0)
for j in stage.get_jobs_in_queue():
    if j['STATE'] == 'PENDING':
        id = j['JOBID']
        run_command("scancel {}".format(j['JOBID']))
