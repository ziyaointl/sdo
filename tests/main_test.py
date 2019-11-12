import sys
sys.path.append('../')
from settings import *
from subprocess import run, PIPE, STDOUT, CalledProcessError, Popen, TimeoutExpired
import os
import signal

BRICK = '0486p005'

def launch_runbrick(qname, name):
    print('Lanching', name)
    try:
        p = run(['../scripts/{}.sh'.format(qname), BRICK, '-W 400 -H 400'], stdout=PIPE, stderr=STDOUT, check=True)
    except CalledProcessError as e:
        print(e.output.decode("utf-8"))
        print(name, 'failed')
        sys.exit()
    print(p.stdout.decode("utf-8"))
    print(name, 'succeeded')

# Launch prefarm
launch_runbrick(PREFARM_QNAME, 'Prefarm')

# Launch farm
print('Launching farm')
farm = Popen(['../scripts/launch-farm.sh'], preexec_fn=os.setsid)
print('Launching worker')
worker = Popen(['../scripts/launch-worker.sh tcp://$(hostname):5555'], preexec_fn=os.setsid, shell=True)
try:
    outs1, errs1 = farm.communicate(timeout=60)
except TimeoutExpired:
    os.killpg(os.getpgid(farm.pid), signal.SIGTERM)
    os.killpg(os.getpgid(worker.pid), signal.SIGTERM)
    outs1, errs1 = farm.communicate()
    outs2, errs2 = worker.communicate()
print(outs1, errs1)
print(outs2, errs2)
try:
    os.killpg(os.getpgid(farm.pid), signal.SIGTERM)
except ProcessLookupError:
    pass
print('Farm launch ended')
try:
    os.killpg(os.getpgid(worker.pid), signal.SIGTERM)
except ProcessLookupError:
    pass
print('Worker launch ended')

# Launch postfarm
launch_runbrick(POSTFARM_QNAME, 'Postfarm')

