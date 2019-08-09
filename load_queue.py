"""Load bricks from file to the Prefarm qdo queue
"""

from settings import PREFARM_QNAME
import sys, qdo

if len(sys.argv) != 2:
    print('Usage python load_queue.py <inputfile>')
    exit(1)

with open(sys.argv[1], 'r') as f:
    try:
        q = qdo.connect(PREFARM_QNAME)
    except ValueError:
        q = qdo.create(PREFARM_QNAME)
    tasks = set(t.task for t in q.tasks())
    count = 0
    for l in f:
        task = l.strip()
        if task not in tasks:
            q.add(task)
            tasks.add(task)
            count += 1
    print('Added', count, 'tasks to', PREFARM_QNAME)
