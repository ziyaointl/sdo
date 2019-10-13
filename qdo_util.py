import qdo
import sqlite3

def transfer_queue(target_q, source_q, source_state):
    """If previous stage is QdoCentricStage, add tasks of a certain state
    from the previous stage's queue to this stage's queue
    """
    to_be_transferred = set([])
    existing = set([t.task for t in target_q.tasks()])
    for t in source_q.tasks():
        if t.state == source_state and t.task not in existing:
            to_be_transferred.add(t.task)
    target_q.add_multiple(to_be_transferred)
    print('Transferred', len(to_be_transferred), 'tasks')

def set_all_tasks_with_state(queue, source_state, target_state):
    n = 0
    for t in queue.tasks():
        if t.state == source_state:
            t.set_state(target_state)
            n += 1
    print('Setting', n, 'tasks from', source_state, 'to', target_state)

def record_all_tasks_with_state(queue, state, table):
    conn = sqlite3.connect('sdo.db')
    c = conn.cursor()
    for t in queue.tasks():
        if t.state == state:
            c.execute('INSERT INTO {} VALUES (?)'.format(table),
                (t.task))
    conn.commit()
    conn.close()
