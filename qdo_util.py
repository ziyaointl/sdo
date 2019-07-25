import qdo

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
