from stages import *

prefix = 'ziyao-dr9m-south-'
stages_def = {
    prefix + '1': {
        'prev_stage': None,
        'tasks_per_nodehr': 3.5,
        'cores_per_worker': 17,
        'job_duration': 4,
        'arch': 'knl',
        'max_nodes_per_job': 80
    },
    prefix + '2': {
        'prev_stage': prefix + '1',
        'tasks_per_nodehr': 1/4,
        'job_duration': 4,
        'cores_per_worker': 34,
        'arch': 'knl',
        'max_nodes_per_job': 80
    }
}
