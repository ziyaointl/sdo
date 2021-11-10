from stages import SentinelStage, RunbrickPyStage, TaskSource
from collections import namedtuple

# QOS choices
def regular():
    return "-q regular"

def debug():
    return "-q debug"

def realtime():
    return "-q realtime --exclusive"

def reservation(name):
    return "--reservation " + name

def bigmem():
    return "-q bigmem --cluster escori"


TaskSource = namedtuple('TaskSource', ['name', 'states'])
StageDefinition = namedtuple('StageDefinition',
    [
        # Basic info
        'name', # Name of the stage
        'qname', # qdo queue for the stage
        'prev_stage', # Optional: name of the previous stage; this stage would not start scheduling if previous stage did not finish
        'task_srcs', # Optional: automatically import tasks from previous queues; format: TaskSource(queue_name, ['Failed', 'Running', 'Succeeded', 'Killed'])
        # Resource allocation per brick
        'tasks_per_nodehr', # Estimated number of bricks completed per nodehr
        'cores_per_worker', # Number of cores to allocated per brick
        'cores_per_worker_actual', # Number of cores actually allowed per brick (to prevent memory exhaustion) # TODO: Find a better abstraction
        'mem_per_worker', # Amount of memory to allocate per worker # TODO: automate this?
        # Job configuration
        'job_duration', # Duration of a job, in hours
        'max_nodes_per_job', # Max number of nodes to request per job
        'max_number_of_jobs', # Max number of jobs that the queue can contain at a time # TODO: Remove this?
        'arch', # Architecture of the nodes to request, could be haswell, knl, or amd
        'qos', # NERSC queue type, should be a call to QOS functions below
        'allocation', # Which NERSC allocation to use for this stage
        # Runbrick configuration
        'stage', # Stop runbrick at this stage; For stage names, see https://github.com/legacysurvey/legacypipe/blob/main/py/legacypipe/runbrick.py
        'write_stage', # A list of stages at which we'd like checkpoint
        # Job automation
        'revive_all', # Whether to revive killed tasks; TODO: deprecate?
    ]
)


def gen_stages(stages_def, default_def):
    """Given stage definitions, generate a list of stage instances
    """
    # Merge stage defs with default; if a field is contained in both, the default one is overwritten
    stages_def = [ {**default_def, **sdef} for sdef in stages_def ]

    stages_dict = {}
    stages_list = []

    for sdef in stages_def:
        curr_stage = sdef['class'](
            sdef['name'],
            sdef['qname'],
            SentinelStage() if sdef['prev_stage'] is None else stages_dict[sdef['prev_stage']],
            tasks_per_nodehr=sdef['tasks_per_nodehr'],
            cores_per_worker=sdef['cores_per_worker'],
            cores_per_worker_actual=sdef['cores_per_worker_actual'],
            job_duration=sdef['job_duration'],
            arch=sdef['arch'],
            max_nodes_per_job=sdef['max_nodes_per_job'],
            allocation=sdef['allocation'],
            max_number_of_jobs=sdef['max_number_of_jobs'],
            qos=sdef['qos'],
            stage=sdef['stage'],
            write_stage=sdef['write_stage'],
            revive_all=sdef['revive_all'],
            mem_per_worker=sdef['mem_per_worker'],
            task_srcs=sdef['task_srcs'],
        )
        stages_list.append(curr_stage)
        stages_dict[sdef['name']] = curr_stage

    return stages_list

# Constants
PREFIX = 'ziyao-dr9m-south-'
HASWELL_MEM = 125000000
KNL_MEM = 93750000

stage_instances = gen_stages(
    [
    #{
    #    'name': PREFIX + '0',
    #    'qname': PREFIX + '0',
    #    'prev_stage': None,
    #    'tasks_per_nodehr': 20,
    #    'cores_per_worker': 8,
    #    'cores_per_worker_actual': 8,
    #    'mem_per_worker': HASWELL_MEM // 4,
    #    'job_duration': 0.5,
    #    'arch': 'haswell',
    #    'max_nodes_per_job': 20,
    #    'max_number_of_jobs': 4,
    #    'stage': 'tims',
    #    'write_stage': ['tims'],
    #    'qos': debug(),
    #    'revive_all': True,
    #},
    #{
    #    'name': PREFIX + '1',
    #    'qname': PREFIX + '1',
    #    'prev_stage': None,
    #    'tasks_per_nodehr': 3.5,
    #    'cores_per_worker': 17,
    #    'cores_per_worker_actual': 17,
    #    'mem_per_worker': KNL_MEM // 4,
    #    'job_duration': 4,
    #    'arch': 'knl',
    #    'write_stage': ['srcs'],
    #    'max_nodes_per_job': 80,
    #    'max_number_of_jobs': 1,
    #},
    #{
    #    'name': PREFIX + '2',
    #    'prev_stage': PREFIX + '0',
    #    'tasks_per_nodehr': 1/4,
    #    'cores_per_worker': 34,
    #    'cores_per_worker_actual': 34,
    #    'mem_per_worker': KNL_MEM // 2,
    #    'job_duration': 7,
    #    'arch': 'knl',
    #    'max_nodes_per_job': 340,
    #    'max_number_of_jobs': 1,
    #    'class': PostFarmStage,
    #    'write_stage': ['srcs']
    #    'qos': reservation('dr9_priority'),
    #},
    #{
    #    'name': PREFIX + '4',
    #    'prev_stage': None,
    #    'tasks_per_nodehr': 1,
    #    'cores_per_worker': 17,
    #    'cores_per_worker_actual': 17,
    #    'mem_per_worker': KNL_MEM // 4,
    #    'job_duration': 6,
    #    'arch': 'knl',
    #    'max_nodes_per_job': 20,
    #    'max_number_of_jobs': 0,
    #    'stage': 'fitblobs',
    #    'write_stage': ['fitblobs'],
    #    'class': PostFarmStage,
    #    'qos': reservation('dr9_priority'),
    #},
    #{
    #    'name': PREFIX + '5',
    #    'prev_stage': PREFIX + '4',
    #    'tasks_per_nodehr': 0.1,
    #    'cores_per_worker': 34,
    #    'cores_per_worker_actual': 8,
    #    'mem_per_worker': KNL_MEM // 2,
    #    'job_duration': 6,
    #    'arch': 'knl',
    #    'write_stage': ['srcs'],
    #    'max_nodes_per_job': 20,
    #    'max_number_of_jobs': 0,
    #    'class': PostFarmStage,
    #    'qos': reservation('dr9_priority'),
    #},
    #{
    #    'name': PREFIX + 'failed',
    #    'qname': PREFIX + 'failed',
    #    'prev_stage': None,
    #    'tasks_per_nodehr': 0.01,
    #    'cores_per_worker': 32,
    #    'mem_per_worker': HASWELL_MEM,
    #    'write_stage': ['srcs'],
    #    'cores_per_worker_actual': 16,
    #    'job_duration': 24,
    #    'max_nodes_per_job': 32,
    #    'max_number_of_jobs': 0,
    #    'arch': 'haswell',
    #    'qos': reservation('dr9_haswell')
    #},
    #{
    #    'name': PREFIX + 'bigmem',
    #    'qname': PREFIX + 'bigmem',
    #    'prev_stage': None,
    #    'task_srcs': [
    #        TaskSource(PREFIX + 'failed', ['Failed', 'Running', 'Killed'])
    #    ],
    #    'tasks_per_nodehr': 0.01,
    #    'cores_per_worker': 8,
    #    'mem_per_worker': HASWELL_MEM*10,
    #    'write_stage': ['srcs', 'fitblobs', 'coadds', 'wise_forced'],
    #    'cores_per_worker_actual': 8,
    #    'job_duration': 72,
    #    'max_nodes_per_job': 1,
    #    'max_number_of_jobs': 0,
    #    'arch': 'haswell',
    #    'qos': bigmem()
    #},
    #{
    #    'name': PREFIX + 'bigmem-resv',
    #    'qname': PREFIX + 'bigmem',
    #    'prev_stage': None,
    #    'tasks_per_nodehr': 0.001,
    #    'cores_per_worker': 32,
    #    'mem_per_worker': HASWELL_MEM*10,
    #    'write_stage': ['srcs', 'fitblobs', 'wise_forced'],
    #    'cores_per_worker_actual': 8,
    #    'job_duration': 56,
    #    'max_nodes_per_job': 1,
    #    'max_number_of_jobs': 0,
    #    'arch': 'haswell',
    #    'qos': reservation('dr9_haswell')
    #},
    #{
    #    'name': PREFIX + '7',
    #    'prev_stage': None,
    #    'tasks_per_nodehr': 0.1,
    #    'cores_per_worker': 17,
    #    'cores_per_worker_actual': 17,
    #    'mem_per_worker': KNL_MEM // 4,
    #    'job_duration': 6.9,
    #    'arch': 'knl',
    #    'write_stage': ['tims', 'fitblobs'],
    #    'stage': 'fitblobs',
    #    'max_nodes_per_job': 19,
    #    'max_number_of_jobs': 0,
    #    'class': PostFarmStage,
    #    'qos': reservation('dr9_priority'),
    #},
    #{   # tims for south < -28
    #    'name': PREFIX + '8',
    #    'qname': PREFIX + '8',
    #    'prev_stage': None,
    #    'tasks_per_nodehr': 20,
    #    'cores_per_worker': 8,
    #    'cores_per_worker_actual': 8,
    #    'mem_per_worker': HASWELL_MEM // 4,
    #    'job_duration': 0.5,
    #    'arch': 'haswell',
    #    'write_stage': ['tims'],
    #    'stage': 'tims',
    #    'max_nodes_per_job': 20,
    #    'max_number_of_jobs': 0,
    #    'qos': debug(),
    #},
    #{   # fitblobs for south < -28
    #    'name': PREFIX + '9-resv-knl',
    #    'qname': PREFIX + '9',
    #    'prev_stage': PREFIX + '8',
    #    'task_srcs': [
    #        TaskSource(PREFIX + '8', ['Succeeded', 'Killed'])
    #    ],
    #    'tasks_per_nodehr': 0.1,
    #    'cores_per_worker': 17,
    #    'cores_per_worker_actual': 17,
    #    'mem_per_worker': KNL_MEM // 4,
    #    'job_duration': 24,
    #    'arch': 'knl',
    #    'write_stage': ['fitblobs'],
    #    'stage': 'fitblobs',
    #    'max_nodes_per_job': 50,
    #    'max_number_of_jobs': 5,
    #    'qos': reservation('dr9_south'),
    #},
    #{   # fitblobs for south < -28, haswell
    #    'name': PREFIX + '9-resv-haswell',
    #    'qname': PREFIX + '9',
    #    'prev_stage': None,
    #    'tasks_per_nodehr': 1,
    #    'cores_per_worker': 8,
    #    'cores_per_worker_actual': 8,
    #    'mem_per_worker': HASWELL_MEM // 4,
    #    'job_duration': 24,
    #    'arch': 'haswell',
    #    'write_stage': ['fitblobs'],
    #    'stage': 'fitblobs',
    #    'max_nodes_per_job': 1,
    #    'max_number_of_jobs': 12,
    #    'qos': reservation('dr9_haswell'),
    #},
    #{   # fitblobs for south < -28, knl regular
    #    'name': PREFIX + '9-regular-knl',
    #    'qname': PREFIX + '9',
    #    'prev_stage': None,
    #    'tasks_per_nodehr': 0.5,
    #    'cores_per_worker': 17,
    #    'cores_per_worker_actual': 17,
    #    'mem_per_worker': KNL_MEM // 4,
    #    'job_duration': 23,
    #    'arch': 'knl',
    #    'write_stage': ['fitblobs'],
    #    'stage': 'fitblobs',
    #    'max_nodes_per_job': 50,
    #    'max_number_of_jobs': 0,
    #    'qos': regular(),
    #},
    #{   # fitblobs for south < -28, realtime
    #    'name': PREFIX + '9-realtime',
    #    'qname': PREFIX + '9',
    #    'prev_stage': None,
    #    'tasks_per_nodehr': 1,
    #    'cores_per_worker': 8,
    #    'cores_per_worker_actual': 8,
    #    'mem_per_worker': HASWELL_MEM // 4,
    #    'job_duration': 12,
    #    'arch': 'haswell',
    #    'write_stage': ['fitblobs'],
    #    'stage': 'fitblobs',
    #    'max_nodes_per_job': 11,
    #    'max_number_of_jobs': 0,
    #    'qos': realtime(),
    #},
    {   # post-fitblobs for south < -28
        'name': PREFIX + '10',
        'qname': PREFIX + '10',
        'prev_stage': None,
        'tasks_per_nodehr': 1,
        'cores_per_worker': 8,
        'cores_per_worker_actual': 8,
        'mem_per_worker': HASWELL_MEM // 4,
        'job_duration': 24,
        'arch': 'haswell',
        'write_stage': ['srcs'],
        'stage': 'writecat',
        'max_nodes_per_job': 1,
        'max_number_of_jobs': 0,
        'qos': reservation('dr9_haswell'),
    },
    {   # finish, realtime
        'name': PREFIX + '10-realtime',
        'qname': PREFIX + '10',
        'prev_stage': None,
        'tasks_per_nodehr': 1,
        'cores_per_worker': 16,
        'cores_per_worker_actual': 16,
        'mem_per_worker': HASWELL_MEM // 2,
        'job_duration': 12,
        'arch': 'haswell',
        'write_stage': ['srcs'],
        'stage': 'writecat',
        'max_nodes_per_job': 12,
        'max_number_of_jobs': 0,
        'qos': realtime(),
    },
    {   # finish, resv
        'name': PREFIX + '10-resv',
        'qname': PREFIX + '10',
        'prev_stage': None,
        'tasks_per_nodehr': 0.01,
        'cores_per_worker': 32,
        'cores_per_worker_actual': 4,
        'mem_per_worker': HASWELL_MEM * 2,
        'job_duration': 24,
        'arch': 'haswell',
        'write_stage': ['srcs', 'coadds, wise_forced'],
        'stage': 'writecat',
        'max_nodes_per_job': 1,
        'max_number_of_jobs': 0,
        'qos': reservation('dr9_finish_haswell2'),
        'allocation': 'm3592',
    },
    {   # finish, regular
        'name': PREFIX + '10-regular',
        'qname': PREFIX + '10',
        'prev_stage': None,
        'tasks_per_nodehr': .00001,
        'cores_per_worker': 32,
        'cores_per_worker_actual': 4,
        'mem_per_worker': HASWELL_MEM * 2,
        'job_duration': 48,
        'arch': 'haswell',
        'write_stage': ['srcs', 'coadds', 'wise_forced'],
        'stage': 'writecat',
        'max_nodes_per_job': 10,
        'max_number_of_jobs': 0,
        'qos': regular(),
        'allocation': 'm3592',
    },
    {
        'name': PREFIX + '10-bigmem',
        'qname': PREFIX + '10-bigmem',
        'prev_stage': None,
        'task_srcs': [
            TaskSource(PREFIX + '10', ['Failed'])
        ],
        'tasks_per_nodehr': 0.01,
        'cores_per_worker': 8,
        'mem_per_worker': HASWELL_MEM*10,
        'write_stage': ['srcs', 'fitblobs', 'coadds', 'wise_forced'],
        'cores_per_worker_actual': 8,
        'job_duration': 72,
        'max_nodes_per_job': 1,
        'max_number_of_jobs': 0,
        'arch': 'haswell',
        'allocation': 'm3592',
        'qos': bigmem()
    },
    {
        'name': PREFIX + '10-cmem',
        'qname': PREFIX + '10-bigmem',
        'prev_stage': None,
        'tasks_per_nodehr': 0.01,
        'cores_per_worker': 32,
        'mem_per_worker': HASWELL_MEM*100,
        'write_stage': ['srcs', 'fitblobs', 'coadds', 'wise_forced'],
        'cores_per_worker_actual': 32,
        'job_duration': 48,
        'max_nodes_per_job': 1,
        'max_number_of_jobs': 0,
        'arch': 'amd',
        'allocation': 'desi',
        'qos': '-q bigmem '
    },
    {   # post-fitblobs for south < -28
        'name': PREFIX + 'interactive',
        'qname': PREFIX + 'interactive',
        'prev_stage': None,
        'task_srcs': [],
        'tasks_per_nodehr': 1,
        'cores_per_worker': 16,
        'cores_per_worker_actual': 16,
        'mem_per_worker': HASWELL_MEM // 2,
        'job_duration': 24,
        'arch': 'haswell',
        'write_stage': ['fitblobs'],
        'stage': 'writecat',
        'max_nodes_per_job': 1,
        'max_number_of_jobs': 0,
        'qos': reservation('dr9_haswell'),
    }],
    #{
    #    'name': PREFIX + '3',
    #    'prev_stage': PREFIX + '2',
    #    'tasks_per_nodehr': 1/6,
    #    'cores_per_worker': 32,
    #    'cores_per_worker_actual': 8,
    #    'mem_per_worker': HASWELL_MEM // 4,
    #    'job_duration': 6,
    #    'write_stage': 'srcs',
    #    'arch': 'haswell',
    #    'max_nodes_per_job': 80,
    #}],
    {
        'allocation': 'desi',
        #'allocation': 'm3592',
        'qos': regular(),
        'class': RunbrickPyStage,
        'max_number_of_jobs': 10,
        'stage': 'writecat',
        'write_stage': [],
        'task_srcs': [],
        'revive_all': False,
    }
)
