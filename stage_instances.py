from stages import SentinelStage, PostFarmStage, PostFarmScavengerStage

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
            revive_all=sdef['revive_all']
        )
        stages_list.append(curr_stage)
        stages_dict[sdef['name']] = curr_stage

    return stages_list

# Constants
PREFIX = 'ziyao-dr9m-south-'
HASWELL_MEM = 125000000
KNL_MEM = 93750000

# QOS choices
def regular():
    return "-q regular"

def debug():
    return "-q debug"

def realtime():
    return "-q realtime --exclusive"

def reservation(name):
    return "--reservation " + name

# Generate stage instances
stage_instances = gen_stages(
    [{
        'name': PREFIX + '0',
        'prev_stage': None,
        'tasks_per_nodehr': 20,
        'cores_per_worker': 8,
        'cores_per_worker_actual': 8,
        'mem_per_worker': HASWELL_MEM // 4,
        'job_duration': 0.5,
        'arch': 'haswell',
        'max_nodes_per_job': 20,
        'max_number_of_jobs': 4,
        'stage': 'tims',
        'write_stage': 'tims',
        'qos': debug(),
        'revive_all': True,
    },
    {
        'name': PREFIX + '1',
        'prev_stage': None,
        'tasks_per_nodehr': 3.5,
        'cores_per_worker': 17,
        'cores_per_worker_actual': 17,
        'mem_per_worker': KNL_MEM // 4,
        'job_duration': 4,
        'arch': 'knl',
        'write_stage': 'srcs',
        'max_nodes_per_job': 80,
        'max_number_of_jobs': 1,
    },
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
    #    'write_stage': 'srcs'
    #    'qos': reservation('dr9_priority'),
    #},
    {
        'name': PREFIX + '4',
        'prev_stage': None,
        'tasks_per_nodehr': 1/4,
        'cores_per_worker': 34,
        'cores_per_worker_actual': 34,
        'mem_per_worker': KNL_MEM // 2,
        'job_duration': 7,
        'arch': 'knl',
        'max_nodes_per_job': 350,
        'max_number_of_jobs': 1,
        'stage': 'fitblobs',
        'write_stage': 'fitblobs',
        'class': PostFarmStage,
        'qos': reservation('dr9_priority'),
    },
    {
        'name': PREFIX + '5',
        'prev_stage': None,
        'tasks_per_nodehr': 2,
        'cores_per_worker': 8,
        'cores_per_worker_actual': 8,
        'mem_per_worker': KNL_MEM // 8,
        'job_duration': 4,
        'arch': 'knl',
        'max_nodes_per_job': 20,
        'max_number_of_jobs': 1,
        'class': PostFarmStage,
        'qos': reservation('dr9_priority'),
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
        'qos': regular(),
        'class': PostFarmScavengerStage,
        'max_number_of_jobs': 10,
        'stage': 'writecat',
        'write_stage': None,
        'revive_all': False,
    }
)
