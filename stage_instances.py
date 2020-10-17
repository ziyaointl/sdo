from stages import SentinelStage, PostFarmScavengerStage

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
            job_duration=sdef['job_duration'],
            arch=sdef['arch'],
            max_nodes_per_job=sdef['max_nodes_per_job'],
        )
        stages_list.append(curr_stage)
        stages_dict[sdef['name']] = curr_stage

    return stages_list

prefix = 'ziyao-dr9m-south-'
stage_instances = gen_stages(
    [{
        'name': prefix + '1',
        'prev_stage': None,
        'tasks_per_nodehr': 3.5,
        'cores_per_worker': 17,
        'job_duration': 4,
        'arch': 'knl',
        'max_nodes_per_job': 80,
    },
    {
        'name': prefix + '2',
        'prev_stage': prefix + '1',
        'tasks_per_nodehr': 1/4,
        'job_duration': 4,
        'cores_per_worker': 34,
        'arch': 'knl',
        'max_nodes_per_job': 80,
    },
    {
        'name': prefix + '3',
        'prev_stage': prefix + '2',
        'tasks_per_nodehr': 1/6,
        'job_duration': 6,
        'cores_per_worker': 32,
        'arch': 'haswell',
        'max_nodes_per_job': 80,
    }],
    {
        'allocation': 'desi', # TODO
        'queue': 'regular', # TODO
        'class': PostFarmScavengerStage,
    }
)
