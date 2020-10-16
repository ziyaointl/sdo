from settings import *
from stages import *
from dashboard import render
from init import init

def main():
    """Here, cores_per_worker indicates how many cores are allocated to each worker.
    For the actual number of cores (processes) used, refer to init.py
    """
    init()
    sentinel = SentinelStage()
    #prefarm = PreFarmStage(PREFARM_QNAME, sentinel, 16, job_duration=4, cores_per_worker=8)
    #prefarm_scavenger_one = PreFarmScavengerStage(PREFARM_SCAVENGER_ONE_QNAME, prefarm, 8, job_duration=4, cores_per_worker=17)
    #prefarm_scavenger_two = PreFarmScavengerStage(PREFARM_SCAVENGER_TWO_QNAME, prefarm_scavenger_one, 1/6, job_duration=12, cores_per_worker=17)
    #farm = FarmStage(FARM_QNAME, prefarm_scavenger_two, 4)
    postfarm = PostFarmStage(
        POSTFARM_QNAME,
        sentinel,
        tasks_per_nodehr=3.5,
        cores_per_worker=17,
        job_duration=4,
        arch='knl',
        max_nodes_per_job=80
    )
    postfarm_scavenger_one = PostFarmScavengerStage(
        POSTFARM_SCAVENGER_ONE_QNAME,
        postfarm,
        tasks_per_nodehr=1/4,
        job_duration=4,
        cores_per_worker=34,
        arch='knl',
        max_nodes_per_job=80
    )
    postfarm_scavenger_two = PostFarmScavengerStage(
        POSTFARM_SCAVENGER_TWO_QNAME,
        postfarm_scavenger_one,
        tasks_per_nodehr=1/6,
        job_duration=6,
        cores_per_worker=32,
        arch='haswell',
        max_nodes_per_job=80
    )

    stages = [postfarm, postfarm_scavenger_one, postfarm_scavenger_two]

    separator_len = 40
    for s in stages:
        print('='*separator_len)
        print(s.name)
        print('-'*separator_len)

        s.add_tasks() # Add tasks from previous stage
        s.revive_or_archive() # Revive killed tasks or move them to failed
        s.schedule_jobs() # Schedule new jobs if needed
        s.print_status()
        print('='*separator_len + '\n')
    render(stages)

main()
