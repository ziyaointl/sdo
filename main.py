from settings import *
from stages import *
from dashboard import render

def main():
    """Here, cores_per_worker indicates how many cores are allocated to each worker.
    For the actual number of cores (processes) used, refer to init.py
    """
    sentinel = SentinelStage()
    #prefarm = PreFarmStage(PREFARM_QNAME, sentinel, 16, job_duration=4, cores_per_worker=8)
    #prefarm_scavenger_one = PreFarmScavengerStage(PREFARM_SCAVENGER_ONE_QNAME, prefarm, 8, job_duration=4, cores_per_worker=17)
    #prefarm_scavenger_two = PreFarmScavengerStage(PREFARM_SCAVENGER_TWO_QNAME, prefarm_scavenger_one, 1/6, job_duration=12, cores_per_worker=17)
    #farm = FarmStage(FARM_QNAME, prefarm_scavenger_two, 4)
    postfarm = PostFarmStage(POSTFARM_QNAME, sentinel, 3.5, cores_per_worker=17, arch='knl', max_nodes_per_job=40)
    postfarm_scavenger_one = PostFarmScavengerStage(POSTFARM_SCAVENGER_ONE_QNAME,
        postfarm, 1/4, job_duration=4, cores_per_worker=34, arch='knl', max_nodes_per_job=40)
    postfarm_scavenger_two = PostFarmScavengerStage(POSTFARM_SCAVENGER_TWO_QNAME,
        postfarm_scavenger_one, 1/6, job_duration=6, cores_per_worker=32, arch='haswell', max_nodes_per_job=40)
    stages = [postfarm, postfarm_scavenger_one, postfarm_scavenger_two]

    for s in stages:
        s.add_tasks()
        is_done = s.is_done()
        print('Stage:', s.name, ', Done?', is_done,
              ', Retries:', s.get_current_retries())
        if is_done:
            continue
        s.attempt_recover()
        s.schedule_jobs()
        s.print_status()
    render(stages)

main()
