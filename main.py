from settings import *
from stages import *

def main():
    sentinel = SentinelStage()
    prefarm = PreFarmStage(PREFARM_QNAME, sentinel, 8)
    farm = FarmStage(FARM_QNAME, prefarm, 5)
    postfarm = PostFarmStage(POSTFARM_QNAME, farm, 12)
    postfarm_scavenger_one = PostFarmScavengerStage(POSTFARM_SCAVENGER_ONE_QNAME,
        postfarm, 6, job_duration=4, cores_per_worker=32, arch='haswell')
    postfarm_scavenger_two = PostFarmScavengerStage(POSTFARM_SCAVENGER_TWO_QNAME,
        postfarm_scavenger_one, 2, job_duration=4, cores_per_worker=64, arch='haswell')
    stages = [prefarm, farm, postfarm, postfarm_scavenger_one, postfarm_scavenger_two]

    for s in stages:
        s.add_tasks()
        if s.is_done():
            continue
        if s.previous_stage.is_done():
            s.attempt_recover()
        s.schedule_jobs()
        s.print_status()

main()
