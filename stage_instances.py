from stages import SentinelStage, RunbrickPyStage
from dataclasses import dataclass
from typing import Dict, List, Union

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

@dataclass
class TaskSource:
    """Format: TaskSource(queue_name, ['Failed', 'Running', 'Succeeded', 'Killed'])
    """
    name: str
    states: List[str]

@dataclass
class StageDefinition:
    # Basic info
    name: str # Name of the stage
    qname: str # qdo queue for the stage
    prev_stage: Union[str, None] # Optional: name of the previous stage; this stage would not start scheduling if previous stage did not finish
    task_srcs: List[TaskSource] # Optional: automatically import tasks from previous queues
    # Resource allocation per brick
    tasks_per_nodehr: int # Estimated number of bricks completed per nodehr
    cores_per_worker: int # Number of cores to allocated per brick
    cores_per_worker_actual: int # Number of cores actually allowed per brick (to prevent memory exhaustion)
    mem_per_worker: int # Amount of memory to allocate per brick # TODO: automate this?
    # Job configuration
    job_duration: float # Duration of a job, in hours
    max_nodes_per_job: int # Max number of nodes to request per job
    max_number_of_jobs: int # Max number of jobs that the queue can contain at a time # TODO: Remove this?
    arch: str # Architecture of the nodes to request, could be haswell, knl, or amd # TODO: Create an enum/class for this
    qos: str # NERSC queue type, should be a call to QOS functions above
    allocation: str # Which NERSC allocation to use for this stage
    # Runbrick configuration
    stage: str # Stop runbrick at this stage; For stage names, see https://github.com/legacysurvey/legacypipe/blob/main/py/legacypipe/runbrick.py
    write_stage: List[str] # A list of stages at which we'd like checkpoint
    # Job automation
    revive_all: bool # Whether to revive killed tasks; TODO: deprecate?


def gen_stages(stages_def: List[StageDefinition]) -> List[RunbrickPyStage]:
    """Given stage definitions, generate a list of stage instances
    """
    stages_dict: Dict[str, RunbrickPyStage] = {}
    stages_list: List[RunbrickPyStage] = []

    for sdef in stages_def:
        curr_stage = RunbrickPyStage(
            sdef.name,
            sdef.qname,
            SentinelStage() if sdef.prev_stage is None else stages_dict[sdef.prev_stage],
            tasks_per_nodehr=sdef.tasks_per_nodehr,
            cores_per_worker=sdef.cores_per_worker,
            cores_per_worker_actual=sdef.cores_per_worker_actual,
            job_duration=sdef.job_duration,
            arch=sdef.arch,
            max_nodes_per_job=sdef.max_nodes_per_job,
            allocation=sdef.allocation,
            max_number_of_jobs=sdef.max_number_of_jobs,
            qos=sdef.qos,
            stage=sdef.stage,
            write_stage=sdef.write_stage,
            revive_all=sdef.revive_all,
            mem_per_worker=sdef.mem_per_worker,
            task_srcs=sdef.task_srcs,
        )
        stages_list.append(curr_stage)
        stages_dict[sdef.name] = curr_stage

    return stages_list

# Constants
PREFIX = 'ziyao-dr9m-south-'
HASWELL_MEM = 125000000
KNL_MEM = 93750000

stage_instances = gen_stages(
    [ StageDefinition(
        name=PREFIX + '0',
        qname=PREFIX + '0',
        prev_stage=None,
        task_srcs=[],
        tasks_per_nodehr=8,
        cores_per_worker=8,
        cores_per_worker_actual=8,
        mem_per_worker=HASWELL_MEM // 4,
        job_duration=2,
        max_nodes_per_job=40,
        max_number_of_jobs=10,
        arch='haswell',
        qos=regular(),
        allocation='desi',
        stage='srcs',
        write_stage=['srcs', 'tims'],
        revive_all=False,
    )]
)
