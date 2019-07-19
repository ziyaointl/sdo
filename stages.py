from settings import *
from util import *
import math
import subprocess
import qdo
import sqlite3
import csv
import os
import io

class Stage:
    def __init__(self, name, previous_stage, tasks_per_nodehr):
        self.name = name
        self.previous_stage = previous_stage
        self.tasks_per_nodehr = tasks_per_nodehr

    def is_done(self):
        raise NotImplementedError
    
    def add_tasks(self):
        raise NotImplementedError

    def attempt_recover(self):
        raise NotImplementedError

    def schedule_jobs(self):
        raise NotImplementedError
    
    def schedule_one_job(self):
        raise NotImplementedError

    def print_status(self):
        raise NotImplementedError

class SentinelStage(Stage):
    def __init__(self):
        self.previous_stage = self
        self.name = 'Sentinel'

    def is_done(self):
        return True

    def print_status(self):
        print('I am the sentinel. I am always 100% complete.')

class QdoCentricStage(Stage):
    auto_create_queue = False

    def __init__(self, name, previous_stage, tasks_per_nodehr,
        job_duration=2, max_nodes_per_job=20, max_number_of_jobs=100, cores_per_worker=17):
        # self.queue = qdo.connect('name')
        self.cores_per_worker = cores_per_worker
        self.job_duration = job_duration
        self.max_nodes_per_job = max_nodes_per_job
        self.max_number_of_jobs = max_number_of_jobs
        return super().__init__(name, previous_stage, tasks_per_nodehr)

    def attempt_recover(self):
        # Check if previous stage is done and if no jobs are pending/running
        pending_tasks = self.queue.status()['ntasks']['Pending']
        if (self.previous_stage.is_done()
                and pending_tasks == 0
                and len(self.get_jobs_in_queue()) == 0
                and get_current_retries() < MAX_RETRIES):
            command = 'qdo recover {}'.format(self.name)
            output = run_command(command)
            print(output)
            self.increment_retries()

    def is_done(self):
        """
        This stage is done if
            Previous stage is done
        AND
                No tasks are pending or running
            OR
                No tasks are pending, no jobs are in slurm queue,
                but MAX_RETRIES has been reached
        """
        pending_tasks = self.queue.status()['ntasks']['Pending']
        runnning_tasks = self.queue.status()['ntasks']['Running']
        finished_last_retry = (pending_tasks == 0
                            and len(self.get_jobs_in_queue()) == 0
                            and get_current_retries() >= MAX_RETRIES)
        return (self.previous_stage.is_done() and
                ((runnning_tasks == 0 and pending_tasks == 0) or finished_last_retry))

    def get_jobs_in_queue(self):
        # command = 'squeue -u ziyaoz --format=%all'
        # output = run_command(command)
        # Use dummy output for debugging purposes
        output = """ACCOUNT|TRES_PER_NODE|MIN_CPUS|MIN_TMP_DISK|END_TIME|FEATURES|GROUP|OVER_SUBSCRIBE|JOBID|NAME|COMMENT|TIME_LIMIT|MIN_MEMORY|REQ_NODES|COMMAND|PRIORITY|QOS|REASON||ST|USER|RESERVATION|WCKEY|EXC_NODES|NICE|S:C:T|JOBID|EXEC_HOST|CPUS|NODES|DEPENDENCY|ARRAY_JOB_ID|GROUP|SOCKETS_PER_NODE|CORES_PER_SOCKET|THREADS_PER_CORE|ARRAY_TASK_ID|TIME_LEFT|TIME|NODELIST|CONTIGUOUS|PARTITION|PRIORITY|NODELIST(REASON)|START_TIME|STATE|UID|SUBMIT_TIME|LICENSES|CORE_SPEC|SCHEDNODES|WORK_DIR
                desi|craynetwork:1|1|0|2019-07-18T19:13:59|haswell|ziyaoz|NO|23159825|sh|(null)|4:00:00|118G||(null)|0.00001609348692|interactive|None||R|ziyaoz|(null)|(null)||0|*:*:*|23159825|mom5|64|1||23159825|79963|*|*|*|N/A|3:53:54|6:06|nid00105|0|interactive|69121|nid00105|2019-07-18T15:13:59|RUNNING|79963|2019-07-18T15:13:59|(null)|N/A|(null)|/global/u2/z/ziyaoz
                desi|craynetwork:1|1|0|N/A|haswell|ziyaoz|NO|23160238|ziyao-big-blobs|(null)|1-16:00:00|0||/global/cscratch1/sd/landriau/dr8/code/qdo/qdo/etc/qdojob-shifter|0.00001508858987|regular_1|Priority||PD|ziyaoz|(null)|(null)||0|*:*:*|23160238|n/a|1|1||23160238|79963|*|*|*|N/A|1-16:00:00|0:00||0|regular|64805|(Priority)|N/A|PENDING|79963|2019-07-18T15:42:40|(null)|N/A|(null)|/global/u2/z/ziyaoz"""
        csv_r = csv.DictReader(io.StringIO(output), delimiter='|')
        jobs = []
        for row in csv_r:
            # Disable name checking for debugging purposes
            # if row['JobName'] == self.name:
            jobs.append(row)
        return jobs

    def get_current_retries(self):
        """Fetch from the database how many retries this stage has performed
        """
        conn = sqlite3.connect('sdo.db')
        c = conn.cursor()
        c.execute('SELECT times FROM retries WHERE stage=?', (self.name,))
        result = c.fetchone()
        if result == None:
            c.execute('INSERT INTO retries VALUES(?, 0)', (self.name, ))
            result = 0
        else:
            result = result[0]
        conn.close()
        return result

    def increment_retries(self):
        conn = sqlite3.connect('sdo.db')
        c = conn.cursor()
        curr_retries = self.get_current_retries() + 1
        c.execute('UPDATE retries SET times=? WHERE stage=?',
                    (curr_retries, self.name))
        conn.close()
        return curr_retries

class PreFarmStage(QdoCentricStage):
    def add_tasks(self):
        """No need to add tasks
        Bricks that needs to be processed are manually stored in a qdo queue
        named self.name
        """
        pass

    def schedule_jobs(self):
        """Calculate the number of hours to schedule and schedules them
        """
        # Check for number of unfinished hours in the queue
        jobs = self.get_jobs_in_queue()
        total_time_in_queue = timedelta()
        for j in jobs:
            total_time_in_queue += parse_timedelta(j['TIME_LEFT']) * int(j['NODES'])
        print(hours(total_time_in_queue), 'hours in queue')
        # Check for number of tasks that are waiting in the queue
        # pending_tasks = self.queue.status()['ntasks']['Pending']
        pending_tasks = 2340
        # Estimate how many nodehrs needs to be scheduled
        node_hrs_needed = pending_tasks / self.tasks_per_nodehr
        node_hrs_to_schedule = node_hrs_needed - hours(total_time_in_queue)
        # Schedule new jobs
        self.schedule_nodehrs(node_hrs_to_schedule)

    def schedule_nodehrs(self, nodehrs):
        """Schedule a specified number of nodehrs
        """
        # Schedule jobs that uses max_nodes_per_job
        nodehr_per_job = self.max_nodes_per_job * self.job_duration
        while nodehrs > nodehr_per_job:
            self.schedule_one_job(self.max_nodes_per_job, self.job_duration)
            nodehrs -= nodehr_per_job
        # Schedule a job that uses fewer nodes than max_nodes_per_job
        if nodehr_per_job != 0:
            self.schedule_one_job(math.ceil(nodehrs / self.job_duration),
                                    self.job_duration)

    def schedule_one_job(self, nodes, hrs, dryrun=True):
        """Schedule one job
        """
        if dryrun:
            print('Dry Run: ', end='')
        print('Scheduled {}, {} nodes, {} hrs'.format(self.name, nodes, hrs))
        if not dryrun:
            script_path = os.path.join(SDO_DIR, '{}.sh'.format(self.name))
            command = ('QDO_BATCH_PROFILE=cori-shifter qdo launch -v {0} 40'
                '--cores_per_worker {1} --walltime=0:{2}:00'
                '--batchqueue=regular --keep_env '
                '--batchopts "--image=docker:legacysurvey/legacypipe:nersc-dr8.3.2"'
                '--script "{3}"')
            command = command.format(self.name, self.cores_per_worker, hrs, SDO_DIR, script_path)
            output = run_command(command)
            print(output)
