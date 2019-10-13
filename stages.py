from settings import *
from util import run_command, cached_run_command, parse_timedelta, hours
from qdo_util import transfer_queue, set_all_tasks_with_state
from datetime import timedelta
from gen_farm_script import gen_farm_script
import math
import subprocess
import qdo
import qdo_util
import sqlite3
import csv
import os
import io
import re

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
    # TODO: Move cores_per_worker and arch to RunbrickPyStage
    auto_create_queue = False
    max_nodes_per_job = 20

    def __init__(self, name, previous_stage, tasks_per_nodehr,
        job_duration=2, max_number_of_jobs=100,
        cores_per_worker=17, arch='knl'):
        """name: name of the qdo queue; also used for calling the default
        job scheudling script and for distinguishing jobs scheduled by
        different stages
        """
        self.cores_per_worker = cores_per_worker
        self.job_duration = job_duration
        self.max_number_of_jobs = max_number_of_jobs
        self.arch=arch
        try:
            self.queue = qdo.connect(name)
        except ValueError:
            if self.auto_create_queue:
                self.queue = qdo.create(name)
            else:
                raise
        super().__init__(name, previous_stage, tasks_per_nodehr)

    def attempt_recover(self):
        # Check if previous stage is done and if no jobs are pending/running
        pending_tasks = self.queue.status()['ntasks']['Pending']
        if (self.previous_stage.is_done()
                and pending_tasks == 0
                and len(self.get_jobs_in_queue()) == 0
                and self.get_current_retries() < MAX_RETRIES):
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
                All jobs have finished and maximum retry reached
        """
        pending_tasks = self.queue.status()['ntasks']['Pending']
        runnning_tasks = self.queue.status()['ntasks']['Running']
        finished_last_retry = (pending_tasks == 0 # Pending tasks = 0 indicates
                                                  # no new jobs will be scheduled
                            and len(self.get_jobs_in_queue()) == 0
                            and self.get_current_retries() >= MAX_RETRIES)
        if finished_last_retry:
            set_all_tasks_with_state(self.queue, 'Running', 'Failed')
        return (self.previous_stage.is_done() and
                ((runnning_tasks == 0 and pending_tasks == 0) or finished_last_retry))

    def get_jobs_in_queue(self):
        command = 'squeue -u ziyaoz --format=%all'
        output = cached_run_command(command)
        if 'error' in output:
            raise RuntimeError('Slurm not available')
        csv_r = csv.DictReader(io.StringIO(output), delimiter='|')
        jobs = []
        for row in csv_r:
            if row['NAME'] == self.name or row['NAME'] == self.name+'.sh':
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
            conn.commit()
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
        conn.commit()
        conn.close()
        print('Retries incremented to', curr_retries)
        return curr_retries

    def get_time_in_queue(self):
        """Returns the total time scheduled, in timedelta
        """
        jobs = self.get_jobs_in_queue()
        total_time_in_queue = timedelta()
        for j in jobs:
            total_time_in_queue += parse_timedelta(j['TIME_LEFT']) * int(j['NODES'])
        return total_time_in_queue

    def schedule_jobs(self):
        """Calculate the number of hours to schedule and schedules them
        """
        # Check for number of unfinished hours in the queue
        total_time_in_queue = self.get_time_in_queue()
        print('Node hrs already scheduled, waiting to run: ', hours(total_time_in_queue))
        # Check for number of tasks that are waiting in the queue
        pending_tasks = self.queue.status()['ntasks']['Pending']
        print('Pending tasks:', pending_tasks)
        # Estimate how many nodehrs needs to be scheduled
        node_hrs_needed = pending_tasks / self.tasks_per_nodehr
        node_hrs_to_schedule = node_hrs_needed - hours(total_time_in_queue)
        print('Node hrs to schedule:', node_hrs_to_schedule)
        # Schedule new jobs
        self.schedule_nodehrs(node_hrs_to_schedule, schedule_remainder=(self.previous_stage.is_done()
                and self.number_of_jobs_in_queue() == 0))

    def schedule_nodehrs(self, nodehrs, schedule_remainder=False):
        """Schedule a specified number of nodehrs
        schedule_remainder: schedule jobs smaller than max_nodes_per_job
        """
        # Schedule jobs that uses max_nodes_per_job
        nodehr_per_job = self.max_nodes_per_job * self.job_duration
        curr_jobs_in_queue = self.number_of_jobs_in_queue()
        while nodehrs > nodehr_per_job and curr_jobs_in_queue < self.max_number_of_jobs:
            self.schedule_one_job(self.max_nodes_per_job, self.job_duration, dryrun=False)
            nodehrs -= nodehr_per_job
            curr_jobs_in_queue += 1
        # Schedule a job that uses fewer nodes than max_nodes_per_job
        if nodehrs > 0 and schedule_remainder and curr_jobs_in_queue < self.max_number_of_jobs:
            self.schedule_one_job(math.ceil(nodehrs / self.job_duration),
                                    self.job_duration, dryrun=False)

    def number_of_jobs_in_queue(self):
        return len(self.get_jobs_in_queue())

    def add_tasks_from_previous_queue(self, task_state):
        """If previous stage is QdoCentricStage, add tasks of a certain state
        from the previous stage's queue to this stage's queue
        """
        self.add_tasks_from_previous_queues(1, task_state)

    def add_tasks_from_previous_queues(self, queues, task_state):
        prev_stage = self.previous_stage
        for _ in range(queues):
            assert isinstance(self.previous_stage,
                QdoCentricStage), "Previous stage is not QdoCentricStage"
            transfer_queue(self.queue, prev_stage.queue, task_state)
            prev_stage = prev_stage.previous_stage

    def print_status(self):
        print('Status for queue', self.queue.name)
        print(self.queue.status())
        print('='*40)
    
    def record_job(self, command_output):
        """Record the scheduled job id into database
        """
        p = re.compile('Submitted batch job (\d+)')
        m = p.search(command_output)
        if m:
            jobid = m.group(1)
            conn = sqlite3.connect('sdo.db')
            c = conn.cursor()
            c.execute('INSERT INTO jobs(stage, jobid) VALUES(?, ?)', (self.name, jobid))
            conn.commit()
            conn.close()
            print('Recorded jobid', jobid)

class RunbrickPyStage(QdoCentricStage):
    def schedule_one_job(self, nodes, hrs, dryrun=True):
        """Schedule one job
        """
        if dryrun:
            print('Dry Run: ', end='')
        print('Scheduled {}, {} nodes, {} hrs'.format(self.name, nodes, hrs))
        # {0} queuename
        # {1} cores_per_worker
        # {2} walltime in minutes
        # {3} script directory
        # {4} nworkers
        # {5} profile
        # {6} image profile
        script_path = os.path.join(SDO_SCRIPT_DIR, '{}.sh'.format(self.name))
        profile = 'cori-knl-shifter' if self.arch == 'knl' else 'cori-shifter'
        account = KNL_ACCT if self.arch == 'knl' else HASWELL_ACCT
        cores = 68 if self.arch == 'knl' else 32
        nworkers = (cores // self.cores_per_worker) * nodes
        command = ('QDO_BATCH_PROFILE={5} qdo launch -v {0} {4} '
            '--cores_per_worker {1} --walltime=0:{2}:00 '
            '--batchqueue=regular --keep_env '
            '--batchopts "--image=docker:legacysurvey/legacypipe:{6} --account={7}" '
            '--script "{3}"')
        command = command.format(self.name, self.cores_per_worker, int(hrs * 60),
                                    script_path, nworkers, profile, IMAGE_TAG, account)
        if dryrun:
            print(command)
        else:
            output = run_command(command)
            self.record_job(output)

class PreFarmStage(RunbrickPyStage):
    def add_tasks(self):
        """No need to add tasks
        Bricks that needs to be processed are manually stored in a qdo queue
        named self.name
        """
        pass

class PreFarmScavengerStage(RunbrickPyStage):
    auto_create_queue = True
    def add_tasks(self):
        self.add_tasks_from_previous_queue('Failed')

class PostFarmStage(RunbrickPyStage):
    auto_create_queue = True
    def add_tasks(self):
        """Take tasks that are done from the farm stage and put them in
        the queue
        """
        self.add_tasks_from_previous_queue('Succeeded')

class PostFarmScavengerStage(RunbrickPyStage):
    max_nodes_per_job = 1
    auto_create_queue = True
    def add_tasks(self):
        """Take tasks that failed from the postfarm stage and put them in the queue
        """
        self.add_tasks_from_previous_queue('Failed')

class FarmStage(QdoCentricStage):
    auto_create_queue = True
    max_nodes_per_job = 24

    def add_tasks(self):
        self.add_tasks_from_previous_queues(3, 'Succeeded')

    def schedule_one_job(self, nodes, hrs, dryrun=True):
        script_path = gen_farm_script(FARM_QNAME, nodes, int(hrs*60), 'regular',
                IMAGE_TAG, SDO_SCRIPT_DIR, 'haswell', 64)
        command = 'sbatch {}'.format(script_path)
        if dryrun:
            print(command)
        else:
            output = run_command(command)
            self.record_job(output)

    def number_of_jobs_in_queue(self):
        return len(self.get_jobs_in_queue()) / 2
