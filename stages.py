from settings import *
from util import run_command, cached_run_command, parse_timedelta, hours
from qdo_util import transfer_queue, set_all_tasks_with_state, record_all_tasks_with_state, get_tasks_with_state
from datetime import timedelta
from gen_farm_script import gen_farm_script
from pprint import pprint
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

    def revive_or_archive(self):
        raise NotImplementedError

    def add_tasks(self):
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

    def __init__(self, name, previous_stage, tasks_per_nodehr=4,
        job_duration=2, max_number_of_jobs=15,
        cores_per_worker=17, arch='knl', max_nodes_per_job=20):
        """name: name of the qdo queue; also used for calling the default
        job scheudling script and for distinguishing jobs scheduled by
        different stages
        """
        self.cores_per_worker = cores_per_worker
        self.job_duration = job_duration
        self.max_number_of_jobs = max_number_of_jobs
        self.arch=arch
        self.max_nodes_per_job = max_nodes_per_job
        try:
            self.queue = qdo.connect(name)
        except ValueError:
            if self.auto_create_queue:
                self.queue = qdo.create(name)
            else:
                raise
        super().__init__(name, previous_stage, tasks_per_nodehr)

    def all_jobs_pending(self):
        for j in self.get_jobs_in_queue():
            if j['STATE'] != 'PENDING':
                return False
        return True

    def cancel_all_jobs(self):
        for j in self.get_jobs_in_queue():
            output = run_command("scancel {}".format(j['JOBID']))
            print(output)

    def revive_or_archive(self):
        """Either revive or archive killed tasks
        """
        # Move running tasks to killed if no jobs are running
        # Not sure how often this happens, so this code is currently commented out
        #if self.all_jobs_pending():
        #    set_all_tasks_with_state(self.queue, 'Running', 'Killed')

        def create(task):
            conn = sqlite3.connect('sdo.db')
            c = conn.cursor()
            c.execute('INSERT INTO retries (stage, brick) VALUES(?,?)', (self.name, task.task))
            conn.commit()
            conn.close()

        def retry_count(task):
            conn = sqlite3.connect('sdo.db')
            c = conn.cursor()
            c.execute('SELECT count FROM retries WHERE stage=? AND brick=?', (self.name, task.task))
            count = c.fetchone()
            conn.close()
            if count is None:
                create(task)
                return 0
            return count[0]

        def increment_retry_count(task):
            conn = sqlite3.connect('sdo.db')
            c = conn.cursor()
            c.execute('UPDATE retries SET count = count + 1 WHERE stage=? AND brick=?', (self.name, task.task))
            conn.commit()
            conn.close()

        for t in get_tasks_with_state(self.queue, 'Killed'):
            # Move killed tasks that exceeded retry count to failed
            if retry_count(t) >= MAX_RETRIES:
                print('Moving', t.task, 'to failed')
                t.set_state('Failed')
            else: # Add 1 to the retry count of remaning killed tasks
                print('Incrementing retry count for', t.task)
                increment_retry_count(t)

        # Move all killed tasks to pending with higher priority
        # Notice that tasks could get killed before executing the next line, causing a somewhat harmless(?) race
        # One potential solution is to supply a taskfilter to revive
        self.queue.revive(priority=2)

    def is_done(self):
        """
        This stage is done if
            Previous stage is done
        AND
            No tasks are pending or running or killed

        Note: One may want to run revive_or_archive before running this function to clean up killed tasks
        """
        tasks = self.queue.status()['ntasks']
        n_pending = tasks['Pending']
        n_running = tasks['Running']
        n_killed = tasks['Killed']

        done = (self.previous_stage.is_done() and n_pending == 0 and n_running == 0 and n_killed == 0)
        if done:
            self.cancel_all_jobs()
        return done

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
        if self.is_done():
            return

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
        # Only schedule new jobs when previous stage is done
        if self.previous_stage.is_done():
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

    def add_tasks_from_previous_queue(self, task_state, condition=lambda x: True):
        """If previous stage is QdoCentricStage, add tasks of a certain state
        from the previous stage's queue to this stage's queue
        """
        self.add_tasks_from_previous_queues(1, task_state, condition)

    def add_tasks_from_previous_queues(self, queues, task_state, condition=lambda x: True):
        prev_stage = self.previous_stage
        for _ in range(queues):
            if isinstance(self.previous_stage, SentinelStage):
                return
            assert isinstance(self.previous_stage,
                QdoCentricStage), "Previous stage is not QdoCentricStage"
            transfer_queue(self.queue, prev_stage.queue, task_state, condition)
            prev_stage = prev_stage.previous_stage

    def print_status(self):
        pprint(self.queue.status())

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
        batchopts = "--image=docker:legacysurvey/legacypipe:{} --account={}".format(IMAGE_TAG, account)
        batchopts += " --bbf={}".format(BBF) if BURST_BUFFER else ""
        command = ('QDO_BATCH_PROFILE={5} qdo launch -v {0} {4} '
            '--cores_per_worker {1} --walltime=0:{2}:00 '
            '--batchqueue=regular --keep_env '
            '--batchopts "{6}" '
            '--script "{3}"')
        command = command.format(self.name, self.cores_per_worker, int(hrs * 60),
                                    script_path, nworkers, profile, batchopts)
        if dryrun:
            print(command)
        else:
            output = run_command(command)
            self.record_job(output)
            print(output)

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
        def is_timed_out_brick(brick):
            """Check in the database to see if a brick was marked as timed-out
            """
            conn = sqlite3.connect('sdo.db')
            c = conn.cursor()
            c.execute("SELECT * FROM farm_timeouts WHERE brick=?", (brick,))
            matches = c.fetchall()
            return len(matches) > 0

        self.add_tasks_from_previous_queue('Succeeded')
        self.add_tasks_from_previous_queue('Failed', is_timed_out_brick)

class PostFarmScavengerStage(RunbrickPyStage):
    auto_create_queue = True
    def add_tasks(self):
        """Take tasks that failed from the postfarm stage and put them in the queue
        """
        self.add_tasks_from_previous_queue('Failed')

class FarmStage(QdoCentricStage):
    auto_create_queue = True

    def add_tasks(self):
        self.add_tasks_from_previous_queues(3, 'Succeeded')

    def schedule_one_job(self, nodes, hrs, dryrun=True):
        script_path = gen_farm_script(FARM_QNAME, nodes, int(hrs*60), 'regular',
                IMAGE_TAG, SDO_SCRIPT_DIR, 'haswell', 64)
        command = 'sbatch {}'.format(script_path)
        command += " --bbf={}".format(BBF) if BURST_BUFFER else ""
        if dryrun:
            print(command)
        else:
            output = run_command(command)
            self.record_job(output)

    def number_of_jobs_in_queue(self):
        return len(self.get_jobs_in_queue()) / 2
