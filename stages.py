from settings import *
import subprocess
import qdo
import sqlite3

class Stage:
    def __init__(self, name, previous_stage):
        self.name = name
        self.previous_stage = previous_stage

    def is_done(self):
        raise NotImplementedError
    
    def add_tasks(self):
        raise NotImplementedError

    def attempt_recover():
        raise NotImplementedError

    def schedule_jobs():
        raise NotImplementedError
    
    def print_status():
        raise NotImplementedError

class SentinelStage(Stage):
    def __init__(self):
        self.previous_stage = self
        self.name = 'Sentinel'

    def is_done(self):
        return True

    def print_status():
        print('I am the sentinel. I am always 100% complete.')

class PreFarmStage(Stage):
    def __init__(self, name, previous_stage):
        self.queue = qdo.connect('name')
        return super().__init__(name, previous_stage)
    
    def add_tasks(self):
        """No need to add tasks
        Bricks that needs to be processed are manually stored in a qdo queue
        named self.name
        """
        pass
    
    def schedule_jobs(self):
        pass

    def is_done(self):
        n_pending = self.queue.status()['ntasks']['Pending']
        n_running = self.queue.status()['ntasks']['Running']
        return ((n_running == 0 and n_pending == 0) or
            (n_pending == 0 and get_current_retries() >= MAX_RETRIES))

    def get_current_retries(self):
        conn = sqlite3.connect('sdo.db')
        c = conn.cursor()
        c.execute('SELECT * FROM retries WHERE stage=?', self.name)
        print(c.fetchone())
