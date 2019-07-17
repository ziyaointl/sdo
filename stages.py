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

    def attempt_recover(self):
        raise NotImplementedError

    def schedule_jobs(self):
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

    def attempt_recover(self):
        pass

    def is_done(self):
        n_pending = self.queue.status()['ntasks']['Pending']
        n_running = self.queue.status()['ntasks']['Running']
        return ((n_running == 0 and n_pending == 0) or
            (n_pending == 0 and get_current_retries() >= MAX_RETRIES))

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
