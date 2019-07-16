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
