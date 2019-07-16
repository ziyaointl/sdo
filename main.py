from settings import *

def main():
    stages = []
    # INITIALIZE stages

    for s in stages:
        if s.is_done():
            continue
        s.add_tasks()
        if s.previous_stage.is_done():
            s.attempt_recover()
        s.schedule_jobs()
        s.print_status()

# main()
