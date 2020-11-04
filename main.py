from settings import *
from stages import *
from stage_instances import stage_instances
from dashboard import render
from init import init

def main():
    """Executes each stage and renders report
    """
    init()
    separator_len = 40
    for s in stage_instances:
        print('='*separator_len)
        print(s.name)
        print('-'*separator_len)

        s.add_tasks() # Add tasks from previous stage
        #s.revive_or_archive() # Revive killed tasks or move them to failed
        s.schedule_jobs() # Schedule new jobs if needed
        s.print_status()
        print('='*separator_len + '\n')
    #render(stage_instances)

main()
