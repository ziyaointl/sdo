import sqlite3
import os
from shutil import copyfile
from settings import *
from util import *
from stage_instances import stage_instances

def init():
    # Initialize database
    conn = sqlite3.connect('sdo.db')
    c = conn.cursor()
    try:
        c.execute("CREATE TABLE jobs (stage VARCHAR NOT NULL, jobid INT, creationTime DATETIME NOT NULL DEFAULT(DATETIME('now')))")
    except sqlite3.OperationalError:
        print('jobs TABLE already exists, skipping')
    try:
        c.execute("CREATE TABLE retries (stage VARCHAR NOT NULL, brick VARCHAR NOT NULL, count INT DEFAULT 0)")
    except sqlite3.OperationalError:
        print('retries TABLE already exists, skipping')
    conn.commit()
    conn.close()

    # Initialize scripts folder
    if not os.path.exists('scripts'):
        os.makedirs('scripts')

    # Read templates
    runbrick_script = read_template('runbrick/runbrick-shifter.sh')

    # Copy qdo_login.sh
    copyfile(QDO_LOGIN_PATH, 'scripts/qdo_login.sh')

    # Write runbrick script
    def write_runbrick(qname, ncores, stage, mem, write_stage):
        fout = open('scripts/{}.sh'.format(qname), 'w')
        write_stage = ' '.join('--write-stage ' + s for s in write_stage)
        if write_stage == '':
            write_stage = '--no-write'
        fout.write(runbrick_script.format(LEGACY_SURVEY_DIR, ncores, stage, TELESCOPE, mem, EXTRA_PARAMS, write_stage))
        fout.close()

    for s in stage_instances:
        write_runbrick(s.name, s.cores_per_worker_actual, s.stage, s.mem_per_worker, s.write_stage)

    run_command('chmod +x -v scripts/*')
