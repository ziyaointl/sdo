import sqlite3
import os
from shutil import copyfile
from settings import *
from util import *

def init():# Initialize database
    conn = sqlite3.connect('sdo.db')
    c = conn.cursor()
    try:
        c.execute('CREATE TABLE retries (stage VARCHAR NOT NULL, times INT)')
    except sqlite3.OperationalError:
        print('retries TABLE already exists, skipping')
    try:
        c.execute("CREATE TABLE jobs (stage VARCHAR NOT NULL, jobid INT, creationTime DATETIME NOT NULL DEFAULT(DATETIME('now')))")
    except sqlite3.OperationalError:
        print('jobs TABLE already exists, skipping')
    try:
        c.execute("CREATE TABLE farm_timeouts (brick VARCHAR NOT NULL)")
    except sqlite3.OperationalError:
        print('farm_timeouts TABLE already exists, skipping')
    conn.commit()
    conn.close()

    # Initialize scripts folder
    if not os.path.exists('scripts'):
        os.makedirs('scripts')

    # Read templates
    runbrick_script = read_template('runbrick/runbrick-shifter.sh')
    launch_farm_script = read_template('farm/launch-farm.sh')
    launch_worker_script = read_template('farm/launch-worker.sh')

    # Copy mpi_bugfix.sh
    copyfile('templates/farm/mpi_bugfix.sh', 'scripts/mpi_bugfix.sh')
    # Copy launch-worker.sh
    copyfile('templates/farm/launch-worker.sh', 'scripts/launch-worker.sh')
    # Copy qdo_login.sh
    copyfile(QDO_LOGIN_PATH, 'scripts/qdo_login.sh')

    # Write scripts
    def write_runbrick(qname, ncores, stage, mem):
        fout = open('scripts/{}.sh'.format(qname), 'w')
        fout.write(runbrick_script.format(LEGACY_SURVEY_DIR, ncores, stage, TELESCOPE, mem, EXTRA_PARAMS))
        fout.close()

    write_runbrick(PREFARM_QNAME, 8, 'srcs', KNL_MEM // 8)
    write_runbrick(PREFARM_SCAVENGER_ONE_QNAME, 17, 'srcs', KNL_MEM // 4)
    write_runbrick(PREFARM_SCAVENGER_TWO_QNAME, 17, 'srcs', KNL_MEM // 4)
    fout = open('scripts/launch-farm.sh', 'w')
    fout.write(launch_farm_script.format(LEGACY_SURVEY_DIR, FARM_QNAME, SDO_SCRIPT_DIR, SDO_DIR))
    fout.close()
    write_runbrick(POSTFARM_QNAME, 17, 'writecat', KNL_MEM // 4)
    write_runbrick(POSTFARM_SCAVENGER_ONE_QNAME, 34, 'writecat', KNL_MEM // 2)
    write_runbrick(POSTFARM_SCAVENGER_TWO_QNAME, 8, 'writecat', HASWELL_MEM)
    run_command('chmod +x -v scripts/*')
