import sqlite3
import os
from shutil import copyfile
from settings import *
from util import *

# Initialize database
conn = sqlite3.connect('sdo.db')
c = conn.cursor()
try:
    c.execute('CREATE TABLE retries (stage VARCHAR NOT NULL, times INT)')
except sqlite3.OperationalError:
    print('SQL table already exists, skipping')
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
fout = open('scripts/{}.sh'.format(PREFARM_QNAME), 'w')
fout.write(runbrick_script.format(LEGACY_SURVEY_DIR, 17, 'srcs'))
fout.close()
fout = open('scripts/launch-farm.sh', 'w')
fout.write(launch_farm_script.format(LEGACY_SURVEY_DIR, FARM_QNAME, SDO_SCRIPT_DIR))
fout.close()
fout = open('scripts/{}.sh'.format(POSTFARM_QNAME), 'w')
fout.write(runbrick_script.format(LEGACY_SURVEY_DIR, 17, 'writecat'))
fout.close()
fout = open('scripts/{}.sh'.format(POSTFARM_SCAVENGER_ONE_QNAME), 'w')
fout.write(runbrick_script.format(LEGACY_SURVEY_DIR, 8, 'writecat'))
fout.close()
fout = open('scripts/{}.sh'.format(POSTFARM_SCAVENGER_TWO_QNAME), 'w')
fout.write(runbrick_script.format(LEGACY_SURVEY_DIR, 1, 'writecat'))
fout.close()
run_command('chmod +x -v scripts/*')
