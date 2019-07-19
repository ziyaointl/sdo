import sqlite3
import os
from settings import *
from util import *

# Initialize database
conn = sqlite3.connect('sdo.db')
c = conn.cursor()
c.execute('CREATE TABLE retries (stage VARCHAR NOT NULL, times INT)')
conn.commit()
conn.close()

# Initialize scripts
if not os.path.exists('scripts'):
    os.makedirs('scripts')

# Read templates
fin = open('templates/runbrick/runbrick-shifter.sh', 'r')
runbrick_script = fin.read()
runbrick_script = make_template_format_friendly(runbrick_script)
fin.close()

# Write scripts
fout = open('scripts/prefarm.sh', 'w')
fout.write(runbrick_script.format(LEGACY_SURVEY_DIR, 17, 'srcs'))
fout.close()
