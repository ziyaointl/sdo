import sqlite3
import sys 
import csv
from io import StringIO
sys.path.append('..')
from util import run_command, parse_timedelta
from datetime import timedelta
from util import hours

conn = sqlite3.connect('sdo.db')
c = conn.cursor()
times = {}
for row in c.execute('SELECT * FROM jobs'):
    stage, jobid = row[0], row[1]
    output = run_command('sacct -j {} -l -P'.format(jobid))
    output = StringIO(output)
    reader = csv.DictReader(output, delimiter='|')
    for row in reader:
        if '.' not in row['JobID']:
            times[stage] = times.get(stage, timedelta()) + parse_timedelta(row['Elapsed'])
            print('Parsed', row['JobID'])
for k, dt in times.items():
    print(k, hours(dt), 'hours')
