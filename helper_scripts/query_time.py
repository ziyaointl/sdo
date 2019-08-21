import sqlite3
import sys 
import csv
from io import StringIO
sys.path.append('..')
from util import run_command, parse_timedelta
from datetime import timedelta
from util import hours

# Query db and slurm
conn = sqlite3.connect('../sdo.db')
c = conn.cursor()
times = {}
for row in c.execute('SELECT * FROM jobs'):
    stage, jobid = row[0], row[1]
    output = run_command('sacct -j {} --format=JobID,Elapsed,ReqNodes -P'.format(jobid))
    output = StringIO(output)
    reader = csv.DictReader(output, delimiter='|')
    for row in reader:
        if '.' not in row['JobID']:
            times[stage] = times.get(stage, timedelta()) + (parse_timedelta(row['Elapsed']) * int(row['ReqNodes']))
            print('Parsed', row['JobID'])
# Display results
for k, dt in times.items():
    print(k, hours(dt), 'hours')
    print('Total', hours(sum(times.values(), timedelta())), 'hours')
