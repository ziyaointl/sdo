from datetime import timedelta
from functools import lru_cache
import subprocess
import re
import os

def parse_timedelta(s):
    """Takes in a string in the form of "HH:MM:SS" or "D-HH:MM:SS" or "MM:SS" or
    'INVALID'
    Returns a timedelta object
    """
    if s == 'INVALID':
        return timedelta(0)
    if '-' in s:
        s = s.split('-')
        days, s = int(s[0]), s[1]
    else:
        days = 0
    s = list(map(lambda x: int(x), s.split(':')))
    if len(s) == 3:
        return timedelta(days=days, hours=s[0], minutes=s[1], seconds=s[2])
    return timedelta(days=days, hours=0, minutes=s[0], seconds=s[1])

def hours(dtime):
    """Takes in a timedelta object
    Returns the number of hours
    """
    return dtime.total_seconds() / 3600

def run_command(command):
    """Takes a command an runs it in the shell
    Returns stdout and stderr combined
    """
    print(command)
    output = subprocess.run(command, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, shell=True).stdout
    output = output.decode('utf-8')
    return output

@lru_cache(maxsize=None)
def cached_run_command(command):
    return run_command(command)

def make_template_format_friendly(template, verbose=False):
    """Takes in a script template string, returns a string with all occurances
    of ${VAR} replaced with ${{VAR}}
    """
    p = re.compile(r'\$({[^0-9]+?})')
    res = p.finditer(template)
    all_vars = set()
    for x in res:
        if verbose:
            print(x)
        all_vars.add(x.group(1))
    for v in all_vars:
        if verbose:
            print('Replacing', v, 'with', '{' + v + '}')
        template = template.replace(v, '{' + v + '}')
    return template

def read_template(name):
    """Reads in template from the templates folder, make it format friendly, and
    returns the reusltant string
    """
    fin = open(os.path.join('templates/', name), 'r')
    template = fin.read()
    template = make_template_format_friendly(template)
    fin.close()
    return template
