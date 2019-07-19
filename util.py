from datetime import timedelta
import subprocess

def parse_timedelta(s):
    """Takes in a string in the form of "HH:MM:SS" or "D-HH:MM:SS"
    Returns a timedelta object
    """
    if '-' in s:
        s = s.split('-')
        days, s = int(s[0]), s[1]
    else:
        days = 0
    s = list(map(lambda x: int(x), s.split(':')))
    return timedelta(days=days, hours=s[0], minutes=s[1], seconds=s[2])

