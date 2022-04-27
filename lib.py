from typing import List
from dataclasses import dataclass

# QOS choices
def regular():
    return "-q regular"

def debug():
    return "-q debug"

def realtime():
    return "-q realtime --exclusive"

def reservation(name):
    return "--reservation " + name

def bigmem():
    return "-q bigmem --cluster escori"

# Task sources
class TaskSource:
    pass

@dataclass
class QueueTaskSource(TaskSource):
    """Format: QueueTaskSource(queue_name, ['Failed', 'Running', 'Succeeded', 'Killed'])
    """
    name: str
    states: List[str]

@dataclass
class FileTaskSource(TaskSource):
    """Format: FileTaskSource(file_path)
    If the path can be either absolute or relative to the sdo directory
    The format is one brick per line
    Only works when the current queue is empty
    """
    file_path: str
