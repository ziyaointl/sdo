from settings import *
import bokeh

def plot(stages):
    """Takes in a list of stages and generates a report in the plot/ directory
    Assumes the total number of tasks = the number of tasks in the first stage
    Also assumes all stages are QdoCentricStages
    """
    if len(stages) == 0:
        raise RuntimeError("Stage list empty")
    total_tasks = stages[0].queue.tasks()
    for s in stages:
        # Plot tasks and their states
        # Print the number of retries & done?
        # Print a table of associated tasks
        # Optional: include logs

