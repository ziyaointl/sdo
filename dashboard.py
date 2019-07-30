from settings import *
from jinja2 import Environment, select_autoescape, FileSystemLoader
from bokeh.io import show
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure
from bokeh.embed import components

def plot(stages):
    """Takes in a list of stages and generates a report in the reports/ directory
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
        return

env = Environment(
    loader=FileSystemLoader('reports/templates')
)
template = env.get_template('index.html')

# Dummy variables
queue_state = {'Pending': 35, 'Running': 10, 'Succeeded': 20, 'Failed': 2}
jobs = [{'name': 'prefarm.sh', 'duration': '2:00:00'}]
tasks = [{'name': '1234p567', 'state': 'Running'},
        {'name': '1334p567', 'state': 'Pending'},
        {'name': '1434p567', 'state': 'Failed'}]
queue_name = "Prefarm"

# Status Plot
states, counts = zip(*queue_state.items())
colors = ['#4390ca', '#32bdb2', '#99d593', '#d53e4f']
source = ColumnDataSource(data=dict(states=states, counts=counts, color=colors))
hover = HoverTool(tooltips=[('State', '@states'), ('Count', '@counts')])
p = figure(x_range=states, y_range=(0, max(counts)*1.5), plot_height=400, title=queue_name, toolbar_location=None, tools=[hover], sizing_mode='scale_width')
p.vbar(x='states', top='counts', color='color', legend='states', width = 0.9, source=source)
p.min_border = 50
p.xgrid.grid_line_color = None
p.legend.orientation = "horizontal"
p.legend.location = "top_center"

script, divs = components(p)
print(script)
with open('reports/index.html', 'w') as f:
    f.write(template.render(plot=divs[0], script=script, retries=1, done=False, stage=queue_name), jobs=divs[1], tasks=divs[2])
