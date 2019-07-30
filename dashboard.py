from settings import *
from jinja2 import Environment, select_autoescape, FileSystemLoader
from bokeh.io import show
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.models.widgets import DataTable, TableColumn
from bokeh.plotting import figure
from bokeh.embed import components

def render(stages):
    """Takes in a list of stages and generates a report in the reports/ directory
    Assumes the total number of tasks = the number of tasks in the first stage
    Also assumes all stages are QdoCentricStages
    """
    if len(stages) == 0:
        raise RuntimeError("Stage list empty")
    env = Environment(loader=FileSystemLoader('reports/templates'))
    template = env.get_template('index.html')
    for s in stages:
        # Initialize variables
        queue_state = s.queue.status()['ntasks']
        jobs = s.get_jobs_in_queue(clear_cache=True)
        tasks = s.queue.tasks()
        queue_name = s.name

        # Status Plot
        states, counts = zip(*queue_state.items())
        colors = ['#f2e38d', '#4390ca', '#32bdb2', '#99d593', '#d53e4f']
        source = ColumnDataSource(data=dict(states=states, counts=counts, color=colors))
        hover = HoverTool(tooltips=[('State', '@states'), ('Count', '@counts')])
        p = figure(x_range=states,
                    y_range=(0, max(counts)*1.5),
                    plot_height=400,
                    title=queue_name,
                    toolbar_location=None,
                    tools=[hover],
                    sizing_mode='scale_width')
        p.vbar(x='states', top='counts', color='color', width = 0.9, source=source)
        p.min_border = 50
        p.xgrid.grid_line_color = None

        # Jobs
        name, jobid, time_limit, qos, nodes, used_time, state, submit_time, work_dir = zip(*[(j['NAME'], j['JOBID'], j['TIME_LIMIT'], j['QOS'], j['NODES'], j['TIME'], j['STATE'], j['SUBMIT_TIME'], j['WORK_DIR']) for j in jobs])
        d = {}
        for column in ['name', 'jobid', 'time_limit', 'qos', 'nodes', 'used_time', 'state', 'submit_time', 'work_dir']:
            d[column] = locals()[column]
        source = ColumnDataSource(data=d)
        columns = [
                TableColumn(field="jobid", title="ID"),
                TableColumn(field="name", title="Name"),
                TableColumn(field="state", title="State"),
                TableColumn(field="time_limit", title="Time Limit"),
                TableColumn(field="used_time", title="Used Time"),
                TableColumn(field="qos", title="QOS"),
                TableColumn(field="nodes", title="Nodes"),
                TableColumn(field="submit_time", title="Submit Time"),
                TableColumn(field="work_dir", title="Working Directory")
        ]
        jobs_table = DataTable(source=source, columns=columns, height=200, sizing_mode='stretch_both')

        # Tasks
        name, state = zip(*[(t.task, t.state) for t in tasks])
        source = ColumnDataSource(data=dict(name=name, state=state))
        columns = [
                TableColumn(field="name", title="Name"),
                TableColumn(field="state", title="State")
        ]
        tasks_table = DataTable(source=source, columns=columns, height=200, sizing_mode='stretch_both')

        script, divs = components((p, jobs_table, tasks_table))

        with open('reports/{}.html'.format(queue_name), 'w') as f:
            f.write(template.render(plot=divs[0],
                                    script=script,
                                    retries=1,
                                    done=False,
                                    stage=queue_name,
                                    jobs=divs[1],
                                    tasks=divs[2]))
        print('Written {}.html'.format(queue_name))
