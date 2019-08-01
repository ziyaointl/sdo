from settings import *
from util import cached_run_command, hours
from jinja2 import Environment, select_autoescape, FileSystemLoader
from bokeh.io import show
from bokeh.models import ColumnDataSource, HoverTool, LabelSet
from bokeh.models.widgets import DataTable, TableColumn
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.embed import components
from time import strftime

def render(stages):
    """Takes in a list of stages and generates a report in the reports/ directory
    Assumes the total number of tasks = the number of tasks in the first stage
    Also assumes all stages are QdoCentricStages
    """
    if len(stages) == 0:
        raise RuntimeError("Stage list empty")
    env = Environment(loader=FileSystemLoader('reports/templates'))
    template = env.get_template('stage.html')
    cached_run_command.cache_clear()

    total_tasks = len(stages[0].queue.tasks())
    succeeded_tasks = []
    status_plots = []

    for s in stages:
        # Initialize variables
        queue_state = dict(s.queue.status()['ntasks'])
        queue_state.pop('Waiting')
        jobs = s.get_jobs_in_queue()
        tasks = s.queue.tasks()
        queue_name = s.name

        # Track progress
        succeeded_tasks.append(int(queue_state['Succeeded']))

        # Status Plot
        states, counts = zip(*queue_state.items())
        colors = ['#4390ca', '#32bdb2', '#99d593', '#d53e4f']
        source = ColumnDataSource(data=dict(states=states, counts=counts, color=colors))
        p = figure(x_range=states,
                    y_range=(0, max(counts)*1.5),
                    plot_height=400,
                    tools='',
                    toolbar_location=None,
                    sizing_mode='scale_width')
        p.vbar(x='states', top='counts', color='color', width = 0.5, source=source)
        p.min_border = 50
        p.xgrid.grid_line_color = None
        status_plots.append(p)

        # Labels
        labels = LabelSet(x='states', y='counts', text='counts', level='glyph',
         x_offset=0, y_offset=0, source=source, text_align='center')
        p.add_layout(labels)

        # Jobs
        if len(jobs) != 0:
            name, jobid, time_limit, qos, nodes, used_time, state, submit_time, work_dir = zip(*[(j['NAME'], j['JOBID'], j['TIME_LIMIT'], j['QOS'], j['NODES'], j['TIME'], j['STATE'], j['SUBMIT_TIME'], j['WORK_DIR']) for j in jobs])
            d = {}
            for column in ['name', 'jobid', 'time_limit', 'qos', 'nodes', 'used_time', 'state', 'submit_time', 'work_dir']:
                d[column] = locals()[column]
            source = ColumnDataSource(data=d)
        else:
            source = ColumnDataSource()
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
        if len(tasks) != 0:
            name, state = zip(*[(t.task, t.state) for t in tasks])
            source = ColumnDataSource(data=dict(name=name, state=state))
        else:
            source = ColumnDataSource()
        columns = [
                TableColumn(field="name", title="Name"),
                TableColumn(field="state", title="State")
        ]
        tasks_table = DataTable(source=source, columns=columns, height=200, sizing_mode='stretch_both')

        script, divs = components((p, jobs_table, tasks_table))

        with open('reports/generated/{}.html'.format(queue_name), 'w') as f:
            f.write(template.render(plot=divs[0],
                                    script=script,
                                    retries=s.get_current_retries(),
                                    done=s.is_done(),
                                    stage=queue_name,
                                    jobs=jobs,
                                    jobs_table=divs[1],
                                    tasks_table=divs[2],
                                    hours_in_queue=hours(s.get_time_in_queue()),
                                    bokeh=CDN.render(),
                                    timenow=strftime('%Y-%m-%d %H:%M:%S')
                                    ))
        print('Written {}.html'.format(queue_name))

    # Copy css file

    # Generate index.html
    def process_plot(p):
        # Adjust style
        p.background_fill_alpha = 0
        p.border_fill_alpha = 0
        p.min_border = 10
        p.yaxis.visible = False
        p.plot_height = 300
        return p

    script, divs = components(tuple(process_plot(p) for p in status_plots))
    template = env.get_template('index.html')
    with open('reports/generated/index.html', 'w') as f:
        f.write(template.render(script=script,
                                prefarm_plot=divs[0],
                                farm_plot=divs[1],
                                postfarm1_plot=divs[2],
                                postfarm2_plot=divs[3],
                                postfarm3_plot=divs[4],
                                prefarm_name = PREFARM_QNAME,
                                farm_name = FARM_QNAME,
                                postfarm1_name = POSTFARM_QNAME,
                                postfarm2_name = POSTFARM_SCAVENGER_ONE_QNAME,
                                postfarm3_name = POSTFARM_SCAVENGER_TWO_QNAME,
                                prefarm_progress = "{0:.2f}".format(succeeded_tasks[0] / total_tasks * 100),
                                farm_progress = "{0:.2f}".format(succeeded_tasks[1] / total_tasks * 100),
                                postfarm_progress = "{0:.2f}".format(sum(succeeded_tasks[2:]) / total_tasks * 100),
                                bokeh=CDN.render(),
                                timenow=strftime('%Y-%m-%d %H:%M:%S')
                                ))
    print('Written index.html'.format(queue_name))
