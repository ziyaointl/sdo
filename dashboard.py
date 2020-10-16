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
from shutil import copyfile
from itertools import zip_longest

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
    timenow = strftime('%Y-%m-%d_%H:%M:%S')
    os.makedirs('reports/history', exist_ok=True)
    os.makedirs('reports/current', exist_ok=True)
    os.makedirs(PUBLIC_REPORT_PATH, exist_ok=True)

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
        states, counts = zip(*{k : queue_state[k]for k in ['Pending', 'Running', 'Succeeded', 'Failed']}.items())
        colors = ['#4390ca', '#32bdb2', '#99d593', '#d53e4f']
        source = ColumnDataSource(data=dict(states=states, counts=counts, color=colors))
        p = figure(x_range=states,
                    y_range=(0, total_tasks*1.3 or 1),
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
            name, jobid, time_limit, qos, nodes, used_time, state, submit_time, work_dir, account = zip(*[(j['NAME'], j['JOBID'], j['TIME_LIMIT'], j['QOS'], j['NODES'], j['TIME'], j['STATE'], j['SUBMIT_TIME'], j['WORK_DIR'], j['ACCOUNT']) for j in jobs])
            d = {}
            for column in ['name', 'jobid', 'time_limit', 'qos', 'nodes', 'used_time', 'state', 'submit_time', 'work_dir', 'account']:
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
                TableColumn(field="work_dir", title="Working Directory"),
                TableColumn(field="account", title="Account")
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
        filename = 'reports/current/{}.html'.format(queue_name)
        with open(filename, 'w') as f:
            f.write(template.render(plot=divs[0],
                                    script=script,
                                    done=s.is_done(),
                                    cores_per_worker=s.cores_per_worker,
                                    arch=s.arch,
                                    est_efficiency=s.tasks_per_nodehr,
                                    default_duration=s.job_duration,
                                    stage=queue_name,
                                    jobs=jobs,
                                    jobs_table=divs[1],
                                    tasks_table=divs[2],
                                    hours_in_queue=hours(s.get_time_in_queue()),
                                    bokeh=CDN.render(),
                                    timenow=timenow
                                    ))
        copyfile(filename, 'reports/history/{}-{}.html'.format(queue_name, timenow))
        copyfile(filename, os.path.join(PUBLIC_REPORT_PATH, '{}.html'.format(queue_name)))
        print('Wrote {}.html'.format(queue_name))

    # Copy css file
    copyfile('reports/templates/styles.css', 'reports/current/styles.css')
    copyfile('reports/templates/styles.css', 'reports/history/styles.css')
    copyfile('reports/templates/styles.css', os.path.join(PUBLIC_REPORT_PATH, 'styles.css'))

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
    filename = 'reports/current/index.html'
    if SUMMARY_FORMAT == 'classic':
        template = env.get_template('index.html')
        with open(filename, 'w') as f:
            f.write(template.render(script=script,
                                    prefarm1_plot=divs[0],
                                    prefarm2_plot=divs[1],
                                    prefarm3_plot=divs[2],
                                    farm_plot=divs[3],
                                    postfarm1_plot=divs[4],
                                    postfarm2_plot=divs[5],
                                    postfarm3_plot=divs[6],
                                    prefarm1_name = PREFARM_QNAME,
                                    prefarm2_name = PREFARM_SCAVENGER_ONE_QNAME,
                                    prefarm3_name = PREFARM_SCAVENGER_TWO_QNAME,
                                    farm_name = FARM_QNAME,
                                    postfarm1_name = POSTFARM_QNAME,
                                    postfarm2_name = POSTFARM_SCAVENGER_ONE_QNAME,
                                    postfarm3_name = POSTFARM_SCAVENGER_TWO_QNAME,
                                    prefarm_progress = "{0:.2f}".format(sum(succeeded_tasks[:3]) / total_tasks * 100),
                                    farm_progress = "{0:.2f}".format(succeeded_tasks[3] / total_tasks * 100),
                                    postfarm_progress = "{0:.2f}".format(sum(succeeded_tasks[4:]) / total_tasks * 100),
                                    bokeh=CDN.render(),
                                    timenow=timenow
                                    ))
    elif SUMMARY_FORMAT == 'simple':
        template = env.get_template('index_simple.html')
        names = [POSTFARM_QNAME, POSTFARM_SCAVENGER_ONE_QNAME, POSTFARM_SCAVENGER_TWO_QNAME]
        names = [('name', n) for n in names]
        plots = [('plot', d) for d in divs]
        simple_stages = [dict(d) for d in zip(names, plots)]
        # Passes the same iterator three times to zip_longest
        # https://stackoverflow.com/questions/1624883/alternative-way-to-split-a-list-into-groups-of-n
        stages_in_rows = zip_longest(*(iter(simple_stages),)*3)
        with open(filename, 'w') as f:
            f.write(template.render(script=script,
                                    bokeh=CDN.render(),
                                    timenow=timenow,
                                    stages_in_rows=stages_in_rows
            ))
    copyfile(filename, 'reports/history/index-{}.html'.format(timenow))
    copyfile(filename, os.path.join(PUBLIC_REPORT_PATH, 'index.html'))
    print('Wrote index.html'.format(queue_name))
