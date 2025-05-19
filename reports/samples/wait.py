import pandas as pd
import numpy as np
from reports import misc, log
from config import db
from config import cfg
import mariadb
import plotly.express as px
import os
import pathlib
import pickle
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def start():
    # Toggle use of the database
    use_db_server = db.use_db_server

    # Determine the root of the project location
    script_path = os.path.realpath(__file__)
    project_path, report_path = script_path.split(cfg.project_code)
    log.logger.info('Starting {} - using DB {}'.format(report_path, use_db_server))

    # Determine the name and location of the scratch file
    report_path = pathlib.Path(report_path)
    script_name = pathlib.Path(report_path.name)
    report_path = str(report_path.parent).lstrip(os.path.sep)
    report_name = script_name.stem
    scratch_name = '.'.join([report_name, 'pkl'])
    scratch_prefix = '_'.join([report_path, scratch_name])
    scratch_prefix = scratch_prefix.replace(os.path.sep, '_')
    scratch_path = os.path.join(project_path, 'scratch', scratch_prefix)
    output_path = os.path.join(project_path, 'output', report_path)

    # If we're using the database, connect using the credentials from the config file
    if use_db_server:
        conn = mariadb.connect(host=db.hostname,
                               port=db.port,
                               database=db.database,
                               user=db.username,
                               password=db.password)

        # Query the number of job submissions for each institute per date
        # Ensure the end date is older than the start date (i.e. it's completed)
        query = """SELECT 
        TIMESTAMPDIFF(MINUTE, DATE_FORMAT(FROM_UNIXTIME(t1.time_submit), '%Y-%m-%d %H:%i:%S'), 
        DATE_FORMAT(FROM_UNIXTIME(t1.time_start), '%Y-%m-%d %H:%i:%S')) as wait,
        t1.partition
        FROM devcluster_job_table AS t1
        WHERE t1.time_start < t1.time_end
        AND t1.time_start <> 0
        AND t1.partition <> ''"""

        jobs_df = pd.read_sql(sql=query, con=conn)

        # Store the date in our scratch in case we want it later
        scratch_handle = open(scratch_path, 'wb')
        pickle.dump(jobs_df, scratch_handle)
        scratch_handle.close()
    else:
        # If we're not using the database load the scratch file
        jobs_df = pd.read_pickle(scratch_path)

    # Replace any NA for 0
    jobs_df.fillna(value=0, inplace=True)
    jobs_df = jobs_df[~(jobs_df['wait'] < 0)]

    partitions = sorted(jobs_df['partition'].unique())

    number_ticks = 15
    min_wait = 0
    max_wait = max(jobs_df['wait']) * 1.1
    ticks = np.linspace(min_wait, max_wait, number_ticks, dtype=int)

    jobs_df['dhhmm'] = jobs_df['wait'].apply(lambda t: misc.convert_minutes_to_dhhmm(t))

    labels = []
    for i in range(number_ticks):
        tick = misc.convert_minutes_to_dhhmm(ticks[i])
        labels.append('{}'.format(tick))

    summary_df = jobs_df.groupby(['partition']).agg(avg=('wait', 'mean'),
                                                    quant=('wait', misc.percentile(0.95)),
                                                    max=('wait', 'max')).reset_index()

    fig = make_subplots(rows=1, cols=1, shared_yaxes=True, horizontal_spacing=0)
    fig.add_trace(go.Bar(x=summary_df['max'], y=summary_df['partition'], textposition='inside', name='Max',
                         hoverinfo='text',
                         orientation='h', text=summary_df['max'].map(lambda t: misc.convert_minutes_to_dhhmm(t)), opacity=0.5,
                         width=0.7, showlegend=True, marker_color='#ed7d31'), 1, 1)
    fig.add_trace(go.Bar(x=summary_df['quant'], y=summary_df['partition'], textposition='inside', name='95th Percentile',
                         hoverinfo='text',
                         orientation='h', text=summary_df['quant'].map(lambda t: misc.convert_minutes_to_dhhmm(t)), opacity=0.75,
                         width=0.7, showlegend=True, marker_color='#ed7d31'), 1, 1)
    fig.add_trace(go.Bar(x=summary_df['avg'], y=summary_df['partition'], textposition='inside', name='Mean',
                         hoverinfo='text',
                         orientation='h', text=summary_df['avg'].map(lambda t: misc.convert_minutes_to_dhhmm(t)),
                         width=0.7, showlegend=True, marker_color='#ed7d31'), 1, 1)

    fig.update_xaxes(row=1, col=1, range=[min_wait, max_wait], tickmode='array',
                     tickvals=ticks, ticktext=labels, title_text='Duration (D-HH:MM)')
    fig.update_yaxes(autorange='reversed')
    fig.update_layout(title={'text': f'Wait Times</b>',
                             'y': 0.97,
                             'x': 0.5,
                             'xanchor': 'center',
                             'yanchor': 'top'},
                      barmode='overlay',
                      height=cfg.graph_height // 12 * len(partitions), width=cfg.graph_width)

    # Save the output
    output_name = '.'.join([report_name, 'html'])
    output_file = os.path.join(output_path, output_name)
    fig.write_html(output_file,
                   config={'displayModeBar': True,
                           'displaylogo': False,
                           'modeBarButtonsToRemove': ['lasso2d']})


if __name__ == '__main__':
    start()
