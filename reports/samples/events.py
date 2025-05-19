import pandas as pd
import numpy as np
from config import db
from config import cfg
import mariadb
import plotly.express as px
import os
import pathlib
import pickle
from datetime import timedelta
from datetime import date
from dateutil.relativedelta import relativedelta
from reports import log
import hostlist


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

    # If we're using the database, provide the creds
    if use_db_server:
        conn = mariadb.connect(host=db.hostname,
                               port=db.port,
                               database=db.database,
                               user=db.username,
                               password=db.password)

        # Query database
        query = """SELECT DATE_FORMAT(FROM_UNIXTIME(t1.time_start), '%Y-%m-%d %H:%i') AS sdate,
        DATE_FORMAT(FROM_UNIXTIME(t1.time_end), '%Y-%m-%d %H:%i') AS edate,
        t1.node_name,
        t1.reason
        FROM devcluster_event_table AS t1
        WHERE t1.time_start < t1.time_end 
        AND t1.time_start <> 0
        AND t1.node_name <> ''"""

        jobs_df = pd.read_sql(sql=query, con=conn)

        # Store the date in our scratch in case we want it later
        scratch_handle = open(scratch_path, 'wb')
        pickle.dump(jobs_df, scratch_handle)
        scratch_handle.close()
    else:
        # If we're not using the database load the scratch file
        jobs_df = pd.read_pickle(scratch_path)

    # Convert the columns to the correct data types
    jobs_df['sdate'] = pd.to_datetime(jobs_df['sdate'])
    jobs_df['edate'] = pd.to_datetime(jobs_df['edate'])
    jobs_df.sort_values('sdate', inplace=True)

    # Drop records for debugging purposes
    #drop_point = pd.to_datetime(date.today() - relativedelta(days=10))
    #jobs_df = jobs_df[jobs_df['sdate'] > drop_point]

    # Determine the start and end datetime for the plot
    start_date = min(jobs_df.sdate)
    start_date = start_date.strftime('%d/%m/%Y')
    end_date = max(jobs_df.edate)
    end_date = end_date.strftime('%d/%m/%Y')

    jobs_df.sort_values('node_name', inplace=True)

    jobs_df = jobs_df.to_dict(orient='records')

    fig = px.timeline(jobs_df, x_start='sdate', x_end='edate', y='node_name',
                      hover_data=['sdate', 'edate', 'node_name', 'reason'],
                      labels={'sdate': 'Start', 'edate': 'End', 'node_name': 'Node', 'reason': 'Reason'},
                      height=cfg.graph_height, width=cfg.graph_width,
                      title=f'Node Events<br><sup>{start_date} to {end_date}</sup>')
    #fig.update_traces(hovertemplate=None,
    #                  hoverinfo='skip',
    #                  showlegend=False)

    # Update the axes to make them look better
    fig.update_xaxes(range=[start_date, end_date], type='date', tick0=start_date, tickangle=90,
                     dtick='172800000', tickformat='%H:%M %d %b')
    fig.update_yaxes(autorange='reversed')

    # Save the output
    output_name = '.'.join([report_name, 'html'])
    output_file = os.path.join(output_path, output_name)
    fig.write_html(output_file,
                   config={'displayModeBar': True,
                           'displaylogo': False,
                           'modeBarButtonsToRemove': ['lasso2d']})


if __name__ == '__main__':
    start()
