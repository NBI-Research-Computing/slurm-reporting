import pandas as pd
import numpy as np
from config import db
from config import cfg
import mariadb
import plotly.express as px
import os
import pathlib
import pickle
from reports import log, misc
from plotly.subplots import make_subplots
import plotly.graph_objects as go


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
        query = """SELECT DATE_FORMAT(FROM_UNIXTIME(time_start), '%Y-%m-%d %H:%i') AS sdate,
        DATE_FORMAT(FROM_UNIXTIME(time_end), '%Y-%m-%d %H:%i') AS edate,
        TIMESTAMPDIFF(MINUTE, DATE_FORMAT(FROM_UNIXTIME(t1.time_start), '%Y-%m-%d %H:%i:%S'), 
        DATE_FORMAT(FROM_UNIXTIME(t1.time_end), '%Y-%m-%d %H:%i:%S')) as elapsed,
        t1.partition
        FROM devcluster_job_table AS t1
        WHERE t1.time_start < t1.time_end
        AND t1.time_start <> 0
        AND t1.partition <> ''
        AND t1.state = 3"""

        jobs_df = pd.read_sql(sql=query, con=conn)

        # Store the date in our scratch in case we want it later
        scratch_handle = open(scratch_path, 'wb')
        pickle.dump(jobs_df, scratch_handle)
        scratch_handle.close()
    else:
        # If we're not using the database load the scratch file
        jobs_df = pd.read_pickle(scratch_path)

    # Convert the date field to a datetime format
    jobs_df['sdate'] = pd.to_datetime(jobs_df['sdate'])
    jobs_df['edate'] = pd.to_datetime(jobs_df['edate'])
    # Replace any NA for 0
    jobs_df.dropna(inplace=True)

    partitions = sorted(jobs_df['partition'].unique())
    number_partitions = len(partitions)
    cols = 3
    quotient = number_partitions // cols
    remainder = number_partitions % cols

    if remainder > 0:
        rows = quotient + 1
    else:
        rows = quotient

    fig = make_subplots(rows=rows, cols=cols, subplot_titles=(partitions))

    curr_row = 1
    curr_col = 0
    for idx, partition in enumerate(partitions):
        if curr_col >= cols:
            curr_row += 1
            curr_col = 1
        else:
            curr_col += 1

        partitionFilter = jobs_df['partition'] == partition

        partition_df = pd.DataFrame()
        percentile = []
        data = []
        for i in range(0, 101, 1):
            x = i / 100.0
            percentile.append(i)
            data.append(int(jobs_df.loc[partitionFilter, 'elapsed'].quantile(x)))

        partition_df['percentile'] = percentile
        partition_df['elapsed'] = data
        partition_df['dhhmm'] = partition_df['elapsed'].apply(lambda t: misc.convert_minutes_to_dhhmm(t))

        fig.add_trace(
            go.Scatter(x=partition_df['percentile'], y=partition_df['dhhmm'], name=partition,
                       hovertemplate='Percentile: %{x}<br>D-HHMM: %{y}'),
            row=curr_row, col=curr_col
        )

        fig.update_xaxes(title_text='Percentile', dtick='10', row=curr_row, col=curr_col)

        # Update yaxis properties
        fig.update_yaxes(title_text='Elapsed (D-HHMM)', tickformat='none', row=curr_row, col=curr_col)

        # Update title and height
        fig.update_layout(title_text='Elapsed Time Percentiles', height=cfg.graph_height // 3 * curr_row, width=cfg.graph_width)

    # Save the output
    output_name = '.'.join([report_name, 'html'])
    output_file = os.path.join(output_path, output_name)
    fig.write_html(output_file,
                   config={'displayModeBar': True,
                           'displaylogo': False,
                           'modeBarButtonsToRemove': ['lasso2d']})


if __name__ == '__main__':
    start()
