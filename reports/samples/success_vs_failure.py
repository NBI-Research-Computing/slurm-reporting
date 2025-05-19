import pandas as pd
from config import db
from config import cfg
import mariadb
import plotly.express as px
import os
import pathlib
import pickle
from reports import log
from random import choice
from string import ascii_lowercase, digits

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
        DATE_FORMAT(FROM_UNIXTIME(t1.time_end), '%Y-%m-%d') AS date,
        t1.state,
        COUNT(t1.state) AS jobcount,
        t1.account,
        t2.user
        FROM devcluster_job_table AS t1
        LEFT JOIN devcluster_assoc_table as t2 on t1.id_assoc=t2.id_assoc
        WHERE t1.time_start < t1.time_end
        AND t1.time_start <> 0
        AND t1.partition <> ''
        GROUP BY date, t2.user, t1.state"""

        jobs_df = pd.read_sql(sql=query, con=conn)

        # Store the date in our scratch in case we want it later
        scratch_handle = open(scratch_path, 'wb')
        pickle.dump(jobs_df, scratch_handle)
        scratch_handle.close()
    else:
        # If we're not using the database load the scratch file
        jobs_df = pd.read_pickle(scratch_path)

    jobs_df['date'] = pd.to_datetime(jobs_df['date'], format='%Y-%m-%d')
    # Replace any NA for 0
    jobs_df.fillna(value=0, inplace=True)
    jobs_df = jobs_df[jobs_df['state'].isin([3, 5])]
    jobs_df['state'] = jobs_df['state'].replace({3: 'success', 5: 'failure'})

    institutes = sorted(jobs_df['account'].unique())

    # Determine the start and end datetime for the plot
    start_date = min(jobs_df.date)
    start_date = start_date.strftime('%d/%m/%Y')
    end_date = max(jobs_df.date)
    end_date = end_date.strftime('%d/%m/%Y')

    # Create a stacked bar chart based on each institute's job count
    fig = px.sunburst(jobs_df, path=['account', 'state', 'user'], values='jobcount',
                      height=cfg.graph_height, width=cfg.graph_width,
                      title=f'Job Success vs Failures per User<br><sup>{start_date} to {end_date}</sup>')

    fig.update_traces(hovertemplate='Label: %{id}<br>Jobs: %{value}')

    # Save the output
    output_name = '.'.join([report_name, 'html'])
    output_file = os.path.join(output_path, output_name)
    fig.write_html(output_file,
                   config={'displayModeBar': True,
                           'displaylogo': False,
                           'modeBarButtonsToRemove': ['lasso2d']})


if __name__ == '__main__':
    start()
