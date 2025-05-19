import os
from subprocess import run, PIPE
import sys
from config import cfg
import shlex


def node_list():
    # Determine the root of the project location
    script_path = os.path.realpath(__file__)
    project_path, report_path = script_path.split(cfg.project_code)
    scratch_path = os.path.join(project_path, 'scratch', 'node_list.csv')

    cmd = 'sinfo -ahN -o "%n|%P|%c|%m|%G"'
    cmd = shlex.split(cmd)
    proc = run(cmd, stdout=PIPE)
    cmd = 'grep -v "rc\*"'
    cmd = shlex.split(cmd)
    with open(scratch_path, 'w') as file:
        proc = run(cmd, input=proc.stdout, stdout=file)

    if proc.returncode != 0:
        sys.exit(1)


def convert_minutes_to_dhhmm(time):
    days, hours = divmod(int(time), 1440)
    hours, minutes = divmod(hours, 60)
    dhhmm = '{:d}-{:02d}:{:02d}'.format(days, hours, minutes)
    return dhhmm


# Calcuate percentiles for use in Pandas aggregation
# (https://stackoverflow.com/questions/17578115/pass-percentiles-to-pandas-agg-function)
def percentile(n):
    def percentile_(x):
        return int(x.quantile(n))
    percentile_.__name__ = 'percentile_{:02.0f}'.format(n * 100)
    return percentile_
