import logging
import sys
import os
from config import cfg


# Determine the root of the project location
script_path = os.path.realpath(__file__)
project_path, report_path = script_path.split(cfg.project_code)
log_name = cfg.project_log
log_path = os.path.join(project_path, 'output', log_name)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                              '%Y-%m-%d %H:%M')

if sys.stdout.isatty():
    handler = logging.StreamHandler()
else:
    handler = logging.FileHandler(log_path)

handler.setFormatter(formatter)
logger.addHandler(handler)
