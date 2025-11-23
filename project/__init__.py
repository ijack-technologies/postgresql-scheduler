"""
PostgreSQL Scheduler package for AWS RDS database maintenance and AWS IoT synchronization.

This package contains scheduled jobs that run on AWS EC2 to maintain the RDS PostgreSQL
database and synchronize data with AWS IoT device shadows. Jobs include time series
aggregation, data cleanup, gateway configuration sync, and monitoring.
"""

import sys
from pathlib import Path

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = str(Path(__file__).parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)
