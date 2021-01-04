# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import os
import platform
import sys
import unittest
import logging
import pickle
import requests
from twilio.rest import Client

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = '/workspace/cron.d'
try:
    sys.path.index(pythonpath) 
except ValueError:
    sys.path.insert(0, pythonpath) 

# local imports
from utils import (
    Config, configure_logging, run_query, error_wrapper, send_mailgun_email, send_twilio_phone, send_twilio_sms
)
import time_series_mv_refresh
import gateways_mv_refresh
import alarm_log_mv_refresh_old_non_surface
import alarm_log_mv_refresh
import alarm_log_delete_duplicates
import update_gw_power_unit_id_from_shadow
import synch_aws_iot_shadow_with_aws_rds_postgres_config

LOGFILE_NAME = 'test_main_programs'

c = Config()
c.DEV_TEST_PRD = 'development'
c.logger = configure_logging(
    __name__,
    logfile_name = LOGFILE_NAME, 
    path_to_log_directory='/var/log/'
)

class TestAll(unittest.TestCase):

    # # executed after each test
    # def tearDown(self):
    #     pass

    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = 'development'
        c.TEST_FUNC = True
        

    def test_time_series(self):
        """Test the main program"""
        global c
        time_series_mv_refresh.main(c)
        

    def test_alarm_log_refresh_old(self):
        """Test the main program"""
        global c
        alarm_log_mv_refresh_old_non_surface.main(c)
        

    def test_alarm_log_refresh_new(self):
        """Test the main program"""
        global c
        alarm_log_mv_refresh.main(c)
        

    def test_alarm_log_delete_duplicates(self):
        """Test the main program"""
        global c
        alarm_log_delete_duplicates.main(c)


if __name__ == '__main__':
    unittest.main()
    