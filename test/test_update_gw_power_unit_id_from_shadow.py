# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
from typing import OrderedDict
from datetime import date
import unittest
from unittest.mock import patch

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


# import alarm_log_mv_refresh_old_non_surface
from cron_d import update_gw_power_unit_id_from_shadow

# local imports
from cron_d.utils import Config, configure_logging, run_query

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


LOGFILE_NAME = "test_main_programs"

c = Config()
c.DEV_TEST_PRD = "development"
c.logger = configure_logging(
    __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
)


class TestAll(unittest.TestCase):

    # # executed after each test
    # def tearDown(self):
    #     pass

    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        c.TEST_FUNC = True

    @patch("cron_d.update_gw_power_unit_id_from_shadow.send_mailgun_email")
    @patch("cron_d.update_gw_power_unit_id_from_shadow.run_query")
    def test_update_structures_table(
        self,
        mock_run_query,
        mock_send_mailgun_email,
    ):
        """
        Test the function that sends an email warning of a GPS lat/lon update,
        and that auto-updates the public.structures table
        """
        global c
        c.TEST_FUNC = False  # using mocks instead

        power_unit_id = 313
        power_unit_shadow = 200476
        column = "gps_lon"
        new_value = -190.01567895242
        structure = 190619
        aws_thing = "1000051"
        db_value = -120.09023308333333

        sql_get_info_str = update_gw_power_unit_id_from_shadow.sql_get_info(
            c, power_unit_id, power_unit_shadow, structure, aws_thing
        )

        _, rows = run_query(
            c, sql_get_info_str, db="ijack", execute=True, fetchall=True, commit=False
        )
        mock_run_query.return_value = (["col1", "col2"], rows)

        # Run the main function we're testing
        update_gw_power_unit_id_from_shadow.update_structures_table(
            c,
            power_unit_id=power_unit_id,
            power_unit_shadow=power_unit_shadow,
            column=column,
            new_value=new_value,
            structure=structure,
            aws_thing=aws_thing,
            db_value=db_value,
            # Testing only
            execute=False,
            commit=False,
        )

        dict_ = rows[0]
        sql_update = update_gw_power_unit_id_from_shadow.get_sql_update(
            c,
            column,
            new_value,
            db_value,
            power_unit_id,
            dict_,
            power_unit_shadow,
            structure,
            aws_thing,
        )

        html = update_gw_power_unit_id_from_shadow.get_html(
            power_unit_shadow, sql_update, dict_
        )

        # mock_send_mailgun_email.assert_called_once()
        mock_send_mailgun_email.assert_called_once_with(
            c,
            text="",
            html=html,
            emailees_list=["smccarthy@myijack.com"],
            subject="NOT updating GPS in structures table - just testing!",
        )

    @patch("cron_d.update_gw_power_unit_id_from_shadow.send_mailgun_email")
    @patch("cron_d.update_gw_power_unit_id_from_shadow.run_query")
    def test_compare_shadow_and_db(self, mock_run_query, mock_send_mailgun_email):
        """Test that a small change will trigger an update"""
        global c
        structure_ging = 10002
        power_unit_ging = 10002
        power_unit_ging_id = 316
        aws_thing_ging = "00:60:E0:84:A7:15"
        mock_run_query.return_value = None, [
            OrderedDict(
                [
                    ("id", 660),
                    ("structure", 10002.0),
                    ("structure_slave_id", None),
                    ("structure_slave", None),
                    ("downhole", "Ging's basement July 2021"),
                    ("surface", "Calgary"),
                    ("location", "Ging's basement July 2021 @ Calgary"),
                    ("gps_lat", 51.008458),
                    ("gps_lon", -114.073852),
                    ("power_unit_id", 316),
                    ("power_unit", 10002.0),
                    ("power_unit_str", "10002"),
                    ("gateway_id", 120),
                    ("gateway", "00:60:E0:84:A7:15"),
                    ("aws_thing", "00:60:E0:84:A7:15"),
                    ("qb_sale", None),
                    ("unit_type_id", 2),
                    ("unit_type", "XFER"),
                    ("model_type_id", 44),
                    ("model", "TEST"),
                    ("model_unit_type_id", 8),
                    ("model_unit_type", "TEST"),
                    ("model_type_id_slave", None),
                    ("model_slave", None),
                    ("model_unit_type_id_slave", None),
                    ("model_unit_type_slave", None),
                    ("customer_id", 1),
                    ("customer", "IJACK"),
                    ("cust_sub_group_id", 45),
                    ("cust_sub_group", "Calgary Ging's Basement"),
                    ("run_mfg_date", date(2022, 1, 1)),
                    ("structure_install_date", date(2021, 1, 28)),
                    ("slave_install_date", None),
                    (
                        "notes_1",
                        "For testing new gateways that don't have a structure or power unit yet",
                    ),
                    ("well_license", ""),
                    ("time_zone_id", 3),
                    ("time_zone", "America/Edmonton"),
                    ("apn", ""),
                ]
            )
        ]

        update_gw_power_unit_id_from_shadow.compare_shadow_and_db(
            c=c,
            shadow_value=-108.01001,
            db_value=-108.0,
            db_column="gps_lon",
            power_unit_id=power_unit_ging_id,
            power_unit_shadow=power_unit_ging,
            structure=structure_ging,
            aws_thing=aws_thing_ging,
        )

        mock_send_mailgun_email.assert_called_once()
        self.assertTrue(mock_run_query.call_count == 2)

        # Run the test a second time, with a smaller change, and assert it doesn't trigger an update
        mock_send_mailgun_email.reset_mock()
        mock_run_query.reset_mock()

        update_gw_power_unit_id_from_shadow.compare_shadow_and_db(
            c=c,
            shadow_value=-108.009,
            db_value=-108.0,
            db_column="gps_lon",
            power_unit_id=power_unit_ging_id,
            power_unit_shadow=power_unit_ging,
            structure=structure_ging,
            aws_thing=aws_thing_ging,
        )

        mock_send_mailgun_email.assert_not_called()
        mock_run_query.assert_not_called()


if __name__ == "__main__":
    unittest.main()
