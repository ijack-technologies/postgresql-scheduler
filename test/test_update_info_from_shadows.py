# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import pickle
import sys
import time
import unittest
from datetime import date
from pathlib import Path
from typing import OrderedDict, Tuple
from unittest.mock import patch

# from psycopg2.extras import DictCursor
from psycopg2.sql import SQL, Literal

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from project import update_info_from_shadows
from project.logger_config import logger
from project.utils import Config, get_conn, run_query

LOGFILE_NAME = "test_main_programs"

c = Config()
c.DEV_TEST_PRD = "development"


fixture_folder = Path(pythonpath).joinpath("test").joinpath("fixtures")


class TestAll(unittest.TestCase):
    # # executed after each test
    # def tearDown(self):
    #     pass

    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        c.TEST_FUNC = True

    @patch("project.update_info_from_shadows.send_mailgun_email")
    @patch("project.update_info_from_shadows.run_query")
    def test_update_structures_table_gps(
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
        power_unit_shadow = "200476"
        gps_lon_new = -121.0
        gps_lon_old = -120.0
        gps_lat_new = 51.0
        gps_lat_old = 51.0
        structure = 190619
        aws_thing = "1000051"

        sql_get_info_str = update_info_from_shadows.sql_get_info(
            c, power_unit_id, power_unit_shadow, structure, aws_thing
        )

        _, rows = run_query(sql_get_info_str, db="ijack", fetchall=True, commit=False)
        mock_run_query.return_value = (["col1", "col2"], rows)

        # Run the main function we're testing
        update_info_from_shadows.update_structures_table_gps(
            c,
            power_unit_id=power_unit_id,
            power_unit_shadow_str=power_unit_shadow,
            gps_lat_new=gps_lat_new,
            gps_lat_old=gps_lat_old,
            gps_lon_new=gps_lon_new,
            gps_lon_old=gps_lon_old,
            structure=structure,
            aws_thing=aws_thing,
            # Testing only
            execute=False,
            commit=False,
        )

        dict_ = rows[0]
        sql_update = update_info_from_shadows.get_sql_update(
            c,
            gps_lat_new=gps_lat_new,
            gps_lat_old=gps_lat_old,
            gps_lon_new=gps_lon_new,
            gps_lon_old=gps_lon_old,
            power_unit_id=power_unit_id,
            dict_=dict_,
            power_unit_shadow_str=power_unit_shadow,
            structure=structure,
            aws_thing=aws_thing,
        )

        update_info_from_shadows.get_html(power_unit_shadow, sql_update, dict_)

        # mock_send_mailgun_email.assert_called_once()
        # mock_send_mailgun_email.assert_called_once_with(
        #     c,
        #     text="",
        #     html=html,
        #     emailees_list=["smccarthy@myijack.com"],
        #     subject="NOT updating GPS in structures table - just testing!",
        # )
        mock_send_mailgun_email.assert_not_called()

    @patch("project.update_info_from_shadows.send_mailgun_email")
    @patch("project.update_info_from_shadows.run_query")
    def test_compare_shadow_and_db_gps(self, mock_run_query, mock_send_mailgun_email):
        """Test that a small change in GPS will trigger an update"""
        global c
        structure_ging = 10009
        power_unit_ging = 10009
        power_unit_ging_id = 316
        aws_thing_ging = "00:60:E0:84:A7:15"
        mock_run_query.return_value = (
            None,
            [
                OrderedDict(
                    [
                        ("id", 660),
                        ("structure", 10009.0),
                        ("structure_slave_id", None),
                        ("structure_slave", None),
                        ("downhole", "Ging's basement July 2021"),
                        ("surface", "Calgary"),
                        ("location", "Ging's basement July 2021 @ Calgary"),
                        ("gps_lat", 51.008458),
                        ("gps_lon", -114.073852),
                        ("power_unit_id", 316),
                        ("power_unit", 10009.0),
                        ("power_unit_str", "10009"),
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
            ],
        )

        km = update_info_from_shadows.calc_distance(
            lat1=51.0, lon1=-108.0, lat2=51.0, lon2=-108.01001
        )
        self.assertGreater(km, 0.01)

        update_info_from_shadows.compare_shadow_and_db_gps(
            c=c,
            lat_shadow_float=51.0,
            lat_db_float=51.0,
            lon_shadow_float=-108.01001,
            lon_db_float=-108.0,
            power_unit_id=power_unit_ging_id,
            power_unit_shadow_str=power_unit_ging,
            structure=structure_ging,
            aws_thing=aws_thing_ging,
        )

        # mock_send_mailgun_email.assert_called_once()
        mock_send_mailgun_email.assert_not_called()
        self.assertTrue(mock_run_query.call_count == 2)

        # Run the test a second time, with a smaller change, and assert it doesn't trigger an update
        mock_send_mailgun_email.reset_mock()
        mock_run_query.reset_mock()

        km = update_info_from_shadows.calc_distance(
            lat1=51.0, lon1=-108.0, lat2=51.0, lon2=-108.00009
        )
        self.assertLess(km, 0.01)

        update_info_from_shadows.compare_shadow_and_db_gps(
            c=c,
            lat_shadow_float=51.0,
            lat_db_float=51.0,
            lon_shadow_float=-108.00009,
            lon_db_float=-108.0,
            power_unit_id=power_unit_ging_id,
            power_unit_shadow_str=power_unit_ging,
            structure=structure_ging,
            aws_thing=aws_thing_ging,
        )

        mock_send_mailgun_email.assert_not_called()
        mock_run_query.assert_not_called()

    @patch("project.update_info_from_shadows.already_emailed_recently")
    @patch("project.update_info_from_shadows.record_email_sent")
    @patch("project.update_info_from_shadows.send_mailgun_email")
    @patch("project.update_info_from_shadows.upsert_gw_info")
    @patch("project.update_info_from_shadows.run_query")
    @patch("project.update_info_from_shadows.get_client_iot")
    @patch("project.update_info_from_shadows.get_device_shadows_in_threadpool")
    # @patch("project.update_info_from_shadows.get_structure_records")
    # @patch("project.update_info_from_shadows.get_power_unit_records")
    @patch("project.update_info_from_shadows.get_gateway_records")
    @patch("project.update_info_from_shadows.get_conn")
    @patch("project.update_info_from_shadows.exit_if_already_running")
    def test_must_update_gps_for_unit(
        self,
        mock_exit_if_already_running,
        mock_get_conn,
        mock_get_gateway_records,
        # mock_get_power_unit_records,
        # mock_get_structure_records,
        mock_get_device_shadows_in_threadpool,
        mock_get_client_iot,
        mock_run_query,
        mock_upsert_gw_info,
        mock_send_mailgun_email,
        mock_record_email_sent,
        mock_already_emailed_recently,
    ):
        """Test that a small change will trigger an update for the unit's GPS location"""
        global c
        c.TEST_FUNC = False
        c.EMAIL_LIST_SERVICE_PRODUCTION_IT = ["smccarthy@myijack.com"]

        structure_ging = 10009
        power_unit_ging = 10009
        power_unit_ging_id = 316
        aws_thing_ging = "00:60:E0:84:A7:15"
        mock_already_emailed_recently.return_value = False

        mocks = {
            "gw_rows.pkl": mock_get_gateway_records,
            # "pu_rows.pkl": mock_get_power_unit_records,
            # "structure_rows.pkl": mock_get_structure_records,
            "shadows.pkl": mock_get_device_shadows_in_threadpool,
        }
        for filename, mock in mocks.items():
            with open(str(fixture_folder.joinpath(filename)), "rb") as file:
                mock.return_value = pickle.load(file)

        mock_run_query.return_value = (
            None,
            [
                OrderedDict(
                    [
                        ("id", 660),
                        ("structure", 10009.0),
                        ("structure_slave_id", None),
                        ("structure_slave", None),
                        ("downhole", "Ging's basement July 2021"),
                        ("surface", "Calgary"),
                        ("location", "Ging's basement July 2021 @ Calgary"),
                        ("gps_lat", 51.008458),
                        ("gps_lon", -114.22),
                        ("power_unit_id", 316),
                        ("power_unit", 200999.0),
                        ("power_unit_str", "200999"),
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
            ],
        )

        update_info_from_shadows.main(c=c, commit=False)

        # The info from AWS RDS is updated in the gateway's AWS IoT thing shadow,
        # so the gateway can update its config
        mock_upsert_gw_info.assert_called()
        mock_exit_if_already_running.assert_called_once()
        mock_get_conn.assert_called_once()
        mock_get_gateway_records.assert_called_once()
        # mock_get_power_unit_records.assert_called_once()
        # mock_get_structure_records.assert_called_once()
        mock_get_device_shadows_in_threadpool.assert_called_once()
        mock_get_client_iot.assert_called_once()
        # These get called if something else is updated
        mock_already_emailed_recently.assert_not_called()
        mock_record_email_sent.assert_not_called()

        # The power unit and gateway are already matched
        # power_unit_id_shadow == power_unit_id_gw
        mock_send_mailgun_email.assert_not_called()

        # 1. query vw_structures_joined for structure info
        # 2. update_structures_table_gps
        self.assertEqual(mock_run_query.call_count, 2)

        # Run the test a second time, with a smaller change,
        # and assert it doesn't trigger a GPS update
        mock_send_mailgun_email.reset_mock()
        mock_run_query.reset_mock()
        mock_upsert_gw_info.reset_mock()
        mock_already_emailed_recently.reset_mock()
        mock_record_email_sent.reset_mock()

        km = update_info_from_shadows.calc_distance(
            lat1=51.0, lon1=-108.0, lat2=51.0, lon2=-108.00009
        )
        self.assertLess(km, 0.01)

        update_info_from_shadows.compare_shadow_and_db_gps(
            c=c,
            lat_shadow_float=51.0,
            lat_db_float=51.0,
            lon_shadow_float=-108.00009,
            lon_db_float=-108.0,
            power_unit_id=power_unit_ging_id,
            power_unit_shadow_str=power_unit_ging,
            structure=structure_ging,
            aws_thing=aws_thing_ging,
        )

        mock_send_mailgun_email.assert_not_called()
        mock_run_query.assert_not_called()
        mock_record_email_sent.assert_not_called()
        mock_already_emailed_recently.assert_not_called()

    def test_upsert_gw_info(self):
        """Test the 'upsert_gw_info' function"""
        global c
        c.TEST_FUNC = False

        # lambda_access gateway
        aws_thing = "lambda_access"
        gateway_id = 93

        with get_conn(db="aws_rds"):
            # Update the record
            os_pretty_name_updated = "some pretty OS"
            sql = f"""
            INSERT INTO public.gw_info
                (gateway_id, aws_thing, os_name, os_pretty_name)
                VALUES ({gateway_id}, '{aws_thing}', null, '{os_pretty_name_updated}')
                ON CONFLICT (gateway_id) DO UPDATE
                    set os_name = null, os_pretty_name = '{os_pretty_name_updated}'
            """
            run_query(sql, db="ijack", fetchall=False, commit=True)

            # Check the record
            sql = f"select os_name, os_pretty_name from public.gw_info where gateway_id = {gateway_id}"
            columns, rows = run_query(sql, db="ijack", fetchall=True, commit=False)
            self.assertEqual(columns, ["os_name", "os_pretty_name"])
            self.assertIsNone(rows[0]["os_name"])
            self.assertEqual(rows[0]["os_pretty_name"], os_pretty_name_updated)

            os_wanted = "Sean's OS"
            os_pretty_name_not_updated = "Sean's OS - Pretty"
            timestamp_10_seconds_ago = time.time() - 10
            shadow = {
                "state": {
                    "reported": {
                        "OS_NAME": os_wanted,
                        # Test lowercase doesn't get updated
                        # since it's looking for an uppercase metric name
                        "os_pretty_name": os_pretty_name_not_updated,
                        "OS_VERSION": "",
                        "OS_VERSION_ID": "",
                        "OS_RELEASE": "",
                        "OS_MACHINE": "",
                        "OS_PLATFORM": "",
                        "OS_PYTHON_VERSION": "",
                        "MODEM_MODEL": "",
                        "MODEM_FIRMWARE_REV": "",
                        "MODEM_DRIVERS": "",
                        "SIM_OPERATOR": "",
                        "HYD_EGAS": 1,
                        "WARN1_EGAS": 0,
                        "WARN2_EGAS": 0,
                        "connected": 1,
                        "SERIAL_NUMBER": 200999,
                        "HOURS": 666777.5,
                    }
                },
                "metadata": {
                    "reported": {
                        "OS_NAME": {"timestamp": timestamp_10_seconds_ago},
                        "os_pretty_name": {"timestamp": timestamp_10_seconds_ago},
                        "OS_VERSION": {"timestamp": timestamp_10_seconds_ago},
                        "OS_VERSION_ID": {"timestamp": timestamp_10_seconds_ago},
                        "OS_RELEASE": {"timestamp": timestamp_10_seconds_ago},
                        "OS_MACHINE": {"timestamp": timestamp_10_seconds_ago},
                        "OS_PLATFORM": {"timestamp": timestamp_10_seconds_ago},
                        "OS_PYTHON_VERSION": {"timestamp": timestamp_10_seconds_ago},
                        "MODEM_MODEL": {"timestamp": timestamp_10_seconds_ago},
                        "MODEM_FIRMWARE_REV": {"timestamp": timestamp_10_seconds_ago},
                        "MODEM_DRIVERS": {"timestamp": timestamp_10_seconds_ago},
                        "SIM_OPERATOR": {"timestamp": timestamp_10_seconds_ago},
                    }
                },
            }

            bool_return = update_info_from_shadows.upsert_gw_info(
                c, gateway_id, aws_thing, shadow
            )

            self.assertTrue(bool_return)

            # Check the record
            sql = f"select os_name, os_pretty_name from public.gw_info where gateway_id = {gateway_id}"
            columns, rows = run_query(sql, db="ijack", fetchall=True, commit=False)
            self.assertTrue(rows)
            self.assertEqual(rows[0]["os_name"], os_wanted)

            # This one doesn't get updated since it's not uppercase
            os_pretty_name_in_db = rows[0]["os_pretty_name"]
            self.assertNotEqual(os_pretty_name_in_db, os_pretty_name_not_updated)
            self.assertEqual(os_pretty_name_in_db, os_pretty_name_updated)

    def test_upsert_gw_info_no_aws_thing_no_gateway_id(self):
        """Test the 'upsert_gw_info' function"""
        global c
        c.TEST_FUNC = False

        bool_return = update_info_from_shadows.upsert_gw_info(
            c, gateway_id=None, aws_thing="something", shadow={}
        )
        self.assertFalse(bool_return)

        bool_return = update_info_from_shadows.upsert_gw_info(
            c, gateway_id="something", aws_thing=None, shadow={}
        )
        self.assertFalse(bool_return)

    def test_record_can_bus_cellular_test(self):
        """Test that we can record when gateways are correctly tested"""
        global c
        gateway_id_lambda_access = 93

        def test_gw_table(gateway_id) -> Tuple[bool, bool]:
            """Check what's in the public.gw table"""
            columns, rows = run_query(
                SQL(
                    "select test_can_bus, test_cellular from public.gw where id = {gateway_id}"
                ).format(gateway_id=Literal(gateway_id)),
                fetchall=True,
            )
            if not rows:
                return None, None
            test_can_bus = rows[0]["test_can_bus"]
            test_cellular = rows[0]["test_cellular"]
            return test_can_bus, test_cellular

        def test_gw_inserted_table(gateway_id) -> Tuple[bool, bool]:
            """Check what's in the public.gw_tested_cellular table"""
            columns, rows = run_query(
                SQL(
                    """SELECT id, user_id, timestamp_utc, gateway_id, network_id FROM public.gw_tested_cellular where gateway_id = {gateway_id}"""
                ).format(gateway_id=Literal(gateway_id)),
                fetchall=True,
            )
            if not rows:
                return None, None, None, None
            timestamp_utc = rows[0]["timestamp_utc"]
            user_id = rows[0]["user_id"]
            gateway_id = rows[0]["gateway_id"]
            network_id = rows[0]["network_id"]
            return timestamp_utc, user_id, gateway_id, network_id

        def delete_test_insert(gateway_id):
            """Delete the data we test-inserted"""
            sql = SQL(
                """
                delete from public.gw_tested_cellular
                where gateway_id = {gateway_id}
                """
            ).format(gateway_id=Literal(gateway_id))
            print(sql)
            run_query(
                sql=sql,
                fetchall=False,
                commit=True,
            )

        try:
            bool_return = update_info_from_shadows.record_can_bus_cellular_test(
                gateway_id_lambda_access, cellular_good=False, can_bus_good=False
            )
            can_bus, cellular = test_gw_table(gateway_id_lambda_access)
            self.assertFalse(can_bus)
            self.assertFalse(cellular)

            timestamp_utc, user_id, gateway_id, network_id = test_gw_inserted_table(
                gateway_id_lambda_access
            )
            try:
                self.assertIsNone(timestamp_utc)
            except AssertionError:
                delete_test_insert(gateway_id_lambda_access)
                raise
            self.assertIsNone(user_id)
            self.assertIsNone(gateway_id)
            self.assertIsNone(network_id)

            bool_return = update_info_from_shadows.record_can_bus_cellular_test(
                gateway_id_lambda_access, cellular_good=True, can_bus_good=True
            )
            can_bus, cellular = test_gw_table(gateway_id_lambda_access)
            self.assertTrue(can_bus)
            self.assertTrue(cellular)

            timestamp_utc, user_id, gateway_id, network_id = test_gw_inserted_table(
                gateway_id_lambda_access
            )
            user_id_shop_auto = 788  # SHOP automated user
            network_id_sasktel = 1
            self.assertEqual(user_id, user_id_shop_auto)
            self.assertEqual(network_id, network_id_sasktel)

        finally:
            delete_test_insert(gateway_id_lambda_access)

        self.assertTrue(bool_return)

    def test_gateway_with_dict_timestamps(self):
        """
        Test gateway 00:60:E0:81:4C:67 (power unit 10014) that had dict timestamps
        causing TypeError: '>' not supported between instances of 'dict' and 'int'

        This test verifies that both seconds_since_last_any_msg() and
        get_shadow_table_html() properly handle dict values in timestamp fields.

        Related commits:
        - 14b7e4e: Fix TypeError in seconds_since_last_any_msg function
        - 12301e1: Fix additional TypeError in timestamp sorting
        """
        global c
        c.TEST_FUNC = False

        # Gateway info from error report
        aws_thing = "00:60:E0:81:4C:67"
        power_unit = 10014

        # Get the AWS IoT client and fetch the actual shadow
        client_iot = update_info_from_shadows.get_client_iot()
        shadow = update_info_from_shadows.get_iot_device_shadow(
            c, client_iot, aws_thing
        )

        # Verify we got a valid shadow
        self.assertIsInstance(shadow, dict)
        self.assertIn("state", shadow)

        # Test seconds_since_last_any_msg - this was throwing TypeError at line 670
        # The fix at lines 671-672 checks if time_received is numeric before comparing
        seconds_since, msg, latest_metric = (
            update_info_from_shadows.seconds_since_last_any_msg(c, shadow)
        )

        # Verify the function returned valid values
        self.assertIsInstance(seconds_since, (int, float))
        self.assertIsInstance(msg, str)
        self.assertIsInstance(latest_metric, (str, type(None)))

        # Test get_shadow_table_html - this was also throwing TypeError at line 507
        # The fix at lines 505-510 filters out non-numeric timestamps before sorting
        html = update_info_from_shadows.get_shadow_table_html(c, shadow)

        # Verify HTML was generated
        self.assertIsInstance(html, str)
        self.assertIn("<table>", html)

        # Log success for debugging
        logger.info(
            f"Successfully tested gateway {aws_thing} (power unit {power_unit})"
        )
        logger.info(f"Last message: {msg} ago, metric: {latest_metric}")


if __name__ == "__main__":
    unittest.main()
