# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

from pathlib import Path
import pickle
import sys
from typing import OrderedDict
from datetime import date
import unittest
from unittest.mock import patch, MagicMock

# from psycopg2.extras import DictCursor
from psycopg2.sql import SQL, Literal
import time
from typing import Tuple

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from cron_d import update_info_from_shadows
from cron_d.utils import Config, configure_logging, run_query, get_conn


LOGFILE_NAME = "test_main_programs"

c = Config()
c.DEV_TEST_PRD = "development"
c.logger = configure_logging(
    __name__, logfile_name=LOGFILE_NAME, path_to_log_directory="/var/log/"
)

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

    @patch("cron_d.update_info_from_shadows.send_mailgun_email")
    @patch("cron_d.update_info_from_shadows.run_query")
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

        sql_get_info_str = update_info_from_shadows.sql_get_info(
            c, power_unit_id, power_unit_shadow, structure, aws_thing
        )

        _, rows = run_query(
            c, sql_get_info_str, db="ijack", execute=True, fetchall=True, commit=False
        )
        mock_run_query.return_value = (["col1", "col2"], rows)

        # Run the main function we're testing
        update_info_from_shadows.update_structures_table(
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
        sql_update = update_info_from_shadows.get_sql_update(
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

        html = update_info_from_shadows.get_html(power_unit_shadow, sql_update, dict_)

        # mock_send_mailgun_email.assert_called_once()
        mock_send_mailgun_email.assert_called_once_with(
            c,
            text="",
            html=html,
            emailees_list=["smccarthy@myijack.com"],
            subject="NOT updating GPS in structures table - just testing!",
        )

    @patch("cron_d.update_info_from_shadows.send_mailgun_email")
    @patch("cron_d.update_info_from_shadows.run_query")
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

        update_info_from_shadows.compare_shadow_and_db(
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

        update_info_from_shadows.compare_shadow_and_db(
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

    @patch("cron_d.update_info_from_shadows.upsert_gw_info")
    @patch("cron_d.update_info_from_shadows.run_query")
    @patch("cron_d.update_info_from_shadows.get_client_iot")
    @patch("cron_d.update_info_from_shadows.get_device_shadows_in_threadpool")
    @patch("cron_d.update_info_from_shadows.get_structure_records")
    @patch("cron_d.update_info_from_shadows.get_power_unit_records")
    @patch("cron_d.update_info_from_shadows.get_gateway_records")
    @patch("cron_d.update_info_from_shadows.get_conn")
    @patch("cron_d.update_info_from_shadows.exit_if_already_running")
    def test_need_to_update_power_unit_for_gateway(
        self,
        mock_exit_if_already_running,
        mock_get_conn,
        mock_get_gateway_records,
        mock_get_power_unit_records,
        mock_get_structure_records,
        mock_get_device_shadows_in_threadpool,
        mock_get_client_iot,
        mock_run_query,
        mock_upsert_gw_info,
    ):
        """Test that a small change will trigger an update"""
        global c
        c.TEST_FUNC = False
        c.EMAIL_LIST_SERVICE_PRODUCTION_IT = ["smccarthy@myijack.com"]

        structure_ging = 10002
        power_unit_ging = 10002
        power_unit_ging_id = 316
        aws_thing_ging = "00:60:E0:84:A7:15"

        mocks = {
            "gw_rows.pkl": mock_get_gateway_records,
            "pu_rows.pkl": mock_get_power_unit_records,
            "structure_rows.pkl": mock_get_structure_records,
            "shadows.pkl": mock_get_device_shadows_in_threadpool,
        }
        for filename, mock in mocks.items():
            with open(str(fixture_folder.joinpath(filename)), "rb") as file:
                mock.return_value = pickle.load(file)

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

        update_info_from_shadows.main(c=c, commit=False)

        mock_exit_if_already_running.assert_called_once()
        mock_get_conn.assert_called_once()
        mock_get_gateway_records.assert_called_once()
        mock_get_power_unit_records.assert_called_once()
        mock_get_structure_records.assert_called_once()
        mock_get_device_shadows_in_threadpool.assert_called_once()
        mock_get_client_iot.assert_called_once()

        self.assertEqual(mock_run_query.call_count, 2)
        mock_upsert_gw_info.assert_called()

        # Run the test a second time, with a smaller change, and assert it doesn't trigger an update
        # mock_send_mailgun_email.reset_mock()
        mock_run_query.reset_mock()
        mock_upsert_gw_info.reset_mock()

        update_info_from_shadows.compare_shadow_and_db(
            c=c,
            shadow_value=-108.009,
            db_value=-108.0,
            db_column="gps_lon",
            power_unit_id=power_unit_ging_id,
            power_unit_shadow=power_unit_ging,
            structure=structure_ging,
            aws_thing=aws_thing_ging,
        )

        # mock_send_mailgun_email.assert_not_called()
        mock_run_query.assert_not_called()

    def test_upsert_gw_info(self):
        """Test the 'upsert_gw_info' function"""
        global c
        c.TEST_FUNC = False

        # lambda_access gateway
        aws_thing = "lambda_access"
        gateway_id = 93

        conn = get_conn(c, db="ijack")

        try:
            # Update the record
            os_pretty_name_updated = "some pretty OS"
            sql = f"""
            INSERT INTO public.gw_info
                (gateway_id, aws_thing, os_name, os_pretty_name)
                VALUES ({gateway_id}, '{aws_thing}', null, '{os_pretty_name_updated}')
                ON CONFLICT (gateway_id) DO UPDATE
                    set os_name = null, os_pretty_name = '{os_pretty_name_updated}'
            """
            run_query(c, sql, db="ijack", fetchall=False, conn=conn, commit=True)

            # Check the record
            sql = f"select os_name, os_pretty_name from public.gw_info where gateway_id = {gateway_id}"
            columns, rows = run_query(
                c, sql, db="ijack", fetchall=True, conn=conn, commit=False
            )
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
                c, gateway_id, aws_thing, shadow, conn
            )

            self.assertTrue(bool_return)

            # Check the record
            sql = f"select os_name, os_pretty_name from public.gw_info where gateway_id = {gateway_id}"
            columns, rows = run_query(
                c, sql, db="ijack", fetchall=True, conn=conn, commit=False
            )
            self.assertTrue(rows)
            self.assertEqual(rows[0]["os_name"], os_wanted)

            # This one doesn't get updated since it's not uppercase
            os_pretty_name_in_db = rows[0]["os_pretty_name"]
            self.assertNotEqual(os_pretty_name_in_db, os_pretty_name_not_updated)
            self.assertEqual(os_pretty_name_in_db, os_pretty_name_updated)
        except Exception:
            raise
        finally:
            conn.close()

    def test_upsert_gw_info_no_aws_thing_no_gateway_id(self):
        """Test the 'upsert_gw_info' function"""
        global c
        c.TEST_FUNC = False

        bool_return = update_info_from_shadows.upsert_gw_info(
            c, gateway_id=None, aws_thing="something", shadow={}, conn=MagicMock()
        )
        self.assertFalse(bool_return)

        bool_return = update_info_from_shadows.upsert_gw_info(
            c, gateway_id="something", aws_thing=None, shadow={}, conn=MagicMock()
        )
        self.assertFalse(bool_return)

    def test_record_can_bus_cellular_test(self):
        """Test that we can record when gateways are correctly tested"""
        global c
        gateway_id_lambda_access = 93

        def test_gw_table(gateway_id) -> Tuple[bool, bool]:
            """Check what's in the public.gw table"""
            columns, rows = run_query(
                c,
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
                c,
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
            run_query(
                c,
                SQL("delete from gw_tested_cellular where id = {gateway_id}").format(
                    gateway_id=Literal(gateway_id)
                ),
                fetchall=False,
                commit=True,
            )

        try:
            conn = get_conn(c, db="ijack")

            bool_return = update_info_from_shadows.record_can_bus_cellular_test(
                c, gateway_id_lambda_access, cellular_good=False, can_bus_good=False
            )
            can_bus, cellular = test_gw_table(gateway_id_lambda_access)
            self.assertFalse(can_bus)
            self.assertFalse(cellular)

            timestamp_utc, user_id, gateway_id, network_id = test_gw_inserted_table(
                gateway_id_lambda_access
            )
            self.assertIsNone(timestamp_utc)
            self.assertIsNone(user_id)
            self.assertIsNone(gateway_id)
            self.assertIsNone(network_id)

            bool_return = update_info_from_shadows.record_can_bus_cellular_test(
                c, gateway_id_lambda_access, cellular_good=True, can_bus_good=True
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
            conn.close()

        self.assertTrue(bool_return)


if __name__ == "__main__":
    unittest.main()
