# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
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
            power_unit_id, power_unit_shadow, structure, aws_thing
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
            execute=False,
        )

        dict_ = rows[0]
        sql_update = update_gw_power_unit_id_from_shadow.get_sql_update(
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
            subject="Updating GPS in public.structures table!",
        )


if __name__ == "__main__":
    unittest.main()
