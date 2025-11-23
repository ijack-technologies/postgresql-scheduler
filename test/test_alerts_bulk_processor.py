"""
Test module for alerts_bulk_processor following DRY and SOLID principles.

Tests cover:
- Processing bulk alerts
- Matching power units with filters
- Upserting individual alerts
- Error handling
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from project.alerts_bulk_processor import AlertBulkProcessor, main
from project.utils import Config


class TestAlertBulkProcessor(unittest.TestCase):
    """Test cases for the AlertBulkProcessor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = Config()
        self.config.DEV_TEST_PRD = "development"
        self.config.TEST_FUNC = True
        self.processor = AlertBulkProcessor(self.config)

    @patch("project.alerts_bulk_processor.run_query")
    def test_process_all_bulk_alerts_no_subscriptions(self, mock_run_query):
        """Test processing when no bulk alert subscriptions exist."""
        # Mock empty result
        mock_run_query.return_value = (None, [])

        result = self.processor.process_all_bulk_alerts()

        # Verify results
        self.assertEqual(result["bulk_subscriptions_processed"], 0)
        self.assertEqual(result["power_units_found"], 0)
        self.assertEqual(result["alerts_inserted"], 0)
        self.assertEqual(result["alerts_updated"], 0)
        self.assertEqual(result["errors"], 0)

        # Verify the query was called
        mock_run_query.assert_called_once_with(
            "SELECT * FROM public.alerts_bulk", db="ijack", fetchall=True
        )

    @patch("project.alerts_bulk_processor.run_query")
    def test_process_all_bulk_alerts_with_subscriptions(self, mock_run_query):
        """Test processing with bulk alert subscriptions."""
        # Mock bulk alert data
        bulk_alerts = [
            {
                "id": 1,
                "user_id": 100,
                "unit_type_id": 1,
                "model_type_id": None,
                "customer_id": None,
                "update_existing_alerts": True,
                "wants_sms": True,
                "wants_email": False,
                "heartbeat": True,
                "change_suction": True,
            },
            {
                "id": 2,
                "user_id": 200,
                "unit_type_id": None,
                "model_type_id": 2,
                "customer_id": 10,
                "update_existing_alerts": False,
                "wants_sms": False,
                "wants_email": True,
                "heartbeat": False,
                "change_dgp": True,
            },
        ]

        # Mock matching power units
        power_units = [{"power_unit_id": 1001}, {"power_unit_id": 1002}]

        # Mock query responses for batch processing
        mock_run_query.side_effect = [
            (None, bulk_alerts),  # Get bulk alerts
            (None, power_units),  # Get matching power units for alert 1
            # Batch upsert for alert 1 (both power units in one query)
            (
                None,
                [
                    {"power_unit_id": 1001, "inserted": True},
                    {"power_unit_id": 1002, "inserted": False},
                ],
            ),
            (None, []),  # No matching power units for alert 2
        ]

        result = self.processor.process_all_bulk_alerts()

        # Verify results
        self.assertEqual(result["bulk_subscriptions_processed"], 2)
        self.assertEqual(result["power_units_found"], 2)
        self.assertEqual(result["alerts_inserted"], 1)
        self.assertEqual(result["alerts_updated"], 1)
        self.assertEqual(result["errors"], 0)

    @patch("project.alerts_bulk_processor.run_query")
    def test_get_matching_power_units_with_all_filters(self, mock_run_query):
        """Test getting matching power units with all filters specified."""
        bulk_alert = {
            "id": 1,
            "unit_type_id": 5,
            "model_type_id": 10,
            "customer_id": 20,
        }

        # Mock query result
        mock_run_query.return_value = (
            None,
            [{"power_unit_id": 1001}, {"power_unit_id": 1002}],
        )

        result = self.processor._get_matching_power_units(bulk_alert)

        # Verify result
        self.assertEqual(result, [1001, 1002])

        # Verify the query includes all filters
        call_args = mock_run_query.call_args
        query = call_args[0][0]
        params = call_args[1]["data"]

        self.assertIn("t1.unit_type_id = %s", query)
        self.assertIn("t1.model_type_id = %s", query)
        self.assertIn("t4.customer_id = %s", query)
        self.assertEqual(params, (5, 10, 20))

    @patch("project.alerts_bulk_processor.run_query")
    def test_get_matching_power_units_with_no_filters(self, mock_run_query):
        """Test getting matching power units with no filters (wildcards)."""
        bulk_alert = {
            "id": 1,
            "unit_type_id": None,
            "model_type_id": None,
            "customer_id": None,
        }

        # Mock query result
        mock_run_query.return_value = (
            None,
            [{"power_unit_id": 1001}, {"power_unit_id": 1002}, {"power_unit_id": 1003}],
        )

        result = self.processor._get_matching_power_units(bulk_alert)

        # Verify result
        self.assertEqual(result, [1001, 1002, 1003])

        # Verify the query doesn't include optional filters but includes wildcard conditions
        call_args = mock_run_query.call_args
        query = call_args[0][0]
        params = call_args[1]["data"]

        # Should not have filter conditions
        self.assertNotIn("t1.unit_type_id = %s", query)
        self.assertNotIn("t1.model_type_id = %s", query)
        self.assertNotIn("t4.customer_id = %s", query)

        # Should have wildcard conditions
        self.assertIn("t1.unit_type_id IS NOT NULL", query)
        self.assertIn("t1.model_type_id IS NOT NULL", query)

        self.assertEqual(params, ())

    @patch("project.alerts_bulk_processor.run_query")
    def test_upsert_individual_alert_insert(self, mock_run_query):
        """Test inserting a new individual alert."""
        bulk_alert = {
            "id": 1,
            "user_id": 100,
            "wants_sms": True,
            "wants_email": False,
            "heartbeat": True,
            "change_suction": True,
            # Include all other fields...
        }
        power_unit_id = 1001

        # Mock insert result
        mock_run_query.return_value = (None, [{"inserted": True}])

        self.processor._upsert_individual_alert(bulk_alert, power_unit_id)

        # Verify stats updated
        self.assertEqual(self.processor.stats["alerts_inserted"], 1)
        self.assertEqual(self.processor.stats["alerts_updated"], 0)

        # Verify query was called with correct parameters
        call_args = mock_run_query.call_args
        self.assertIn("INSERT INTO public.alerts", call_args[0][0])
        self.assertIn("ON CONFLICT", call_args[0][0])
        self.assertEqual(call_args[1]["data"]["user_id"], 100)
        self.assertEqual(call_args[1]["data"]["power_unit_id"], 1001)

    @patch("project.alerts_bulk_processor.run_query")
    def test_upsert_individual_alert_update(self, mock_run_query):
        """Test updating an existing individual alert."""
        bulk_alert = {
            "id": 1,
            "user_id": 100,
            "wants_sms": False,
            "wants_email": True,
            "heartbeat": False,
            "change_dgp": True,
        }
        power_unit_id = 1001

        # Mock update result
        mock_run_query.return_value = (None, [{"inserted": False}])

        self.processor._upsert_individual_alert(bulk_alert, power_unit_id)

        # Verify stats updated
        self.assertEqual(self.processor.stats["alerts_inserted"], 0)
        self.assertEqual(self.processor.stats["alerts_updated"], 1)

    @patch("project.alerts_bulk_processor.run_query")
    def test_create_new_alert_only_existing_alert(self, mock_run_query):
        """Test create_new_alert_only when alert already exists."""
        bulk_alert = {"id": 1, "user_id": 100, "update_existing_alerts": False}
        power_unit_id = 1001

        # Mock existing alert
        mock_run_query.return_value = (None, [{"id": 999}])

        self.processor._create_new_alert_only(bulk_alert, power_unit_id)

        # Verify only one query was made (to check existence)
        self.assertEqual(mock_run_query.call_count, 1)
        self.assertIn("SELECT id FROM public.alerts", mock_run_query.call_args[0][0])

    @patch("project.alerts_bulk_processor.run_query")
    def test_create_new_alert_only_no_existing_alert(self, mock_run_query):
        """Test create_new_alert_only when no alert exists."""
        bulk_alert = {"id": 1, "user_id": 100, "update_existing_alerts": False}
        power_unit_id = 1001

        # Mock no existing alert, then successful insert
        mock_run_query.side_effect = [
            (None, []),  # No existing alert
            (None, [{"inserted": True}]),  # Insert success
        ]

        self.processor._create_new_alert_only(bulk_alert, power_unit_id)

        # Verify two queries were made
        self.assertEqual(mock_run_query.call_count, 2)
        # First query checks existence
        self.assertIn(
            "SELECT id FROM public.alerts", mock_run_query.call_args_list[0][0][0]
        )
        # Second query inserts
        self.assertIn(
            "INSERT INTO public.alerts", mock_run_query.call_args_list[1][0][0]
        )

    @patch("project.alerts_bulk_processor.logger")
    @patch("project.alerts_bulk_processor.run_query")
    def test_get_matching_power_units_error_handling(self, mock_run_query, mock_logger):
        """Test error handling in _get_matching_power_units method."""
        bulk_alert = {"id": 1, "unit_type_id": 1}

        # Mock a database error
        mock_run_query.side_effect = Exception("Database connection failed")

        # Should return empty list and log error
        result = self.processor._get_matching_power_units(bulk_alert)

        self.assertEqual(result, [])
        mock_logger.error.assert_called_once()

    @patch("project.alerts_bulk_processor.AlertBulkProcessor")
    def test_main_function(self, mock_processor_class):
        """Test the main entry point function."""
        # Mock processor instance and results
        mock_processor = MagicMock()
        mock_processor.process_all_bulk_alerts.return_value = {
            "bulk_subscriptions_processed": 5,
            "power_units_found": 10,
            "alerts_inserted": 8,
            "alerts_updated": 2,
            "errors": 0,
        }
        mock_processor_class.return_value = mock_processor

        # Call main
        config = Config()
        result = main(config)

        # Verify processor was created and called
        mock_processor_class.assert_called_once_with(config)
        mock_processor.process_all_bulk_alerts.assert_called_once()

        # Verify result
        self.assertEqual(result["bulk_subscriptions_processed"], 5)
        self.assertEqual(result["alerts_inserted"], 8)

    @patch("project.alerts_bulk_processor.run_query")
    def test_sql_injection_prevention(self, mock_run_query):
        """Test that SQL injection is prevented through parameterized queries."""
        # Create bulk alert with potentially malicious data
        bulk_alert = {
            "id": 1,
            "user_id": 100,
            "unit_type_id": "1; DROP TABLE alerts; --",
            "model_type_id": None,
            "customer_id": None,
        }

        mock_run_query.return_value = (None, [])

        self.processor._get_matching_power_units(bulk_alert)

        # Verify parameterized query was used
        call_args = mock_run_query.call_args
        params = call_args[1]["data"]

        # The malicious string should be in params, not in the SQL
        self.assertIn("1; DROP TABLE alerts; --", params)
        # SQL should use placeholders
        self.assertIn("%s", call_args[0][0])

    @patch("project.alerts_bulk_processor.run_query")
    def test_batch_upsert_alerts(self, mock_run_query):
        """Test batch upserting multiple alerts in a single query."""
        bulk_alert = {
            "id": 1,
            "user_id": 100,
            "wants_sms": True,
            "wants_email": False,
            "heartbeat": True,
            "change_suction": True,
            "change_dgp": True,
            "change_hp_delta": True,
        }
        power_unit_ids = [1001, 1002, 1003, 1004, 1005]

        # Mock batch insert/update result
        mock_run_query.return_value = (
            None,
            [
                {"power_unit_id": 1001, "inserted": True},
                {"power_unit_id": 1002, "inserted": True},
                {"power_unit_id": 1003, "inserted": False},
                {"power_unit_id": 1004, "inserted": False},
                {"power_unit_id": 1005, "inserted": True},
            ],
        )

        # Reset stats
        self.processor.stats = {
            "bulk_subscriptions_processed": 0,
            "power_units_found": 0,
            "alerts_inserted": 0,
            "alerts_updated": 0,
            "errors": 0,
        }

        self.processor._batch_upsert_alerts(bulk_alert, power_unit_ids)

        # Verify stats updated correctly
        self.assertEqual(self.processor.stats["alerts_inserted"], 3)
        self.assertEqual(self.processor.stats["alerts_updated"], 2)

        # Verify batch query was called once
        self.assertEqual(mock_run_query.call_count, 1)
        call_args = mock_run_query.call_args

        # Check that it's a batch insert with multiple values
        query = call_args[0][0]
        self.assertIn("INSERT INTO public.alerts", query)
        self.assertIn("VALUES", query)
        self.assertIn("ON CONFLICT", query)

        # Check that all power units are in the parameters
        params = call_args[1]["data"]
        # Should be a dictionary with named parameters
        self.assertIsInstance(params, dict)
        # Each power unit has 33 parameters, so 5 power units = 165 parameters
        self.assertEqual(len(params), 165)
        # Verify some named parameters exist
        self.assertIn("pu0_user_id", params)
        self.assertIn("pu0_power_unit_id", params)
        self.assertIn("pu4_power_unit_id", params)
        self.assertEqual(params["pu0_user_id"], 100)
        self.assertEqual(params["pu0_power_unit_id"], 1001)
        self.assertEqual(params["pu4_power_unit_id"], 1005)

    @patch("project.alerts_bulk_processor.run_query")
    def test_batch_upsert_alerts_large_batch(self, mock_run_query):
        """Test batch upserting handles large batches by splitting them."""
        bulk_alert = {"id": 1, "user_id": 100}
        # Create 600 power unit IDs (should be split into 2 batches)
        power_unit_ids = list(range(1001, 1601))

        # Mock results for two batches
        mock_run_query.side_effect = [
            # First batch (500 units)
            (None, [{"power_unit_id": i, "inserted": True} for i in range(1001, 1501)]),
            # Second batch (100 units)
            (None, [{"power_unit_id": i, "inserted": True} for i in range(1501, 1601)]),
        ]

        self.processor._batch_upsert_alerts(bulk_alert, power_unit_ids)

        # Verify two batch queries were made
        self.assertEqual(mock_run_query.call_count, 2)

    @patch("project.alerts_bulk_processor.run_query")
    def test_process_bulk_alert_no_update_existing(self, mock_run_query):
        """Test processing when update_existing_alerts is False."""
        bulk_alert = {
            "id": 1,
            "user_id": 100,
            "update_existing_alerts": False,
            "unit_type_id": None,
            "model_type_id": None,
            "customer_id": None,
        }

        # Mock matching power units
        all_power_units = [
            {"power_unit_id": 1001},
            {"power_unit_id": 1002},
            {"power_unit_id": 1003},
            {"power_unit_id": 1004},
        ]

        # Mock existing alerts (1001 and 1003 already exist)
        existing_alerts = [
            {"power_unit_id": 1001},
            {"power_unit_id": 1003},
        ]

        # Mock query responses
        mock_run_query.side_effect = [
            (None, all_power_units),  # Get matching power units
            (None, existing_alerts),  # Get existing alerts
            # Batch insert for new power units only (1002 and 1004)
            (
                None,
                [
                    {"power_unit_id": 1002, "inserted": True},
                    {"power_unit_id": 1004, "inserted": True},
                ],
            ),
        ]

        self.processor._process_single_bulk_alert(bulk_alert)

        # Verify the correct queries were made
        self.assertEqual(mock_run_query.call_count, 3)

        # Check the ANY() query for existing alerts
        second_call = mock_run_query.call_args_list[1]
        query = second_call[0][0]
        self.assertIn("power_unit_id = ANY(%s)", query)

        # Verify only new power units were processed
        third_call = mock_run_query.call_args_list[2]
        # Should be a batch insert for 2 power units
        params = third_call[1]["data"]
        # Should be a dictionary with named parameters
        self.assertIsInstance(params, dict)
        # 2 power units * 33 parameters each = 66 parameters
        self.assertEqual(len(params), 66)
        # Verify the correct power units are processed
        self.assertIn("pu0_power_unit_id", params)
        self.assertIn("pu1_power_unit_id", params)
        self.assertEqual(params["pu0_power_unit_id"], 1002)
        self.assertEqual(params["pu1_power_unit_id"], 1004)

    @patch("project.alerts_bulk_processor.run_query")
    def test_get_matching_power_units_with_update_existing_false(self, mock_run_query):
        """Test that NOT IN subquery is added when update_existing_alerts is False."""
        bulk_alert = {
            "id": 1,
            "user_id": 100,
            "unit_type_id": None,
            "model_type_id": None,
            "customer_id": None,
            "update_existing_alerts": False,
        }

        mock_run_query.return_value = (None, [])

        self.processor._get_matching_power_units(bulk_alert)

        # Verify the query includes NOT IN subquery
        call_args = mock_run_query.call_args
        query = call_args[0][0]

        self.assertIn(
            "t1.power_unit_id NOT IN (SELECT power_unit_id FROM public.alerts WHERE user_id = 100)",
            query,
        )


if __name__ == "__main__":
    unittest.main()
