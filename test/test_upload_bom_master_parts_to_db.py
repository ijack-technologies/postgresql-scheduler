import sys
import unittest
from unittest.mock import Mock, patch

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

import psycopg2

from project.upload_bom_master_parts_to_db import (
    clean_part_number,
    initialize_parts_in_warehouses,
    consolidate_inventory_to_latest_revisions,
)


class TestCleanPartNumber(unittest.TestCase):
    def test_integers(self):
        self.assertEqual(clean_part_number(123), "123")
        self.assertEqual(clean_part_number(0), "0")

    def test_floats(self):
        self.assertEqual(clean_part_number(123.0), "123")
        self.assertEqual(clean_part_number(123.45), "123.45")

    def test_scientific(self):
        self.assertEqual(clean_part_number(1.2e5), "120000")
        self.assertEqual(clean_part_number("1.2e5"), "1.2e5")

    def test_strings(self):
        self.assertEqual(clean_part_number("070-0470"), "070-0470")
        self.assertEqual(clean_part_number("430-1418r0"), "430-1418r0")
        self.assertEqual(clean_part_number("123"), "123")
        self.assertEqual(clean_part_number("007"), "007")
        self.assertEqual(clean_part_number(" 123 "), "123")
        self.assertEqual(clean_part_number(""), "")


class TestInitializePartsInWarehouses(unittest.TestCase):
    """Test cases for initialize_parts_in_warehouses function"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_conn = Mock()
        self.mock_cursor = Mock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(
            return_value=self.mock_cursor
        )
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)

    @patch("project.upload_bom_master_parts_to_db.logger")
    @patch("project.upload_bom_master_parts_to_db.execute_values")
    def test_initialize_parts_no_warehouses(self, mock_execute_values, mock_logger):
        """Test when no active warehouses exist"""
        # Setup
        self.mock_cursor.fetchall.side_effect = [
            [],  # No active warehouses
            [(1, "PART-001"), (2, "PART-002")],  # Active parts
            [],  # No warehouse summary (since no warehouses)
        ]
        self.mock_cursor.fetchone.side_effect = [(0,), (0,)]  # Counts must be tuples

        # Execute
        initialize_parts_in_warehouses(self.mock_conn)

        # Assert
        mock_logger.info.assert_any_call("Found 0 active warehouses")
        mock_logger.info.assert_any_call("Found 2 active parts")
        mock_execute_values.assert_not_called()  # No inserts should happen
        self.mock_conn.commit.assert_called_once()

    @patch("project.upload_bom_master_parts_to_db.logger")
    @patch("project.upload_bom_master_parts_to_db.execute_values")
    def test_initialize_parts_no_parts(self, mock_execute_values, mock_logger):
        """Test when no active parts exist"""
        # Setup
        self.mock_cursor.fetchall.side_effect = [
            [(1, "Warehouse 1"), (2, "Warehouse 2")],  # Active warehouses
            [],  # No active parts
            [],  # Empty warehouse summary (no parts to count)
        ]
        self.mock_cursor.fetchone.side_effect = [(0,), (0,)]  # Counts must be tuples

        # Execute
        initialize_parts_in_warehouses(self.mock_conn)

        # Assert
        mock_logger.info.assert_any_call("Found 2 active warehouses")
        mock_logger.info.assert_any_call("Found 0 active parts")
        mock_execute_values.assert_not_called()  # No inserts should happen
        self.mock_conn.commit.assert_called_once()

    @patch("project.upload_bom_master_parts_to_db.logger")
    @patch("project.upload_bom_master_parts_to_db.execute_values")
    def test_initialize_parts_successful(self, mock_execute_values, mock_logger):
        """Test successful initialization of parts in warehouses"""
        # Setup
        self.mock_cursor.fetchall.side_effect = [
            [(1, "Warehouse 1"), (2, "Warehouse 2")],  # Active warehouses
            [(10, "PART-001"), (11, "PART-002")],  # Active parts
            [("Warehouse 1", 2), ("Warehouse 2", 2)],  # Warehouse summary
        ]
        self.mock_cursor.fetchone.side_effect = [
            (2,),  # Existing count before
            (6,),  # Final count after
        ]

        # Execute
        initialize_parts_in_warehouses(self.mock_conn)

        # Assert
        mock_logger.info.assert_any_call("Found 2 active warehouses")
        mock_logger.info.assert_any_call("Found 2 active parts")
        mock_logger.info.assert_any_call(
            "Total possible warehouse-part combinations: 4"
        )
        mock_logger.info.assert_any_call("Existing warehouse-part relationships: 2")
        mock_logger.info.assert_any_call(
            "Successfully created 4 new warehouse-part relationships"
        )

        # Verify execute_values was called
        mock_execute_values.assert_called()
        args = mock_execute_values.call_args[0]
        self.assertEqual(len(args[2]), 4)  # 2 warehouses × 2 parts = 4 combinations

        self.mock_conn.commit.assert_called_once()

    @patch("project.upload_bom_master_parts_to_db.logger")
    @patch("project.upload_bom_master_parts_to_db.execute_values")
    def test_initialize_parts_batch_processing(self, mock_execute_values, mock_logger):
        """Test batch processing for large datasets"""
        # Setup with many parts to trigger batching
        warehouses = [(1, "Warehouse 1")]
        parts = [(i, f"PART-{i:04d}") for i in range(2000)]  # 2000 parts

        self.mock_cursor.fetchall.side_effect = [
            warehouses,
            parts,
            [("Warehouse 1", 2000)],  # Warehouse summary
        ]
        self.mock_cursor.fetchone.side_effect = [(0,), (2000,)]

        # Execute
        initialize_parts_in_warehouses(self.mock_conn)

        # Assert
        # Should be called twice: once for first 1000, once for remaining 1000
        self.assertEqual(mock_execute_values.call_count, 2)

        # Check batch sizes
        first_batch = mock_execute_values.call_args_list[0][0][2]
        second_batch = mock_execute_values.call_args_list[1][0][2]
        self.assertEqual(len(first_batch), 1000)
        self.assertEqual(len(second_batch), 1000)

    @patch("project.upload_bom_master_parts_to_db.logger")
    @patch("project.upload_bom_master_parts_to_db.execute_values")
    def test_initialize_parts_all_exist(self, mock_execute_values, mock_logger):
        """Test when all warehouse-part relationships already exist"""
        # Setup
        self.mock_cursor.fetchall.side_effect = [
            [(1, "Warehouse 1")],
            [(10, "PART-001")],
            [("Warehouse 1", 1)],  # Warehouse summary
        ]
        self.mock_cursor.fetchone.side_effect = [
            (1,),  # All relationships exist
            (1,),  # No new ones created
        ]

        # Execute
        initialize_parts_in_warehouses(self.mock_conn)

        # Assert
        mock_logger.info.assert_any_call(
            "Successfully created 0 new warehouse-part relationships"
        )
        mock_execute_values.assert_called_once()  # Still called but ON CONFLICT handles it


class TestConsolidateInventoryToLatestRevisions(unittest.TestCase):
    """Test cases for consolidate_inventory_to_latest_revisions function"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_conn = Mock()
        self.mock_cursor = Mock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(
            return_value=self.mock_cursor
        )
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)

    @patch("project.upload_bom_master_parts_to_db.logger")
    def test_consolidate_no_multi_revision_parts(self, mock_logger):
        """Test when no parts have multiple revisions"""
        # Setup
        self.mock_cursor.fetchall.return_value = []  # No parts with multiple revisions

        # Execute
        consolidate_inventory_to_latest_revisions(self.mock_conn)

        # Assert
        mock_logger.info.assert_any_call(
            "Found 0 part families with multiple revisions"
        )
        mock_logger.info.assert_any_call(
            "No parts need consolidation - all parts have single revisions"
        )
        self.mock_conn.commit.assert_not_called()  # No changes to commit

    @patch("project.upload_bom_master_parts_to_db.logger")
    def test_consolidate_single_part_family(self, mock_logger):
        """Test consolidating a single part family with 2 revisions"""
        # Setup
        self.mock_cursor.fetchall.side_effect = [
            # Parts with multiple revisions
            [("PART-ABC", 2, 1.0, 101, "PART-ABC r1")],
            # Older parts
            [(100, "PART-ABC r0", 0.0)],
            # Warehouses
            [(1, "Warehouse 1"), (2, "Warehouse 2")],
        ]

        self.mock_cursor.fetchone.side_effect = [
            # Warehouse 1: warehouse config from previous latest
            (10.0, 100.0, 25.0, 50.0, 5.0, 7, 3.5, 30),  # warehouse management fields
            # Warehouse 1 quantities from old revision
            (5.0, 1.0, 2.0),  # 5 actual, 1 reserved, 2 desired
            (3.0, 0.5, 1.0),  # Current quantities for latest revision
            # Warehouse 2: warehouse config from previous latest
            (15.0, 150.0, 30.0, 60.0, 8.0, 10, 5.0, 30),  # warehouse management fields
            # Warehouse 2 quantities from old revision
            (3.0, 0.0, 1.0),  # 3 actual, 0 reserved, 1 desired
            None,  # No existing record for latest revision
            # Verification queries
            (11.0, 1.5, 4.0),  # Total after consolidation
        ]

        # Execute
        consolidate_inventory_to_latest_revisions(self.mock_conn)

        # Assert
        mock_logger.info.assert_any_call(
            "Found 1 part families with multiple revisions"
        )
        mock_logger.info.assert_any_call(
            "\nProcessing PART-ABC (2 revisions, latest: r1)"
        )
        mock_logger.info.assert_any_call(
            "  Warehouse 1: Transferring 5.0 actual, 1.0 reserved, 2.0 desired"
        )
        mock_logger.info.assert_any_call(
            "    Copying warehouse config: min=10.0, max=100.0, reorder_point=25.0, reorder_qty=50.0"
        )
        mock_logger.info.assert_any_call(
            "  Warehouse 2: Transferring 3.0 actual, 0.0 reserved, 1.0 desired"
        )
        mock_logger.info.assert_any_call(
            "    Copying warehouse config: min=15.0, max=150.0, reorder_point=30.0, reorder_qty=60.0"
        )

        # Verify UPDATE for existing record
        update_calls = [
            call
            for call in self.mock_cursor.execute.call_args_list
            if "UPDATE public.warehouses_parts_rel" in str(call)
            and "quantity = %s" in str(call)
        ]
        self.assertEqual(len(update_calls), 1)
        # Check that warehouse config fields are included
        self.assertIn(
            "warehouse_min_stock = COALESCE(%s, warehouse_min_stock)",
            update_calls[0][0][0],
        )
        # Verify parameters: quantities + 8 warehouse fields + warehouse_id + part_id
        self.assertEqual(len(update_calls[0][0][1]), 13)
        # First 3 are quantities
        self.assertEqual(
            update_calls[0][0][1][:3], (8.0, 1.5, 3.0)
        )  # 5+3=8, 1+0.5=1.5, 2+1=3
        # Next 8 are warehouse management fields
        self.assertEqual(
            update_calls[0][0][1][3:11], (10.0, 100.0, 25.0, 50.0, 5.0, 7, 3.5, 30)
        )
        # Last 2 are IDs
        self.assertEqual(update_calls[0][0][1][11:], (1, 101))

        # Verify INSERT for new record
        insert_calls = [
            call
            for call in self.mock_cursor.execute.call_args_list
            if "INSERT INTO public.warehouses_parts_rel" in str(call)
        ]
        self.assertEqual(len(insert_calls), 1)
        # Check that warehouse config fields are included
        self.assertIn("warehouse_min_stock", insert_calls[0][0][0])
        # Verify parameters for INSERT (more fields now)
        insert_params = insert_calls[0][0][1]
        self.assertEqual(
            insert_params[:5], (2, 101, 3.0, 0.0, 1.0)
        )  # warehouse_id, part_id, quantities
        self.assertEqual(
            insert_params[5:13], (15.0, 150.0, 30.0, 60.0, 8.0, 10, 5.0, 30)
        )  # warehouse fields

        # Verify old revisions set to zero
        zero_update_calls = [
            call
            for call in self.mock_cursor.execute.call_args_list
            if "SET quantity = 0" in str(call)
        ]
        self.assertEqual(len(zero_update_calls), 2)  # One for each warehouse

        self.mock_conn.commit.assert_called_once()

    @patch("project.upload_bom_master_parts_to_db.logger")
    def test_consolidate_multiple_part_families(self, mock_logger):
        """Test consolidating multiple part families"""
        # Setup
        self.mock_cursor.fetchall.side_effect = [
            # Parts with multiple revisions
            [
                ("PART-ABC", 2, 1.0, 101, "PART-ABC r1"),
                ("PART-XYZ", 3, 2.0, 202, "PART-XYZ r2"),
            ],
            # Older parts for PART-ABC
            [(100, "PART-ABC r0", 0.0)],
            [(1, "Warehouse 1")],  # Warehouses for PART-ABC
            # Older parts for PART-XYZ
            [(200, "PART-XYZ r0", 0.0), (201, "PART-XYZ r1", 1.0)],
            [(1, "Warehouse 1")],  # Warehouses for PART-XYZ
        ]

        self.mock_cursor.fetchone.side_effect = [
            # PART-ABC transfers
            (10.0, 100.0, 25.0, 50.0, 5.0, 7, 3.5, 30),  # warehouse config
            (5.0, 1.0, 2.0),  # Quantities from old revision
            None,  # No existing record for latest revision
            # PART-XYZ transfers
            (20.0, 200.0, 50.0, 100.0, 10.0, 14, 7.0, 60),  # warehouse config
            (10.0, 2.0, 5.0),  # Sum of quantities from both old revisions
            None,  # No existing record for latest revision
            # Verification
            (5.0, 1.0, 2.0),  # PART-ABC total
            (10.0, 2.0, 5.0),  # PART-XYZ total
        ]

        # Execute
        consolidate_inventory_to_latest_revisions(self.mock_conn)

        # Assert
        mock_logger.info.assert_any_call(
            "Found 2 part families with multiple revisions"
        )
        mock_logger.info.assert_any_call("Part families processed: 2")
        mock_logger.info.assert_any_call("Warehouse transfers completed: 2")
        mock_logger.info.assert_any_call("Total quantity transferred: 15.0")
        mock_logger.info.assert_any_call("Total quantity reserved transferred: 3.0")
        mock_logger.info.assert_any_call("Total desired quantity transferred: 7.0")

        self.mock_conn.commit.assert_called_once()

    @patch("project.upload_bom_master_parts_to_db.logger")
    def test_consolidate_no_inventory_to_transfer(self, mock_logger):
        """Test when older revisions have no inventory"""
        # Setup
        self.mock_cursor.fetchall.side_effect = [
            [("PART-ABC", 2, 1.0, 101, "PART-ABC r1")],
            [(100, "PART-ABC r0", 0.0)],
            [(1, "Warehouse 1")],
        ]

        self.mock_cursor.fetchone.side_effect = [
            None,  # No warehouse config from previous revision
            (0.0, 0.0, 0.0),  # No inventory in old revision
            (0.0, 0.0, 0.0),  # Verification
        ]

        # Execute
        consolidate_inventory_to_latest_revisions(self.mock_conn)

        # Assert
        # Should not perform any transfers
        update_calls = [
            call
            for call in self.mock_cursor.execute.call_args_list
            if "UPDATE public.warehouses_parts_rel" in str(call)
            and "quantity = %s" in str(call)
        ]
        insert_calls = [
            call
            for call in self.mock_cursor.execute.call_args_list
            if "INSERT INTO public.warehouses_parts_rel" in str(call)
        ]
        self.assertEqual(len(update_calls), 0)
        self.assertEqual(len(insert_calls), 0)

        mock_logger.info.assert_any_call("Total quantity transferred: 0")

    @patch("project.upload_bom_master_parts_to_db.logger")
    def test_consolidate_config_only_no_inventory(self, mock_logger):
        """Test copying warehouse config when there's no inventory to transfer"""
        # Setup
        self.mock_cursor.fetchall.side_effect = [
            [(("PART-ABC", 2, 1.0, 101, "PART-ABC r1"))],
            [(100, "PART-ABC r0", 0.0)],
            [(1, "Warehouse 1")],
        ]

        self.mock_cursor.fetchone.side_effect = [
            (10.0, 100.0, 25.0, 50.0, 5.0, 7, 3.5, 30),  # warehouse config exists
            (0.0, 0.0, 0.0),  # No inventory in old revision
            None,  # No existing record for latest revision
            (0.0, 0.0, 0.0),  # Verification
        ]

        # Execute
        consolidate_inventory_to_latest_revisions(self.mock_conn)

        # Assert
        # Should still perform INSERT with warehouse config even though no inventory
        insert_calls = [
            call
            for call in self.mock_cursor.execute.call_args_list
            if "INSERT INTO public.warehouses_parts_rel" in str(call)
        ]
        self.assertEqual(len(insert_calls), 1)
        # Verify warehouse config is included
        insert_params = insert_calls[0][0][1]
        self.assertEqual(
            insert_params[5:13], (10.0, 100.0, 25.0, 50.0, 5.0, 7, 3.5, 30)
        )

    @patch("project.upload_bom_master_parts_to_db.logger")
    def test_consolidate_preserves_total_quantities(self, mock_logger):
        """Test that consolidation preserves total quantities"""
        # Setup
        self.mock_cursor.fetchall.side_effect = [
            [("PART-ABC", 2, 1.0, 101, "PART-ABC r1")],
            [(100, "PART-ABC r0", 0.0)],
            [(1, "Warehouse 1")],
        ]

        original_total = 10.0
        original_reserved = 2.0
        original_desired = 5.0

        self.mock_cursor.fetchone.side_effect = [
            (10.0, 100.0, 25.0, 50.0, 5.0, 7, 3.5, 30),  # warehouse config
            (
                original_total,
                original_reserved,
                original_desired,
            ),  # Quantities from old revision
            None,  # No existing record for latest revision
            (
                original_total,
                original_reserved,
                original_desired,
            ),  # Verification - should match original
        ]

        # Execute
        consolidate_inventory_to_latest_revisions(self.mock_conn)

        # Assert
        mock_logger.info.assert_any_call(
            f"PART-ABC: Total quantity: {original_total}, Total reserved: {original_reserved}, Total desired: {original_desired}"
        )


class TestIntegrationScenarios(unittest.TestCase):
    """Integration tests for combined scenarios"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_conn = Mock()
        self.mock_cursor = Mock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(
            return_value=self.mock_cursor
        )
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)

    @patch("project.upload_bom_master_parts_to_db.logger")
    @patch("project.upload_bom_master_parts_to_db.execute_values")
    def test_consolidate_then_initialize_workflow(
        self, mock_execute_values, mock_logger
    ):
        """Test the complete workflow: consolidate then initialize"""
        # This tests that consolidation happens before initialization
        # and that initialization creates records for the latest revisions

        # Setup for consolidation
        self.mock_cursor.fetchall.side_effect = [
            # Consolidation: parts with multiple revisions
            [("PART-ABC", 2, 1.0, 101, "PART-ABC r1")],
            [(100, "PART-ABC r0", 0.0)],
            [(1, "Warehouse 1")],
            # Initialization: active warehouses and parts
            [(1, "Warehouse 1"), (2, "Warehouse 2")],
            [(101, "PART-ABC r1"), (102, "PART-XYZ")],  # Latest revision is included
            [("Warehouse 1", 2), ("Warehouse 2", 2)],  # Summary
        ]

        self.mock_cursor.fetchone.side_effect = [
            # Consolidation
            (10.0, 100.0, 25.0, 50.0, 5.0, 7, 3.5, 30),  # warehouse config
            (5.0, 1.0, 2.0),  # Transfer quantities
            None,  # No existing record
            (5.0, 1.0, 2.0),  # Verification
            # Initialization
            (1,),  # Existing count (just the consolidated record)
            (4,),  # Final count
        ]

        # Execute workflow
        consolidate_inventory_to_latest_revisions(self.mock_conn)
        initialize_parts_in_warehouses(self.mock_conn)

        # Assert consolidation happened
        insert_calls = [
            call
            for call in self.mock_cursor.execute.call_args_list
            if "INSERT INTO public.warehouses_parts_rel" in str(call)
        ]
        # At least one INSERT should have happened
        self.assertGreaterEqual(len(insert_calls), 1)
        # Check that the INSERT includes warehouse config fields
        consolidated_insert = None
        for call in insert_calls:
            if "warehouse_min_stock" in str(call):
                consolidated_insert = call
                break
        self.assertIsNotNone(consolidated_insert)

        # Assert initialization happened and includes latest revision
        self.assertEqual(mock_execute_values.call_count, 1)
        values = mock_execute_values.call_args[0][2]
        self.assertEqual(len(values), 4)  # 2 warehouses × 2 parts


class TestErrorHandling(unittest.TestCase):
    """Test error handling scenarios"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_conn = Mock()
        self.mock_cursor = Mock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(
            return_value=self.mock_cursor
        )
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)

    @patch("project.upload_bom_master_parts_to_db.logger")
    @patch("project.upload_bom_master_parts_to_db.execute_values")
    def test_database_error_during_initialization(
        self, mock_execute_values, mock_logger
    ):
        """Test handling of database errors during initialization"""
        # Setup
        self.mock_cursor.fetchall.side_effect = psycopg2.DatabaseError(
            "Connection lost"
        )

        # Execute and assert exception is raised
        with self.assertRaises(psycopg2.DatabaseError):
            initialize_parts_in_warehouses(self.mock_conn)

        # Ensure no commit happened
        self.mock_conn.commit.assert_not_called()

    @patch("project.upload_bom_master_parts_to_db.logger")
    def test_database_error_during_consolidation(self, mock_logger):
        """Test handling of database errors during consolidation"""
        # Setup
        self.mock_cursor.fetchall.side_effect = [
            [("PART-ABC", 2, 1.0, 101, "PART-ABC r1")],
            psycopg2.DatabaseError("Connection lost"),
        ]

        # Execute and assert exception is raised
        with self.assertRaises(psycopg2.DatabaseError):
            consolidate_inventory_to_latest_revisions(self.mock_conn)

        # Ensure no commit happened
        self.mock_conn.commit.assert_not_called()


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_conn = Mock()
        self.mock_cursor = Mock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(
            return_value=self.mock_cursor
        )
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)

    @patch("project.upload_bom_master_parts_to_db.logger")
    def test_consolidate_with_decimal_quantities(self, mock_logger):
        """Test consolidation with decimal quantities"""
        # Setup with decimal quantities
        self.mock_cursor.fetchall.side_effect = [
            [("PART-ABC", 2, 1.0, 101, "PART-ABC r1")],
            [(100, "PART-ABC r0", 0.0)],
            [(1, "Warehouse 1")],
        ]

        self.mock_cursor.fetchone.side_effect = [
            (
                12.5,
                125.5,
                30.25,
                60.5,
                6.25,
                8,
                4.25,
                45,
            ),  # warehouse config with decimals
            (5.25, 1.25, 2.75),  # Decimal quantities
            (1.5, 0.25, 0.5),  # Existing quantities
            (6.75, 1.5, 3.25),  # Verification
        ]

        # Execute
        consolidate_inventory_to_latest_revisions(self.mock_conn)

        # Assert proper handling of decimals
        update_calls = [
            call
            for call in self.mock_cursor.execute.call_args_list
            if "UPDATE public.warehouses_parts_rel" in str(call)
            and "quantity = %s" in str(call)
        ]
        # Check that quantities and warehouse config are handled correctly
        params = update_calls[0][0][1]
        self.assertEqual(
            params[:3], (6.75, 1.5, 3.25)
        )  # Summed quantities with decimals
        self.assertEqual(
            params[3:11], (12.5, 125.5, 30.25, 60.5, 6.25, 8, 4.25, 45)
        )  # Warehouse config with decimals

    @patch("project.upload_bom_master_parts_to_db.logger")
    @patch("project.upload_bom_master_parts_to_db.execute_values")
    def test_initialize_with_very_large_dataset(self, mock_execute_values, mock_logger):
        """Test initialization with very large dataset"""
        # Setup with 10k parts and 10 warehouses = 100k relationships
        warehouses = [(i, f"Warehouse {i}") for i in range(1, 11)]
        parts = [(i, f"PART-{i:06d}") for i in range(1, 10001)]

        self.mock_cursor.fetchall.side_effect = [
            warehouses,
            parts,
            [(f"Warehouse {i}", 10000) for i in range(1, 11)],  # Summary
        ]
        self.mock_cursor.fetchone.side_effect = [(0,), (100000,)]

        # Execute
        initialize_parts_in_warehouses(self.mock_conn)

        # Assert batching occurred
        self.assertEqual(
            mock_execute_values.call_count, 100
        )  # 100k / 1000 = 100 batches
        mock_logger.info.assert_any_call("Processed 10000 records...")
        mock_logger.info.assert_any_call(
            "Successfully created 100000 new warehouse-part relationships"
        )


if __name__ == "__main__":
    unittest.main()
