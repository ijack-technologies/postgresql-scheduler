"""
Test-driven development for the time_series_mv_refresh.py ffill/bfill optimization.

This test file compares the OLD loop-based method vs the NEW column-by-column method
to ensure they produce identical results before deploying to production.

The OLD method (O(n²)):
    for power_unit, group in df.groupby("power_unit"):
        sorted_group = group.sort_values(...).ffill().bfill()
        df.loc[df["power_unit"] == power_unit, :] = sorted_group

The NEW method (O(n), memory-safe):
    df = df.sort_values(['power_unit', 'timestamp_utc'])
    grouped = df.groupby('power_unit', sort=False)
    for col in fill_columns:
        df[col] = grouped[col].ffill()
        df[col] = grouped[col].bfill()

Column-by-column processing keeps peak memory low (~1.9 MB intermediate per column
instead of ~238 MB for all columns at once) while maintaining O(n) time complexity.
"""

import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest


def create_test_dataframe(
    n_power_units: int = 5,
    n_timestamps_per_unit: int = 100,
    nan_probability: float = 0.3,
    seed: int = 42,
) -> pd.DataFrame:
    """Create a test DataFrame that mimics the time_series table structure.

    Args:
        n_power_units: Number of distinct power units
        n_timestamps_per_unit: Number of timestamps per power unit
        nan_probability: Probability of a value being NaN (to simulate missing data)
        seed: Random seed for reproducibility

    Returns:
        DataFrame with structure similar to time_series table
    """
    np.random.seed(seed)

    power_units = [f"20040{i}" for i in range(n_power_units)]
    gateways = [f"00:60:E0:72:66:{i:02d}" for i in range(n_power_units)]

    # Create base timestamp range
    base_time = datetime(2024, 1, 1, 0, 0, 0)

    rows = []
    for pu_idx, power_unit in enumerate(power_units):
        gateway = gateways[pu_idx]
        for ts_idx in range(n_timestamps_per_unit):
            timestamp = base_time + timedelta(minutes=ts_idx * 10)

            # Create row with some NaN values (simulating missing sensor data)
            row = {
                "timestamp_utc": timestamp,
                "power_unit": power_unit,
                "gateway": gateway,
                "spm": np.random.uniform(0, 10)
                if np.random.random() > nan_probability
                else np.nan,
                "cgp": np.random.uniform(0, 500)
                if np.random.random() > nan_probability
                else np.nan,
                "dgp": np.random.uniform(0, 500)
                if np.random.random() > nan_probability
                else np.nan,
                "dtp": np.random.uniform(50, 200)
                if np.random.random() > nan_probability
                else np.nan,
                "hpu": np.random.uniform(0, 100)
                if np.random.random() > nan_probability
                else np.nan,
                "ht": np.random.uniform(0, 100)
                if np.random.random() > nan_probability
                else np.nan,
                "signal": np.random.randint(-100, 0)
                if np.random.random() > nan_probability
                else np.nan,
                # Boolean columns
                "hyd": np.random.choice([True, False])
                if np.random.random() > nan_probability
                else np.nan,
                "mtr": np.random.choice([True, False])
                if np.random.random() > nan_probability
                else np.nan,
            }
            rows.append(row)

    df = pd.DataFrame(rows)

    # Shuffle to simulate out-of-order data arrival (important for testing sort!)
    df = df.sample(frac=1, random_state=seed).reset_index(drop=True)

    return df


def old_method_loop_based(df: pd.DataFrame) -> pd.DataFrame:
    """
    The OLD O(n²) loop-based implementation from time_series_mv_refresh.py (lines 284-302).

    This is the exact code from the original file.
    """
    df = df.copy()

    pd.options.mode.copy_on_write = True
    pd.set_option("future.no_silent_downcasting", True)

    for power_unit, group in df.groupby("power_unit"):
        # Sort by timestamp_utc, then fill in missing values
        sorted_group = (
            group.sort_values("timestamp_utc", ascending=True)
            .infer_objects()
            .ffill()
            .bfill()
        )
        # Replace the original group with the sorted and filled group
        df.loc[df["power_unit"] == power_unit, :] = sorted_group

    # Final sort to match expected output order
    df = df.sort_values(["power_unit", "timestamp_utc"]).reset_index(drop=True)

    return df


def new_method_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """
    The NEW O(n) column-by-column implementation.

    Processes one column at a time to keep memory usage low while
    maintaining O(n) time complexity. This matches production code in
    time_series_mv_refresh.py.
    """
    df = df.copy()

    # Step 1: Sort by power_unit and timestamp (enables proper ffill/bfill within groups)
    df = df.sort_values(["power_unit", "timestamp_utc"])

    # Step 2: Identify columns to fill (all except identifiers and timestamps)
    exclude_cols = ["timestamp_utc", "timestamp_utc_inserted", "power_unit", "gateway"]
    fill_columns = [col for col in df.columns if col not in exclude_cols]

    # Step 3: Column-by-column ffill/bfill to control memory usage
    grouped = df.groupby("power_unit", sort=False)
    for col in fill_columns:
        df[col] = grouped[col].ffill()
        df[col] = grouped[col].bfill()

    # Reset index to match old method
    df = df.reset_index(drop=True)

    return df


class TestFfillBfillOptimization:
    """Tests to verify the new vectorized method produces identical results to the old loop method."""

    def test_small_dataset_identical_results(self):
        """Test that both methods produce identical results on a small dataset."""
        df = create_test_dataframe(n_power_units=3, n_timestamps_per_unit=20)

        result_old = old_method_loop_based(df)
        result_new = new_method_vectorized(df)

        # Compare the DataFrames
        pd.testing.assert_frame_equal(
            result_old,
            result_new,
            check_dtype=False,  # Allow minor dtype differences
            check_exact=False,  # Allow floating point tolerance
            rtol=1e-5,
        )

    def test_medium_dataset_identical_results(self):
        """Test with a medium-sized dataset (more realistic)."""
        df = create_test_dataframe(n_power_units=10, n_timestamps_per_unit=500)

        result_old = old_method_loop_based(df)
        result_new = new_method_vectorized(df)

        pd.testing.assert_frame_equal(
            result_old,
            result_new,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )

    def test_all_nan_column(self):
        """Test handling of a column that's entirely NaN for a power unit."""
        df = create_test_dataframe(
            n_power_units=2, n_timestamps_per_unit=10, nan_probability=0.0
        )

        # Make one entire column NaN for one power unit
        mask = df["power_unit"] == df["power_unit"].unique()[0]
        df.loc[mask, "spm"] = np.nan

        result_old = old_method_loop_based(df)
        result_new = new_method_vectorized(df)

        pd.testing.assert_frame_equal(
            result_old,
            result_new,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )

    def test_no_nan_values(self):
        """Test with no NaN values (edge case)."""
        df = create_test_dataframe(
            n_power_units=3, n_timestamps_per_unit=20, nan_probability=0.0
        )

        result_old = old_method_loop_based(df)
        result_new = new_method_vectorized(df)

        pd.testing.assert_frame_equal(
            result_old,
            result_new,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )

    def test_high_nan_probability(self):
        """Test with very sparse data (many NaN values)."""
        df = create_test_dataframe(
            n_power_units=3, n_timestamps_per_unit=50, nan_probability=0.8
        )

        result_old = old_method_loop_based(df)
        result_new = new_method_vectorized(df)

        pd.testing.assert_frame_equal(
            result_old,
            result_new,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )

    def test_single_power_unit(self):
        """Test with only one power unit."""
        df = create_test_dataframe(n_power_units=1, n_timestamps_per_unit=100)

        result_old = old_method_loop_based(df)
        result_new = new_method_vectorized(df)

        pd.testing.assert_frame_equal(
            result_old,
            result_new,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )

    def test_single_timestamp_per_unit(self):
        """Test with only one timestamp per power unit."""
        df = create_test_dataframe(n_power_units=5, n_timestamps_per_unit=1)

        result_old = old_method_loop_based(df)
        result_new = new_method_vectorized(df)

        pd.testing.assert_frame_equal(
            result_old,
            result_new,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )

    def test_ffill_propagation_order(self):
        """Test that ffill correctly propagates values forward in time order."""
        # Create a simple DataFrame where we know exactly what the result should be
        df = pd.DataFrame(
            {
                "timestamp_utc": [
                    datetime(2024, 1, 1, 0, 0),
                    datetime(2024, 1, 1, 0, 10),
                    datetime(2024, 1, 1, 0, 20),
                    datetime(2024, 1, 1, 0, 30),
                ],
                "power_unit": ["A", "A", "A", "A"],
                "gateway": ["GW1", "GW1", "GW1", "GW1"],
                "spm": [1.0, np.nan, np.nan, 2.0],  # Should become [1.0, 1.0, 1.0, 2.0]
            }
        )

        result_old = old_method_loop_based(df)
        result_new = new_method_vectorized(df)

        # Both should have ffilled the NaN values
        assert result_new["spm"].tolist() == [1.0, 1.0, 1.0, 2.0]
        pd.testing.assert_frame_equal(result_old, result_new, check_dtype=False)

    def test_bfill_propagation(self):
        """Test that bfill correctly fills initial NaN values."""
        df = pd.DataFrame(
            {
                "timestamp_utc": [
                    datetime(2024, 1, 1, 0, 0),
                    datetime(2024, 1, 1, 0, 10),
                    datetime(2024, 1, 1, 0, 20),
                ],
                "power_unit": ["A", "A", "A"],
                "gateway": ["GW1", "GW1", "GW1"],
                "spm": [
                    np.nan,
                    np.nan,
                    5.0,
                ],  # Should become [5.0, 5.0, 5.0] after bfill
            }
        )

        result_old = old_method_loop_based(df)
        result_new = new_method_vectorized(df)

        # Both should have bfilled the initial NaN values
        assert result_new["spm"].tolist() == [5.0, 5.0, 5.0]
        pd.testing.assert_frame_equal(result_old, result_new, check_dtype=False)


class TestPerformance:
    """Performance benchmarks to verify the optimization provides speedup."""

    @pytest.mark.slow
    def test_performance_improvement(self):
        """Verify that the new method is significantly faster than the old method."""
        # Use a larger dataset to see meaningful performance difference
        df = create_test_dataframe(n_power_units=50, n_timestamps_per_unit=200)

        # Time the old method
        start = time.time()
        old_method_loop_based(df)
        old_time = time.time() - start

        # Time the new method
        start = time.time()
        new_method_vectorized(df)
        new_time = time.time() - start

        print(f"\nOld method time: {old_time:.3f}s")
        print(f"New method time: {new_time:.3f}s")
        print(f"Speedup: {old_time / new_time:.1f}x")

        # The new method should be at least 2x faster
        assert new_time < old_time, "New method should be faster than old method"
        # We expect at least 2x speedup, but don't fail on small speedups
        if old_time > 0.1:  # Only assert speedup if old method took meaningful time
            assert old_time / new_time > 1.5, (
                f"Expected at least 1.5x speedup, got {old_time / new_time:.1f}x"
            )


class TestWideDataFrame:
    """Tests with many columns to simulate production's ~127 fill columns."""

    def test_many_columns_identical_results(self):
        """Test with a wide DataFrame (many columns) to simulate production data."""
        np.random.seed(42)

        base_df = create_test_dataframe(n_power_units=5, n_timestamps_per_unit=100)

        # Add 120 extra float columns with NaN gaps (simulating production's ~127 fill columns)
        for i in range(120):
            col_data = np.random.uniform(0, 100, size=len(base_df))
            mask = np.random.random(len(base_df)) < 0.3
            col_data[mask] = np.nan
            base_df[f"extra_col_{i}"] = col_data

        result_old = old_method_loop_based(base_df)
        result_new = new_method_vectorized(base_df)

        pd.testing.assert_frame_equal(
            result_old,
            result_new,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )


if __name__ == "__main__":
    # Run a quick test to verify both methods work
    print("Creating test DataFrame...")
    df = create_test_dataframe(n_power_units=5, n_timestamps_per_unit=50)
    print(f"DataFrame shape: {df.shape}")
    print(f"NaN counts:\n{df.isna().sum()}")

    print("\nRunning old method...")
    start = time.time()
    result_old = old_method_loop_based(df)
    print(f"Old method time: {time.time() - start:.3f}s")

    print("\nRunning new method...")
    start = time.time()
    result_new = new_method_vectorized(df)
    print(f"New method time: {time.time() - start:.3f}s")

    print("\nComparing results...")
    try:
        pd.testing.assert_frame_equal(
            result_old, result_new, check_dtype=False, check_exact=False, rtol=1e-5
        )
        print("✅ Results are IDENTICAL!")
    except AssertionError as e:
        print(f"❌ Results differ: {e}")
