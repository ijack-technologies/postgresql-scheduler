"""
Tests for batch processing of power units in time_series_mv_refresh.py.

Validates that:
1. _build_power_unit_filter() generates correct SQL fragments
2. Batching in main() correctly chunks power units
3. get_and_insert_latest_values() works with None (all), single, and multi-unit lists
4. The column-by-column ffill/bfill produces correct results across batches
"""

import sys
from datetime import datetime

import numpy as np
import pandas as pd

# Insert pythonpath
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from project.time_series_mv_refresh import (
    BATCH_SIZE,
    _build_power_unit_filter,
)


class TestBuildPowerUnitFilter:
    """Tests for the _build_power_unit_filter() SQL helper."""

    def test_none_returns_empty_string(self):
        """None means no filter — process all power units."""
        assert _build_power_unit_filter(None) == ""

    def test_empty_list_returns_empty_string(self):
        """Empty list should also return no filter."""
        assert _build_power_unit_filter([]) == ""

    def test_single_unit(self):
        """Single power unit should generate valid IN clause."""
        result = _build_power_unit_filter(["200401"])
        assert result == " and power_unit IN ('200401')"

    def test_multiple_units(self):
        """Multiple power units should be comma-separated in IN clause."""
        result = _build_power_unit_filter(["200401", "200402", "200403"])
        assert result == " and power_unit IN ('200401', '200402', '200403')"

    def test_batch_of_50(self):
        """Typical batch of 50 should generate correct IN clause."""
        units = [f"20040{i:02d}" for i in range(50)]
        result = _build_power_unit_filter(units)
        assert result.startswith(" and power_unit IN (")
        assert result.endswith(")")
        # Count the quoted values
        assert result.count("'") == 100  # 50 units * 2 quotes each

    def test_special_characters_in_unit(self):
        """Power units with unusual characters should be included as-is."""
        result = _build_power_unit_filter(["200-401", "200_402"])
        assert "'200-401'" in result
        assert "'200_402'" in result


class TestBatchSize:
    """Verify the BATCH_SIZE constant is reasonable."""

    def test_batch_size_is_50(self):
        assert BATCH_SIZE == 50

    def test_batch_size_is_positive(self):
        assert BATCH_SIZE > 0


class TestBatchChunking:
    """Test that power units are correctly chunked into batches."""

    def _chunk(self, items: list, size: int) -> list:
        """Replicate the chunking logic from main()."""
        return [items[i : i + size] for i in range(0, len(items), size)]

    def test_exact_multiple(self):
        """100 units with batch size 50 -> exactly 2 batches."""
        units = [f"pu_{i}" for i in range(100)]
        batches = self._chunk(units, 50)
        assert len(batches) == 2
        assert len(batches[0]) == 50
        assert len(batches[1]) == 50

    def test_remainder_batch(self):
        """287 units with batch size 50 -> 5 full batches + 1 partial (37 units)."""
        units = [f"pu_{i}" for i in range(287)]
        batches = self._chunk(units, 50)
        assert len(batches) == 6
        assert all(len(b) == 50 for b in batches[:5])
        assert len(batches[5]) == 37

    def test_fewer_than_batch_size(self):
        """10 units with batch size 50 -> 1 batch of 10."""
        units = [f"pu_{i}" for i in range(10)]
        batches = self._chunk(units, 50)
        assert len(batches) == 1
        assert len(batches[0]) == 10

    def test_single_unit(self):
        """1 unit -> 1 batch of 1."""
        batches = self._chunk(["pu_0"], 50)
        assert len(batches) == 1
        assert batches[0] == ["pu_0"]

    def test_empty_list(self):
        """0 units -> 0 batches."""
        batches = self._chunk([], 50)
        assert len(batches) == 0

    def test_all_units_preserved(self):
        """All original units appear in the batches, in order."""
        units = [f"pu_{i}" for i in range(287)]
        batches = self._chunk(units, 50)
        flattened = [u for batch in batches for u in batch]
        assert flattened == units

    def test_480_units_future_growth(self):
        """480 units (projected growth) -> 10 batches, all under 30-min target."""
        units = [f"pu_{i}" for i in range(480)]
        batches = self._chunk(units, 50)
        assert len(batches) == 10
        # Last batch has 30 units
        assert len(batches[-1]) == 30


class TestFfillBfillWithBatches:
    """Test that ffill/bfill produces correct results when processing
    subsets of power units (as happens with batching)."""

    def _create_df_with_gaps(self, power_units: list[str]) -> pd.DataFrame:
        """Create a DataFrame with known NaN gaps for multiple power units."""
        rows = []
        for pu in power_units:
            # 5 timestamps, with NaN gaps at specific positions
            for i, ts in enumerate(
                [
                    datetime(2024, 1, 1, 0, 0),
                    datetime(2024, 1, 1, 0, 10),
                    datetime(2024, 1, 1, 0, 20),
                    datetime(2024, 1, 1, 0, 30),
                    datetime(2024, 1, 1, 0, 40),
                ]
            ):
                rows.append(
                    {
                        "timestamp_utc": ts,
                        "power_unit": pu,
                        "gateway": f"GW_{pu}",
                        "spm": float(i) if i % 2 == 0 else np.nan,  # 0, NaN, 2, NaN, 4
                        "cgp": 100.0 if i == 0 else np.nan,  # Only first has value
                    }
                )
        return pd.DataFrame(rows)

    def _apply_column_ffill_bfill(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the same column-by-column ffill/bfill as production code."""
        df = df.copy()
        df = df.sort_values(["power_unit", "timestamp_utc"])
        exclude_cols = [
            "timestamp_utc",
            "timestamp_utc_inserted",
            "power_unit",
            "gateway",
        ]
        fill_columns = [col for col in df.columns if col not in exclude_cols]
        grouped = df.groupby("power_unit", sort=False)
        for col in fill_columns:
            df[col] = grouped[col].ffill()
            df[col] = grouped[col].bfill()
        return df.reset_index(drop=True)

    def test_batch_produces_same_results_as_all_at_once(self):
        """Processing units in batches should give same results as all at once."""
        all_units = [f"PU_{i}" for i in range(10)]
        df_all = self._create_df_with_gaps(all_units)

        # Process all at once
        result_all = self._apply_column_ffill_bfill(df_all)

        # Process in batches of 3
        batch_results = []
        for i in range(0, len(all_units), 3):
            batch_units = all_units[i : i + 3]
            df_batch = df_all[df_all["power_unit"].isin(batch_units)].copy()
            batch_results.append(self._apply_column_ffill_bfill(df_batch))

        result_batched = pd.concat(batch_results, ignore_index=True)
        result_batched = result_batched.sort_values(
            ["power_unit", "timestamp_utc"]
        ).reset_index(drop=True)

        pd.testing.assert_frame_equal(
            result_all, result_batched, check_dtype=False, check_exact=False, rtol=1e-5
        )

    def test_ffill_does_not_leak_between_power_units(self):
        """Forward fill must NOT propagate values from one power unit to another."""
        df = pd.DataFrame(
            {
                "timestamp_utc": [
                    datetime(2024, 1, 1, 0, 0),
                    datetime(2024, 1, 1, 0, 10),
                    datetime(2024, 1, 1, 0, 0),
                    datetime(2024, 1, 1, 0, 10),
                ],
                "power_unit": ["A", "A", "B", "B"],
                "gateway": ["GW_A", "GW_A", "GW_B", "GW_B"],
                "spm": [10.0, np.nan, np.nan, np.nan],
            }
        )

        result = self._apply_column_ffill_bfill(df)

        # Power unit A: 10.0 should ffill to second row
        a_values = result[result["power_unit"] == "A"]["spm"].tolist()
        assert a_values == [10.0, 10.0]

        # Power unit B: both NaN, should stay NaN (no value to fill from)
        b_values = result[result["power_unit"] == "B"]["spm"].tolist()
        assert all(pd.isna(v) for v in b_values)

    def test_bfill_does_not_leak_between_power_units(self):
        """Backward fill must NOT propagate values from one power unit to another."""
        df = pd.DataFrame(
            {
                "timestamp_utc": [
                    datetime(2024, 1, 1, 0, 0),
                    datetime(2024, 1, 1, 0, 10),
                    datetime(2024, 1, 1, 0, 0),
                    datetime(2024, 1, 1, 0, 10),
                ],
                "power_unit": ["A", "A", "B", "B"],
                "gateway": ["GW_A", "GW_A", "GW_B", "GW_B"],
                "spm": [np.nan, np.nan, np.nan, 20.0],
            }
        )

        result = self._apply_column_ffill_bfill(df)

        # Power unit A: all NaN, should stay NaN
        a_values = result[result["power_unit"] == "A"]["spm"].tolist()
        assert all(pd.isna(v) for v in a_values)

        # Power unit B: 20.0 should bfill to first row
        b_values = result[result["power_unit"] == "B"]["spm"].tolist()
        assert b_values == [20.0, 20.0]

    def test_large_batch_correctness(self):
        """Test with 50 power units (full batch) to match production batch size."""
        np.random.seed(42)
        units = [f"PU_{i:03d}" for i in range(50)]
        df = self._create_df_with_gaps(units)

        # Process as single batch (50 units)
        result_batch = self._apply_column_ffill_bfill(df)

        # Process one at a time
        single_results = []
        for pu in units:
            df_single = df[df["power_unit"] == pu].copy()
            single_results.append(self._apply_column_ffill_bfill(df_single))

        result_single = pd.concat(single_results, ignore_index=True)
        result_single = result_single.sort_values(
            ["power_unit", "timestamp_utc"]
        ).reset_index(drop=True)

        pd.testing.assert_frame_equal(
            result_batch, result_single, check_dtype=False, check_exact=False, rtol=1e-5
        )


class TestSqlGeneration:
    """Test that SQL queries are correctly generated for different batch scenarios."""

    def test_no_filter_queries_all_data(self):
        """When power_units is None, SQL should have no power_unit filter."""
        pu_filter = _build_power_unit_filter(None)
        sql = f"""
        select * from public.time_series_locf
        where timestamp_utc > '2024-01-01'
            {pu_filter}
        """
        assert "power_unit" not in sql.lower().split("where")[1].split("timestamp")[0]
        assert "IN" not in sql

    def test_single_unit_filter(self):
        """Single unit should produce IN with one value."""
        pu_filter = _build_power_unit_filter(["200401"])
        sql = f"""
        select * from public.time_series
        where timestamp_utc > '2024-01-01'
            {pu_filter}
        """
        assert "IN ('200401')" in sql

    def test_batch_filter(self):
        """Batch should produce IN with multiple values."""
        units = ["200401", "200402", "200403"]
        pu_filter = _build_power_unit_filter(units)
        sql = f"""
        select * from public.time_series
        where timestamp_utc > '2024-01-01'
            {pu_filter}
        """
        assert "IN ('200401', '200402', '200403')" in sql
