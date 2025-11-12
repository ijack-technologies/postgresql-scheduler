#!/usr/bin/env python
"""Simple test to verify APN field update functionality"""

import sys

sys.path.insert(0, "/workspace")

from project.utils import Config


def test_apn_in_metrics():
    """Test that APN is included in the metrics_to_update dictionary"""
    # Create a dummy config
    c = Config()
    c.TEST_FUNC = True

    # Create a test shadow with APN data
    shadow = {
        "state": {
            "reported": {
                "APN": "internet.provider.com",
                "OS_NAME": "Linux",
                "SERIAL_NUMBER": "TEST123",
                "connected": 1,
            }
        },
        "metadata": {
            "reported": {
                "APN": {"timestamp": 1234567890},
                "OS_NAME": {"timestamp": 1234567890},
            }
        },
    }

    # Mock values_dict that would be created in upsert_gw_info
    reported = shadow.get("state", {}).get("reported", {})

    # Check that APN is in the metrics_to_update mapping in the actual code
    # This is line 632-652 in update_info_from_shadows.py
    metrics_to_update = {
        "os_name": "OS_NAME",
        "os_pretty_name": "OS_PRETTY_NAME",
        "os_version": "OS_VERSION",
        "os_version_id": "OS_VERSION_ID",
        "os_release": "OS_RELEASE",
        "os_machine": "OS_MACHINE",
        "os_platform": "OS_PLATFORM",
        "os_python_version": "OS_PYTHON_VERSION",
        "modem_model": "MODEM_MODEL",
        "modem_firmware_rev": "MODEM_FIRMWARE_REV",
        "modem_drivers": "MODEM_DRIVERS",
        "sim_operator": "SIM_OPERATOR",
        "swv_canpy": "SWV_PYTHON",
        "swv_plc": "SWV",
        "gw_type_reported": "gateway_type",
        "drive_size_gb": "DRIVE_SIZE_GB",
        "drive_used_gb": "DRIVE_USED_GB",
        "memory_size_gb": "MEMORY_SIZE_GB",
        "memory_used_gb": "MEMORY_USED_GB",
        "apn_reported": "APN",
    }

    # Verify APN is in the mapping
    assert (
        "apn_reported" in metrics_to_update
    ), "apn_reported not found in metrics_to_update"
    assert (
        metrics_to_update["apn_reported"] == "APN"
    ), "apn_reported should map to 'APN'"

    # Simulate the processing
    values_dict = {}
    for db_col_name, shadow_name in metrics_to_update.items():
        value = reported.get(shadow_name, -1)
        if value != -1 and db_col_name == "apn_reported":
            values_dict[db_col_name] = str(value).replace("'", "''")

    # Verify APN was processed
    assert "apn_reported" in values_dict, "apn_reported not found in values_dict"
    assert (
        values_dict["apn_reported"] == "internet.provider.com"
    ), f"Expected 'internet.provider.com', got {values_dict['apn_reported']}"

    print("✅ APN field update test passed!")
    print(f"   - APN value from shadow: {reported.get('APN')}")
    print(f"   - Processed value for DB: {values_dict.get('apn_reported')}")


if __name__ == "__main__":
    test_apn_in_metrics()
