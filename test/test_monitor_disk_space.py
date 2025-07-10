import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from project.monitor_disk_space import (
    check_disk_space,
    monitor_disk_space_main,
    send_email_alert,
)
from project.utils import Config


class TestCheckDiskSpace(unittest.TestCase):
    """Test cases for the check_disk_space function."""

    @patch("subprocess.check_output")
    def test_normal_disk_usage_below_threshold(self, mock_check_output):
        """Test when disk usage is below the threshold."""
        # Mock the output of the df command
        mock_df_output = (
            "Filesystem     Size Used Avail Use% Mounted on\n"
            "/dev/sda1      50G  30G   20G  60% /\n"
            "/dev/sdb1      100G 50G   50G  50% /data\n"
        )
        mock_check_output.return_value = mock_df_output

        # Call the function with a threshold of 70%
        alert_needed, disk_info, critical_fs = check_disk_space(threshold_percentage=70)

        # Assertions
        self.assertFalse(alert_needed)
        self.assertEqual(len(disk_info), 2)
        self.assertEqual(len(critical_fs), 0)
        self.assertEqual(disk_info[0]["use_percentage_value"], 60)
        self.assertEqual(disk_info[1]["use_percentage_value"], 50)

    @patch("subprocess.check_output")
    def test_disk_usage_above_threshold(self, mock_check_output):
        """Test when disk usage is above the threshold."""
        # Mock the output of the df command
        mock_df_output = (
            "Filesystem     Size Used Avail Use% Mounted on\n"
            "/dev/sda1      50G  40G   10G  80% /\n"
            "/dev/sdb1      100G 95G   5G   95% /data\n"
        )
        mock_check_output.return_value = mock_df_output

        # Call the function with a threshold of 70%
        alert_needed, disk_info, critical_fs = check_disk_space(threshold_percentage=70)

        # Assertions
        self.assertTrue(alert_needed)
        self.assertEqual(len(disk_info), 2)
        self.assertEqual(len(critical_fs), 2)
        self.assertEqual(critical_fs[0]["use_percentage_value"], 80)
        self.assertEqual(critical_fs[1]["use_percentage_value"], 95)

    @patch("subprocess.check_output")
    def test_mixed_disk_usage(self, mock_check_output):
        """Test with mixed disk usage (some above, some below threshold)."""
        # Mock the output of the df command
        mock_df_output = (
            "Filesystem     Size Used Avail Use% Mounted on\n"
            "/dev/sda1      50G  40G   10G  80% /\n"
            "/dev/sdb1      100G 60G   40G  60% /data\n"
            "/dev/sdc1      200G 190G  10G  95% /backups\n"
        )
        mock_check_output.return_value = mock_df_output

        # Call the function with a threshold of 70%
        alert_needed, disk_info, critical_fs = check_disk_space(threshold_percentage=70)

        # Assertions
        self.assertTrue(alert_needed)
        self.assertEqual(len(disk_info), 3)
        self.assertEqual(len(critical_fs), 2)
        self.assertEqual(critical_fs[0]["use_percentage_value"], 80)
        self.assertEqual(critical_fs[1]["use_percentage_value"], 95)

    @patch("subprocess.check_output")
    def test_special_filesystems_excluded(self, mock_check_output):
        """Test that special filesystems are excluded from consideration."""
        # Mock the output of the df command with some special filesystems
        mock_df_output = (
            "Filesystem     Size Used Avail Use% Mounted on\n"
            "/dev/sda1      50G  45G   5G   90% /\n"
            "tmpfs          1G   0G    1G   0%  /dev/shm\n"
            "devtmpfs       1G   0G    1G   0%  /dev\n"
            "proc           0G   0G    0G   0%  /proc\n"
            "sysfs          0G   0G    0G   0%  /sys\n"
            "/dev/sdb1      100G 90G   10G  90% /data\n"
        )
        mock_check_output.return_value = mock_df_output

        # Call the function
        alert_needed, disk_info, critical_fs = check_disk_space(threshold_percentage=80)

        # Assertions
        self.assertTrue(alert_needed)
        # Only real filesystems matching our criteria should be included
        self.assertEqual(len(disk_info), 2)
        self.assertEqual(disk_info[0]["mount_point"], "/")
        self.assertEqual(disk_info[1]["mount_point"], "/data")
        self.assertEqual(len(critical_fs), 2)

    @patch("subprocess.check_output")
    def test_invalid_percentage_handling(self, mock_check_output):
        """Test handling of invalid percentage values."""
        mock_df_output = (
            "Filesystem     Size Used Avail Use% Mounted on\n"
            "/dev/sda1      50G  40G   10G  80% /\n"
            "/dev/sdb1      100G 60G   40G  N/A /data\n"  # Invalid percentage
        )
        mock_check_output.return_value = mock_df_output

        # Call the function
        alert_needed, disk_info, critical_fs = check_disk_space(threshold_percentage=70)

        # Assertions
        self.assertTrue(alert_needed)
        self.assertEqual(len(disk_info), 1)  # Only valid entry should be included
        self.assertEqual(disk_info[0]["use_percentage_value"], 80)
        self.assertEqual(len(critical_fs), 1)

    @patch(
        "subprocess.check_output", side_effect=subprocess.CalledProcessError(1, "df")
    )
    def test_subprocess_error(self, mock_check_output):
        """Test handling of subprocess errors."""
        # Call the function
        alert_needed, disk_info, critical_fs = check_disk_space()

        # Assertions
        self.assertFalse(alert_needed)
        self.assertEqual(disk_info, [])
        self.assertEqual(critical_fs, [])

    @patch("subprocess.check_output")
    def test_empty_output(self, mock_check_output):
        """Test handling of empty output."""
        mock_check_output.return_value = (
            "Filesystem     Size Used Avail Use% Mounted on\n"
        )

        # Call the function
        alert_needed, disk_info, critical_fs = check_disk_space()

        # Assertions
        self.assertFalse(alert_needed)
        self.assertEqual(disk_info, [])
        self.assertEqual(critical_fs, [])


class TestSendEmailAlert(unittest.TestCase):
    """Test cases for the send_email_alert function."""

    @patch("project.monitor_disk_space.send_mailgun_email")
    @patch("subprocess.check_output")
    def test_email_sent_successfully(self, mock_check_output, mock_send_email):
        """Test successful email sending."""
        # Mock configuration
        mock_config = MagicMock(spec=Config)
        mock_config.EMAIL_LIST_DEV = ["admin@example.com"]

        # Mock instance ID
        mock_check_output.return_value = "i-1234567890abcdef0"

        # Prepare test data
        disk_info = [
            {
                "filesystem": "/dev/sda1",
                "mount_point": "/",
                "size": "50G",
                "used": "45G",
                "available": "5G",
                "use_percentage": "90%",
                "use_percentage_value": 90,
            }
        ]
        critical_fs = disk_info.copy()

        # Mock successful email sending
        mock_send_email.return_value = True

        # Call the function
        result = send_email_alert(mock_config, disk_info, critical_fs)

        # Assertions
        self.assertTrue(result)
        mock_send_email.assert_called_once()
        # Check email parameters
        args, kwargs = mock_send_email.call_args
        self.assertEqual(kwargs["emailees_list"], mock_config.EMAIL_LIST_DEV)
        self.assertEqual(kwargs["subject"], "[ALERT] Low Disk Space on EC2 Server")
        self.assertIn("Low disk space detected", kwargs["text"])
        self.assertIn("EC2 Instance ID: i-1234567890abcdef0", kwargs["text"])

    @patch("project.monitor_disk_space.send_mailgun_email")
    @patch(
        "subprocess.check_output", side_effect=subprocess.CalledProcessError(1, "curl")
    )
    def test_email_without_instance_id(self, mock_check_output, mock_send_email):
        """Test email sending when instance ID cannot be determined."""
        # Mock configuration
        mock_config = MagicMock(spec=Config)
        mock_config.EMAIL_LIST_DEV = ["admin@example.com"]

        # Prepare test data
        disk_info = [
            {
                "filesystem": "/dev/sda1",
                "mount_point": "/",
                "size": "50G",
                "used": "45G",
                "available": "5G",
                "use_percentage": "90%",
                "use_percentage_value": 90,
            }
        ]
        critical_fs = disk_info.copy()

        # Mock successful email sending
        mock_send_email.return_value = True

        # Call the function
        result = send_email_alert(mock_config, disk_info, critical_fs)

        # Assertions
        self.assertTrue(result)
        mock_send_email.assert_called_once()
        # Check email content
        args, kwargs = mock_send_email.call_args
        self.assertIn("Unable to determine EC2 Instance ID", kwargs["text"])

    @patch("subprocess.check_output")
    @patch(
        "project.monitor_disk_space.send_mailgun_email",
        side_effect=Exception("SMTP error"),
    )
    def test_email_sending_failure(self, mock_send_email, mock_check_output):
        """Test handling of email sending failures."""
        # Mock configuration
        mock_config = MagicMock(spec=Config)
        mock_config.EMAIL_LIST_DEV = ["admin@example.com"]
        mock_check_output.return_value = "i-1234567890abcdef0"

        # Prepare test data
        disk_info = [
            {
                "filesystem": "/dev/sda1",
                "mount_point": "/",
                "size": "50G",
                "used": "45G",
                "available": "5G",
                "use_percentage": "90%",
                "use_percentage_value": 90,
            }
        ]
        critical_fs = disk_info.copy()

        # Call the function
        result = send_email_alert(mock_config, disk_info, critical_fs)

        # Assertions
        self.assertFalse(result)
        mock_send_email.assert_called_once()


class TestMonitorDiskSpaceMain(unittest.TestCase):
    """Test cases for the monitor_disk_space_main function."""

    @patch("project.monitor_disk_space.exit_if_already_running")
    @patch("project.monitor_disk_space.check_disk_space")
    @patch("project.monitor_disk_space.send_email_alert")
    def test_no_alert_needed(self, mock_send_email, mock_check_disk, mock_exit_check):
        """Test when no alert is needed."""
        # Mock configuration
        mock_config = MagicMock(spec=Config)

        # Mock disk space check to return no alert needed
        mock_check_disk.return_value = (False, [], [])

        # Call the function
        monitor_disk_space_main(mock_config)

        # Assertions
        mock_exit_check.assert_called_once()
        mock_check_disk.assert_called_once_with(threshold_percentage=90)
        mock_send_email.assert_not_called()

    @patch("project.monitor_disk_space.exit_if_already_running")
    @patch("project.monitor_disk_space.check_disk_space")
    @patch("project.monitor_disk_space.send_email_alert")
    def test_alert_needed_email_success(
        self, mock_send_email, mock_check_disk, mock_exit_check
    ):
        """Test when alert is needed and email is sent successfully."""
        # Mock configuration
        mock_config = MagicMock(spec=Config)

        # Mock disk space data
        disk_info = [{"mount_point": "/", "use_percentage_value": 95}]
        critical_fs = disk_info.copy()

        # Mock disk space check to return alert needed
        mock_check_disk.return_value = (True, disk_info, critical_fs)

        # Mock email sending success
        mock_send_email.return_value = True

        # Call the function
        monitor_disk_space_main(mock_config)

        # Assertions
        mock_exit_check.assert_called_once()
        mock_check_disk.assert_called_once_with(threshold_percentage=90)
        mock_send_email.assert_called_once_with(mock_config, disk_info, critical_fs)

    @patch("project.monitor_disk_space.exit_if_already_running")
    @patch("project.monitor_disk_space.check_disk_space")
    @patch("project.monitor_disk_space.send_email_alert")
    def test_alert_needed_email_failure(
        self, mock_send_email, mock_check_disk, mock_exit_check
    ):
        """Test when alert is needed but email sending fails."""
        # Mock configuration
        mock_config = MagicMock(spec=Config)

        # Mock disk space data
        disk_info = [{"mount_point": "/", "use_percentage_value": 95}]
        critical_fs = disk_info.copy()

        # Mock disk space check to return alert needed
        mock_check_disk.return_value = (True, disk_info, critical_fs)

        # Mock email sending failure
        mock_send_email.return_value = False

        # Call the function
        monitor_disk_space_main(mock_config)

        # Assertions
        mock_exit_check.assert_called_once()
        mock_check_disk.assert_called_once_with(threshold_percentage=90)
        mock_send_email.assert_called_once_with(mock_config, disk_info, critical_fs)

    @patch("project.monitor_disk_space.exit_if_already_running")
    @patch(
        "project.monitor_disk_space.check_disk_space",
        side_effect=Exception("Test exception"),
    )
    def test_exception_handling(self, mock_check_disk, mock_exit_check):
        """Test handling of exceptions in the main function."""
        # Mock configuration
        mock_config = MagicMock(spec=Config)

        # Call the function
        monitor_disk_space_main(mock_config)

        # Assertions
        mock_exit_check.assert_called_once()
        mock_check_disk.assert_called_once_with(threshold_percentage=90)
        # No assertion for error handling, but the function should complete without raising exceptions

    @patch("project.monitor_disk_space.exit_if_already_running")
    @patch("project.monitor_disk_space.check_disk_space")
    @patch("project.monitor_disk_space.send_email_alert")
    def test_custom_threshold(self, mock_send_email, mock_check_disk, mock_exit_check):
        """Test using a custom threshold value."""
        # Mock configuration
        mock_config = MagicMock(spec=Config)

        # Mock disk space check to return no alert needed
        mock_check_disk.return_value = (False, [], [])

        # Call the function with custom threshold
        monitor_disk_space_main(mock_config, threshold=75)

        # Assertions
        mock_exit_check.assert_called_once()
        mock_check_disk.assert_called_once_with(threshold_percentage=75)
        mock_send_email.assert_not_called()


if __name__ == "__main__":
    unittest.main()
