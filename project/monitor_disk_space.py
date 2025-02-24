import subprocess
from pathlib import Path

from project.logger_config import logger
from project.utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
    send_mailgun_email,
)


def check_disk_space(threshold_percentage: int = 90) -> tuple:
    """
    Check the disk space on the EC2 instance and return True if disk usage
    is above the specified threshold percentage.

    Args:
        threshold_percentage (int): The percentage threshold at which to trigger an alert

    Returns:
        tuple: (is_alert_needed (bool), disk_info (dict))
    """
    # Run df command to get disk usage information
    try:
        # Get disk usage in a more structured format
        df_output = subprocess.check_output(
            ["df", "-h", "--output=source,size,used,avail,pcent,target"],
            universal_newlines=True,
        )

        # Process the output
        lines = df_output.strip().split("\n")
        # headers = lines[0].split()

        disk_info = []
        for line in lines[1:]:
            # Skip if line is empty
            if not line.strip():
                continue

            parts = line.split()
            if len(parts) >= 6:  # Ensure we have all expected columns
                # Handle filesystem, size, used, avail, use%, mounted on
                fs_info = {
                    "filesystem": parts[0],
                    "size": parts[1],
                    "used": parts[2],
                    "available": parts[3],
                    "use_percentage": parts[4],
                    "mount_point": " ".join(
                        parts[5:]
                    ),  # Join remaining parts as mount point
                }

                # Convert use percentage to number by removing % sign
                try:
                    use_percentage = int(fs_info["use_percentage"].strip("%"))
                    fs_info["use_percentage_value"] = use_percentage

                    # Only include real filesystems (not special filesystems)
                    # and those that match our threshold criteria
                    if (
                        "/" in fs_info["mount_point"]
                        and not fs_info["mount_point"].startswith("/dev")
                        and not fs_info["mount_point"].startswith("/sys")
                        and not fs_info["mount_point"].startswith("/proc")
                    ):
                        disk_info.append(fs_info)
                except ValueError:
                    # Skip entries with invalid use percentage
                    continue

        # Check if any filesystem exceeds the threshold
        alert_needed = False
        critical_filesystems = []

        for fs in disk_info:
            if fs["use_percentage_value"] >= threshold_percentage:
                alert_needed = True
                critical_filesystems.append(fs)

        return alert_needed, disk_info, critical_filesystems

    except subprocess.CalledProcessError:
        # Log the error
        logger.exception("Error running df command")
        return False, [], []
    except Exception:
        # Catch any other exceptions
        logger.exception("Unexpected error checking disk space")
        return False, [], []


def send_email_alert(c: Config, disk_info: list, critical_filesystems: list) -> bool:
    """
    Send an email alert about low disk space.

    Args:
        disk_info (list): List of dictionaries containing disk information
        critical_filesystems (list): List of filesystems that exceed the threshold

    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    try:
        # Create the email body
        body = "Low disk space detected on the following filesystems:\n\n"

        # Add critical filesystems to the email
        for fs in critical_filesystems:
            body += f"Mount point: {fs['mount_point']}\n"
            body += f"Filesystem: {fs['filesystem']}\n"
            body += f"Size: {fs['size']}\n"
            body += f"Used: {fs['used']} ({fs['use_percentage']})\n"
            body += f"Available: {fs['available']}\n"
            body += "\n"

        # Add all disk info for reference
        body += "\nAll filesystem information:\n\n"
        for fs in disk_info:
            body += f"{fs['mount_point']} - Used: {fs['use_percentage']} (Size: {fs['size']}, Available: {fs['available']})\n"

        # Add the server identification information
        try:
            instance_id = subprocess.check_output(
                ["curl", "-s", "http://169.254.169.254/latest/meta-data/instance-id"],
                universal_newlines=True,
            )
            body += f"\nEC2 Instance ID: {instance_id}\n"
        except subprocess.CalledProcessError:
            body += "\nUnable to determine EC2 Instance ID\n"

        send_mailgun_email(
            c,
            text=body,
            emailees_list=c.EMAIL_LIST_DEV,
            subject="[ALERT] Low Disk Space on EC2 Server",
        )

        return True
    except Exception as e:
        logger.exception(f"Error sending email alert: {e}")
        return False


@error_wrapper(filename=Path(__file__).name)
def monitor_disk_space_main(c: Config) -> None:
    """
    Main function to check disk space and send alerts if needed.

    Args:
        c (Config): The configuration object with SMTP settings

    Returns:
        None
    """

    exit_if_already_running(c, Path(__file__).name)

    # Read configuration from config object
    try:
        # Get disk space threshold from config or use default
        threshold = c.get("monitoring", "disk_space_threshold", 90)

        # Check if we're above the threshold
        alert_needed, disk_info, critical_filesystems = check_disk_space(
            threshold_percentage=threshold
        )

        # If disk usage is above threshold, send an alert
        if alert_needed:
            from project.logger_config import logger

            logger.warning(
                f"Disk space alert triggered. {len(critical_filesystems)} filesystems exceed {threshold}% usage."
            )

            # Send the email alert
            email_sent = send_email_alert(c, disk_info, critical_filesystems)

            if email_sent:
                logger.info("Disk space alert email sent successfully")
            else:
                logger.error("Failed to send disk space alert email")

    except Exception as e:
        from project.logger_config import logger

        logger.error(f"Error in disk space monitoring: {e}")

    return None
