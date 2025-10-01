"""
Daily Development Database Refresh Job
Creates fresh development database from production snapshot for IJACK RCOM project
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

from project.logger_config import logger
from project.utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
)


def refresh_dev_database(use_existing_snapshot: bool = True) -> dict:
    """
    Create a fresh development database from production snapshot

    Args:
        c: Configuration object
        use_existing_snapshot: If True, use most recent snapshot instead of creating new one

    Returns:
        Dict with success status and database details
    """

    # Initialize AWS RDS client
    # rds = boto3.client("rds", region_name="ca-central-1")
    rds = boto3.client("rds", region_name="us-west-2")

    # Generate timestamp for naming
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")

    try:
        # Step 1: Check for existing snapshot or create new one
        snapshot_id = f"prod-to-dev-{date_str}"

        if use_existing_snapshot:
            logger.info("🔍 Looking for recent snapshot...")

            # Get most recent prod-to-dev snapshot
            snapshots = rds.describe_db_snapshots(SnapshotType="manual", MaxRecords=20)

            dev_snapshots = [
                s
                for s in snapshots["DBSnapshots"]
                if s["DBSnapshotIdentifier"].startswith("prod-to-dev-")
                and s["Status"] == "available"
            ]

            if dev_snapshots:
                # Sort by creation time and use most recent
                dev_snapshots.sort(key=lambda x: x["SnapshotCreateTime"], reverse=True)
                snapshot_id = dev_snapshots[0]["DBSnapshotIdentifier"]
                logger.info(f"📸 Using existing snapshot: {snapshot_id}")
            else:
                logger.info("📸 No existing snapshots found, creating new one...")
                use_existing_snapshot = False

        # Step 2: Create new snapshot if needed
        if not use_existing_snapshot:
            logger.info("📸 Creating production snapshot...")

            rds.create_db_snapshot(
                DBInstanceIdentifier="ijack",  # Current production database
                DBSnapshotIdentifier=snapshot_id,
                Tags=[
                    {"Key": "Purpose", "Value": "DevRefresh"},
                    {"Key": "CreatedBy", "Value": "AutomatedRefresh"},
                    {"Key": "Date", "Value": date_str},
                ],
            )

        # Step 3: Wait for snapshot completion if we just created it
        if not use_existing_snapshot:
            logger.info("⏳ Waiting for snapshot to complete...")
            waiter = rds.get_waiter("db_snapshot_completed")
            waiter.wait(
                DBSnapshotIdentifier=snapshot_id,
                WaiterConfig={"Delay": 30, "MaxAttempts": 40},
            )
            logger.info("✅ Production snapshot completed!")

        # Step 4: Replace existing development database
        dev_instance_id = "ijack-dev"

        try:
            # Check if dev database exists and delete it
            logger.info("🗑️ Removing existing development database...")
            rds.describe_db_instances(DBInstanceIdentifier=dev_instance_id)

            rds.delete_db_instance(
                DBInstanceIdentifier=dev_instance_id,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )

            # Wait for deletion (but don't wait too long for nightly job)
            logger.info("⏳ Waiting for development database deletion...")
            waiter = rds.get_waiter("db_instance_deleted")
            waiter.wait(
                DBInstanceIdentifier=dev_instance_id,
                WaiterConfig={"Delay": 30, "MaxAttempts": 20},  # Max 10 minutes
            )

        except rds.exceptions.DBInstanceNotFoundFault:
            logger.info("ℹ️ No existing development database found")

        # Step 5: Restore snapshot to new development instance
        logger.info("🔄 Restoring snapshot to fresh development database...")

        rds.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=dev_instance_id,
            DBSnapshotIdentifier=snapshot_id,
            DBInstanceClass="db.t3.micro",
            MultiAZ=False,
            PubliclyAccessible=False,
            StorageType="gp3",
            StorageEncrypted=True,
            AllocatedStorage=20,  # Start small
            MaxAllocatedStorage=50,  # Allow autoscaling
            CopyTagsToSnapshot=True,
            DeletionProtection=False,  # Allow easy deletion for refresh
            Tags=[
                {"Key": "Environment", "Value": "Development"},
                {"Key": "Project", "Value": "RCOM"},
                {"Key": "RefreshedFrom", "Value": "ijack"},
                {"Key": "RefreshDate", "Value": date_str},
                {"Key": "AutoRefresh", "Value": "nightly"},
            ],
        )

        # Step 6: Wait for restoration (with timeout for nightly job)
        logger.info("⏳ Waiting for development database restoration...")
        waiter = rds.get_waiter("db_instance_available")
        waiter.wait(
            DBInstanceIdentifier=dev_instance_id,
            WaiterConfig={"Delay": 30, "MaxAttempts": 30},  # Max 15 minutes
        )

        # Step 7: Get development database details
        dev_response = rds.describe_db_instances(DBInstanceIdentifier=dev_instance_id)
        dev_endpoint = dev_response["DBInstances"][0]["Endpoint"]["Address"]
        dev_size = dev_response["DBInstances"][0]["AllocatedStorage"]

        logger.info("✅ Development database refresh completed!")
        logger.info(f"📍 Development endpoint: {dev_endpoint}")
        logger.info(f"💾 Database size: {dev_size} GB")

        # Step 8: Cleanup old snapshots (keep last 7 days)
        logger.info("🧹 Cleaning up old development snapshots...")

        snapshots = rds.describe_db_snapshots(SnapshotType="manual", MaxRecords=50)

        # Filter for our dev refresh snapshots
        dev_snapshots = [
            s
            for s in snapshots["DBSnapshots"]
            if s["DBSnapshotIdentifier"].startswith("prod-to-dev-")
        ]

        # Sort by creation time (newest first)
        dev_snapshots.sort(key=lambda x: x["SnapshotCreateTime"], reverse=True)

        # Delete snapshots older than 7 days
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=7)

        deleted_count = 0
        for snapshot in dev_snapshots:
            if snapshot["SnapshotCreateTime"] < cutoff_date:
                try:
                    rds.delete_db_snapshot(
                        DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"]
                    )
                    logger.info(
                        f"🗑️ Deleted old snapshot: {snapshot['DBSnapshotIdentifier']}"
                    )
                    deleted_count += 1
                except Exception as e:
                    logger.warning(
                        f"⚠️ Could not delete snapshot {snapshot['DBSnapshotIdentifier']}: {e}"
                    )

        logger.info(f"🧹 Cleanup completed: {deleted_count} old snapshots removed")

        return {
            "success": True,
            "snapshot_id": snapshot_id,
            "dev_endpoint": dev_endpoint,
            "dev_instance": dev_instance_id,
            "dev_size": dev_size,
            "snapshots_cleaned": deleted_count,
        }

    except Exception as e:
        logger.error(f"❌ Error during database refresh: {e}")
        return {"success": False, "error": str(e)}


@error_wrapper(filename=Path(__file__).name)
def main(c: Config) -> None:
    """Main entrypoint function for development database refresh"""

    exit_if_already_running(c, Path(__file__).name)

    logger.info("🚀 Starting development database refresh...")

    try:
        result = refresh_dev_database(use_existing_snapshot=True)

        if result["success"]:
            logger.info("✅ Development database refresh completed successfully!")
            logger.info(f"📍 Development endpoint: {result['dev_endpoint']}")
            logger.info(f"💾 Instance: {result['dev_instance']}")
            logger.info(f"🧹 Snapshots cleaned: {result['snapshots_cleaned']}")
        else:
            logger.error("❌ Development database refresh failed")
            raise Exception("Database refresh failed")

    except Exception as e:
        logger.error(f"❌ Fatal error during refresh: {e}")
        raise

    return None


if __name__ == "__main__":
    c = Config()
    main(c)
