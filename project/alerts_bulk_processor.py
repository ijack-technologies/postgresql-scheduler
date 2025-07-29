"""
AlertBulkProcessor service for processing bulk alert subscriptions into individual alerts.

This service follows DRY and SOLID principles:
- Single Responsibility: Processes bulk alerts only
- Open/Closed: Extensible for new filter types
- Dependency Inversion: Depends on abstractions (config)
"""

from pathlib import Path
from typing import Dict, List

from project.logger_config import logger
from project.utils import Config, error_wrapper, run_query, utcnow_naive

LOGFILE_NAME = "alerts_bulk_processor"


class AlertBulkProcessor:
    """
    Service for processing bulk alert subscriptions into individual alert records.

    This class handles the nightly job that:
    1. Reads all AlertBulk records
    2. Finds matching power units based on filters
    3. Creates/updates individual Alert records
    """

    def __init__(self, config: Config):
        """
        Initialize the processor with configuration.

        Args:
            config: Configuration object with database settings
        """
        self.config = config
        self.stats = {
            "bulk_subscriptions_processed": 0,
            "power_units_found": 0,
            "alerts_inserted": 0,
            "alerts_updated": 0,
            "errors": 0,
        }

    def process_all_bulk_alerts(self) -> Dict[str, int]:
        """
        Main entry point - process all bulk alert subscriptions.

        Returns:
            Dictionary with processing statistics
        """
        logger.info("Starting bulk alert processing job")

        try:
            # Get all active bulk alert subscriptions
            sql = "SELECT * FROM public.alerts_bulk"
            _, bulk_alerts = run_query(sql, db="ijack", fetchall=True)

            if not bulk_alerts:
                logger.info("No bulk alert subscriptions found to process")
                return self.stats

            logger.info(f"Found {len(bulk_alerts)} bulk alert subscriptions to process")

            for bulk_alert in bulk_alerts:
                try:
                    self._process_single_bulk_alert(bulk_alert)
                    self.stats["bulk_subscriptions_processed"] += 1
                except Exception as e:
                    logger.error(
                        f"Error processing bulk alert {bulk_alert['id']} for user {bulk_alert['user_id']}: {e}"
                    )
                    self.stats["errors"] += 1

            logger.info(f"Bulk alert processing completed successfully: {self.stats}")

        except Exception as e:
            logger.error(f"Critical error in bulk alert processing: {e}")
            self.stats["errors"] += 1

        return self.stats

    def _process_single_bulk_alert(self, bulk_alert: Dict) -> None:
        """
        Process a single bulk alert subscription.

        Args:
            bulk_alert: The alerts_bulk record to process
        """
        logger.debug(
            f"Processing bulk alert {bulk_alert['id']} for user {bulk_alert['user_id']}"
        )

        # Find matching power units based on filters
        power_unit_ids: List[int] = self._get_matching_power_units(bulk_alert)
        self.stats["power_units_found"] += len(power_unit_ids)

        if not power_unit_ids:
            logger.debug(
                f"No matching power units found for bulk alert {bulk_alert['id']}"
            )
            return

        # Process power units in batch based on update_existing_alerts setting
        n_power_units = len(power_unit_ids)
        logger.info(
            f"Processing {n_power_units} power units for bulk alert {bulk_alert['id']} using batch processing..."
        )

        if bulk_alert.get("update_existing_alerts", True):
            # Batch update existing alerts or create new ones
            self._batch_upsert_alerts(bulk_alert, power_unit_ids)
        else:
            # Only create alerts for power units that don't already have alerts
            # First, filter out power units that already have alerts
            sql = """
                SELECT power_unit_id 
                FROM public.alerts 
                WHERE user_id = %s AND power_unit_id = ANY(%s)
            """
            _, existing_alerts = run_query(
                sql,
                db="ijack",
                fetchall=True,
                data=(bulk_alert["user_id"], power_unit_ids),
            )

            existing_power_unit_ids = {row["power_unit_id"] for row in existing_alerts}
            new_power_unit_ids = [
                pid for pid in power_unit_ids if pid not in existing_power_unit_ids
            ]

            if new_power_unit_ids:
                logger.info(
                    f"Creating alerts for {len(new_power_unit_ids)} new power units "
                    f"(skipping {len(existing_power_unit_ids)} existing)"
                )
                self._batch_upsert_alerts(bulk_alert, new_power_unit_ids)
            else:
                logger.info(
                    f"All {len(power_unit_ids)} power units already have alerts for user {bulk_alert['user_id']}"
                )

    def _get_matching_power_units(self, bulk_alert: Dict) -> List[int]:
        """
        Get power unit IDs matching the bulk alert filters using direct table joins.

        Args:
            bulk_alert: The alerts_bulk record with filter criteria

        Returns:
            List of power unit IDs that match the filters
        """
        # Check if this is a wildcard case (all filters are NULL)
        is_wildcard = (
            bulk_alert.get("unit_type_id") is None
            and bulk_alert.get("model_type_id") is None
            and bulk_alert.get("customer_id") is None
        )

        # Build WHERE conditions based on wildcard vs filtered mode
        conditions = []
        params = []

        # Base conditions that always apply
        conditions.append("t4.customer_id IS NOT NULL")
        # 1 = IJACK Inc
        # 2 = No Customer
        # 3 = IJACK Corp
        # 21 = Demo/Test Customer
        conditions.append("t4.customer_id NOT IN (1, 2, 3, 21)")  # Demo/test customers
        conditions.append("t1.power_unit_id IS NOT NULL")
        conditions.append("t1.id IS NOT NULL")  # structure_id
        conditions.append("t3.id IS NOT NULL")  # gateway_id
        conditions.append("t1.structure_install_date IS NOT NULL")
        conditions.append("t1.surface IS NOT NULL")

        # Additional conditions for wildcard case to ensure data quality
        if is_wildcard:
            # Match the simple script's criteria exactly
            conditions.append("t1.unit_type_id IS NOT NULL")
            conditions.append("t1.model_type_id IS NOT NULL")
            logger.debug(
                f"Processing wildcard bulk alert {bulk_alert['id']} - will match ALL eligible power units"
            )
        else:
            # Apply user's filter criteria (NULL = don't filter on that field)
            if bulk_alert.get("unit_type_id") is not None:
                conditions.append("t1.unit_type_id = %s")
                params.append(bulk_alert["unit_type_id"])

            if bulk_alert.get("model_type_id") is not None:
                conditions.append("t1.model_type_id = %s")
                params.append(bulk_alert["model_type_id"])

            if bulk_alert.get("customer_id") is not None:
                conditions.append("t4.customer_id = %s")
                params.append(bulk_alert["customer_id"])

        # If update_existing_alerts is False, exclude power units with existing alerts
        if not bulk_alert.get("update_existing_alerts", True):
            conditions.append(
                f"t1.power_unit_id NOT IN (SELECT power_unit_id FROM public.alerts WHERE user_id = {bulk_alert['user_id']})"
            )

        # Build the complete SQL query
        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT DISTINCT t1.power_unit_id
            FROM structures t1
            LEFT JOIN power_units t2 ON t2.id = t1.power_unit_id
            LEFT JOIN gw t3 ON t3.power_unit_id = t2.id
            LEFT JOIN public.structure_customer_rel t4 ON t4.structure_id = t1.id
            LEFT JOIN public.customers t5 ON t5.id = t4.customer_id
            WHERE {where_clause}
        """

        # Execute query to get distinct power unit IDs
        try:
            _, rows = run_query(sql, db="ijack", fetchall=True, data=tuple(params))
            # Extract IDs from result
            return [
                row["power_unit_id"] for row in rows if row["power_unit_id"] is not None
            ]

        except Exception as e:
            logger.error(
                f"Error querying matching power units for bulk alert {bulk_alert['id']}: {e}"
            )
            return []

    def _upsert_individual_alert(self, bulk_alert: Dict, power_unit_id: int) -> None:
        """
        Insert or update an individual alert record for a power unit.

        Args:
            bulk_alert: The alerts_bulk record with alert settings
            power_unit_id: The power unit ID to create/update alert for
        """
        try:
            # Prepare values for insert/update
            values = {
                "user_id": bulk_alert["user_id"],
                "power_unit_id": power_unit_id,
                "timestamp_utc_inserted": utcnow_naive(),
                # Delivery preferences
                "wants_sms": bulk_alert.get("wants_sms", True),
                "wants_email": bulk_alert.get("wants_email", False),
                "wants_phone": bulk_alert.get("wants_phone", False),
                "wants_short_sms": bulk_alert.get("wants_short_sms", False),
                "wants_short_email": bulk_alert.get("wants_short_email", False),
                "wants_short_phone": bulk_alert.get("wants_short_phone", True),
                "wants_whatsapp": bulk_alert.get("wants_whatsapp", False),
                # Regular alerts
                "heartbeat": bulk_alert.get("heartbeat", True),
                "online_hb": bulk_alert.get("online_hb", False),
                "warn1": bulk_alert.get("warn1", False),
                "warn2": bulk_alert.get("warn2", False),
                "suction": bulk_alert.get("suction", False),
                "discharge": bulk_alert.get("discharge", False),
                "mtr": bulk_alert.get("mtr", False),
                "spm": bulk_alert.get("spm", False),
                "stboxf": bulk_alert.get("stboxf", False),
                "hyd_temp": bulk_alert.get("hyd_temp", False),
                # AI alerts
                "wants_card_ml": bulk_alert.get("wants_card_ml", False),
                # Change detection alerts
                "change_suction": bulk_alert.get("change_suction", True),
                "change_hyd_temp": bulk_alert.get("change_hyd_temp", False),
                "change_dgp": bulk_alert.get("change_dgp", True),
                "change_hp_delta": bulk_alert.get("change_hp_delta", True),
                # Hydraulic oil alerts
                "hyd_oil_lvl": bulk_alert.get("hyd_oil_lvl", False),
                "hyd_filt_life": bulk_alert.get("hyd_filt_life", False),
                "hyd_oil_life": bulk_alert.get("hyd_oil_life", False),
                # Other alerts
                "chk_mtr_ovld": bulk_alert.get("chk_mtr_ovld", False),
                "pwr_fail": bulk_alert.get("pwr_fail", False),
                "soft_start_err": bulk_alert.get("soft_start_err", False),
                "grey_wire_err": bulk_alert.get("grey_wire_err", False),
                "ae011": bulk_alert.get("ae011", False),
            }

            # Use INSERT ... ON CONFLICT DO UPDATE for upsert
            sql = """
                INSERT INTO public.alerts (
                    user_id, power_unit_id, timestamp_utc_inserted,
                    wants_sms, wants_email, wants_phone, wants_short_sms, 
                    wants_short_email, wants_short_phone, wants_whatsapp,
                    heartbeat, online_hb, warn1, warn2, suction, discharge, 
                    mtr, spm, stboxf, hyd_temp, wants_card_ml,
                    change_suction, change_hyd_temp, change_dgp, change_hp_delta,
                    hyd_oil_lvl, hyd_filt_life, hyd_oil_life,
                    chk_mtr_ovld, pwr_fail, soft_start_err, grey_wire_err, ae011
                )
                VALUES (
                    %(user_id)s, %(power_unit_id)s, %(timestamp_utc_inserted)s,
                    %(wants_sms)s, %(wants_email)s, %(wants_phone)s, %(wants_short_sms)s,
                    %(wants_short_email)s, %(wants_short_phone)s, %(wants_whatsapp)s,
                    %(heartbeat)s, %(online_hb)s, %(warn1)s, %(warn2)s, %(suction)s, %(discharge)s,
                    %(mtr)s, %(spm)s, %(stboxf)s, %(hyd_temp)s, %(wants_card_ml)s,
                    %(change_suction)s, %(change_hyd_temp)s, %(change_dgp)s, %(change_hp_delta)s,
                    %(hyd_oil_lvl)s, %(hyd_filt_life)s, %(hyd_oil_life)s,
                    %(chk_mtr_ovld)s, %(pwr_fail)s, %(soft_start_err)s, %(grey_wire_err)s, %(ae011)s
                )
                ON CONFLICT (user_id, power_unit_id) 
                DO UPDATE SET 
                    wants_sms = EXCLUDED.wants_sms,
                    wants_email = EXCLUDED.wants_email,
                    wants_phone = EXCLUDED.wants_phone,
                    wants_short_sms = EXCLUDED.wants_short_sms,
                    wants_short_email = EXCLUDED.wants_short_email,
                    wants_short_phone = EXCLUDED.wants_short_phone,
                    wants_whatsapp = EXCLUDED.wants_whatsapp,
                    heartbeat = EXCLUDED.heartbeat,
                    online_hb = EXCLUDED.online_hb,
                    warn1 = EXCLUDED.warn1,
                    warn2 = EXCLUDED.warn2,
                    suction = EXCLUDED.suction,
                    discharge = EXCLUDED.discharge,
                    mtr = EXCLUDED.mtr,
                    spm = EXCLUDED.spm,
                    stboxf = EXCLUDED.stboxf,
                    hyd_temp = EXCLUDED.hyd_temp,
                    wants_card_ml = EXCLUDED.wants_card_ml,
                    change_suction = EXCLUDED.change_suction,
                    change_hyd_temp = EXCLUDED.change_hyd_temp,
                    change_dgp = EXCLUDED.change_dgp,
                    change_hp_delta = EXCLUDED.change_hp_delta,
                    hyd_oil_lvl = EXCLUDED.hyd_oil_lvl,
                    hyd_filt_life = EXCLUDED.hyd_filt_life,
                    hyd_oil_life = EXCLUDED.hyd_oil_life,
                    chk_mtr_ovld = EXCLUDED.chk_mtr_ovld,
                    pwr_fail = EXCLUDED.pwr_fail,
                    soft_start_err = EXCLUDED.soft_start_err,
                    grey_wire_err = EXCLUDED.grey_wire_err,
                    ae011 = EXCLUDED.ae011
                RETURNING (xmax = 0) AS inserted
            """

            _, result = run_query(
                sql,
                db="ijack",
                fetchall=True,
                commit=True,
                data=values,
                log_query=False,
            )

            if result and result[0]["inserted"]:
                self.stats["alerts_inserted"] += 1
                logger.debug(
                    f"Created new alert for user {bulk_alert['user_id']}, power unit {power_unit_id}"
                )
            else:
                self.stats["alerts_updated"] += 1
                logger.debug(
                    f"Updated alert for user {bulk_alert['user_id']}, power unit {power_unit_id}"
                )

        except Exception as e:
            logger.error(
                f"Error upserting alert for user {bulk_alert['user_id']}, power unit {power_unit_id}: {e}"
            )
            raise

    def _create_new_alert_only(self, bulk_alert: Dict, power_unit_id: int) -> None:
        """
        Create an alert only if one doesn't already exist for this user and power unit.
        This is used when update_existing_alerts is False.

        Args:
            bulk_alert: The alerts_bulk record with alert settings
            power_unit_id: The power unit ID to create alert for
        """
        try:
            # Check if alert already exists
            sql = """
                SELECT id FROM public.alerts 
                WHERE user_id = %s AND power_unit_id = %s
            """
            _, existing = run_query(
                sql,
                db="ijack",
                fetchall=True,
                data=(bulk_alert["user_id"], power_unit_id),
            )

            if existing:
                # Alert already exists, skip creating new one
                logger.debug(
                    f"Skipping existing alert for user {bulk_alert['user_id']}, power unit {power_unit_id}"
                )
                return

            # Create new alert since none exists
            self._upsert_individual_alert(bulk_alert, power_unit_id)

        except Exception as e:
            logger.error(
                f"Error creating new alert for user {bulk_alert['user_id']}, power unit {power_unit_id}: {e}"
            )
            raise

    def _batch_upsert_alerts(self, bulk_alert: Dict, power_unit_ids: List[int]) -> None:
        """
        Batch insert or update multiple alert records for power units.
        Processes up to 500 power units in a single database operation.

        Args:
            bulk_alert: The alerts_bulk record with alert settings
            power_unit_ids: List of power unit IDs to create/update alerts for
        """
        if not power_unit_ids:
            return

        # Process in batches to avoid query size limits
        batch_size = 500
        for batch_start in range(0, len(power_unit_ids), batch_size):
            batch_end = min(batch_start + batch_size, len(power_unit_ids))
            batch_ids = power_unit_ids[batch_start:batch_end]

            # Build values for batch insert
            values_list = []
            params = []
            for power_unit_id in batch_ids:
                # Add all the values for this power unit
                values_list.append(
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                )
                params.extend(
                    [
                        bulk_alert["user_id"],
                        power_unit_id,
                        utcnow_naive(),
                        # Delivery preferences
                        bulk_alert.get("wants_sms", True),
                        bulk_alert.get("wants_email", False),
                        bulk_alert.get("wants_phone", False),
                        bulk_alert.get("wants_short_sms", False),
                        bulk_alert.get("wants_short_email", False),
                        bulk_alert.get("wants_short_phone", True),
                        bulk_alert.get("wants_whatsapp", False),
                        # Regular alerts
                        bulk_alert.get("heartbeat", True),
                        bulk_alert.get("online_hb", False),
                        bulk_alert.get("warn1", False),
                        bulk_alert.get("warn2", False),
                        bulk_alert.get("suction", False),
                        bulk_alert.get("discharge", False),
                        bulk_alert.get("mtr", False),
                        bulk_alert.get("spm", False),
                        bulk_alert.get("stboxf", False),
                        bulk_alert.get("hyd_temp", False),
                        bulk_alert.get("wants_card_ml", False),
                        # Change detection alerts
                        bulk_alert.get("change_suction", True),
                        bulk_alert.get("change_hyd_temp", False),
                        bulk_alert.get("change_dgp", True),
                        bulk_alert.get("change_hp_delta", True),
                        # Hydraulic oil alerts
                        bulk_alert.get("hyd_oil_lvl", False),
                        bulk_alert.get("hyd_filt_life", False),
                        bulk_alert.get("hyd_oil_life", False),
                        # Other alerts
                        bulk_alert.get("chk_mtr_ovld", False),
                        bulk_alert.get("pwr_fail", False),
                        bulk_alert.get("soft_start_err", False),
                        bulk_alert.get("grey_wire_err", False),
                        bulk_alert.get("ae011", False),
                    ]
                )

            # Build the batch SQL query
            sql = f"""
                INSERT INTO public.alerts (
                    user_id, power_unit_id, timestamp_utc_inserted,
                    wants_sms, wants_email, wants_phone, wants_short_sms, 
                    wants_short_email, wants_short_phone, wants_whatsapp,
                    heartbeat, online_hb, warn1, warn2, suction, discharge, 
                    mtr, spm, stboxf, hyd_temp, wants_card_ml,
                    change_suction, change_hyd_temp, change_dgp, change_hp_delta,
                    hyd_oil_lvl, hyd_filt_life, hyd_oil_life,
                    chk_mtr_ovld, pwr_fail, soft_start_err, grey_wire_err, ae011
                )
                VALUES {", ".join(values_list)}
                ON CONFLICT (user_id, power_unit_id) 
                DO UPDATE SET 
                    wants_sms = EXCLUDED.wants_sms,
                    wants_email = EXCLUDED.wants_email,
                    wants_phone = EXCLUDED.wants_phone,
                    wants_short_sms = EXCLUDED.wants_short_sms,
                    wants_short_email = EXCLUDED.wants_short_email,
                    wants_short_phone = EXCLUDED.wants_short_phone,
                    wants_whatsapp = EXCLUDED.wants_whatsapp,
                    heartbeat = EXCLUDED.heartbeat,
                    online_hb = EXCLUDED.online_hb,
                    warn1 = EXCLUDED.warn1,
                    warn2 = EXCLUDED.warn2,
                    suction = EXCLUDED.suction,
                    discharge = EXCLUDED.discharge,
                    mtr = EXCLUDED.mtr,
                    spm = EXCLUDED.spm,
                    stboxf = EXCLUDED.stboxf,
                    hyd_temp = EXCLUDED.hyd_temp,
                    wants_card_ml = EXCLUDED.wants_card_ml,
                    change_suction = EXCLUDED.change_suction,
                    change_hyd_temp = EXCLUDED.change_hyd_temp,
                    change_dgp = EXCLUDED.change_dgp,
                    change_hp_delta = EXCLUDED.change_hp_delta,
                    hyd_oil_lvl = EXCLUDED.hyd_oil_lvl,
                    hyd_filt_life = EXCLUDED.hyd_filt_life,
                    hyd_oil_life = EXCLUDED.hyd_oil_life,
                    chk_mtr_ovld = EXCLUDED.chk_mtr_ovld,
                    pwr_fail = EXCLUDED.pwr_fail,
                    soft_start_err = EXCLUDED.soft_start_err,
                    grey_wire_err = EXCLUDED.grey_wire_err,
                    ae011 = EXCLUDED.ae011
                RETURNING power_unit_id, (xmax = 0) AS inserted
            """

            try:
                _, results = run_query(
                    sql,
                    db="ijack",
                    fetchall=True,
                    commit=True,
                    data=tuple(params),
                    log_query=False,
                )

                # Update statistics based on results
                for result in results:
                    if result["inserted"]:
                        self.stats["alerts_inserted"] += 1
                    else:
                        self.stats["alerts_updated"] += 1

                logger.info(
                    f"Batch processed {len(batch_ids)} alerts for user {bulk_alert['user_id']} "
                    f"(batch {batch_start // batch_size + 1})"
                )

            except Exception as e:
                logger.error(
                    f"Error batch upserting alerts for user {bulk_alert['user_id']}: {e}"
                )
                raise


@error_wrapper(filename=Path(__file__).name)
def main(c: Config) -> Dict[str, int]:
    """
    CLI entry point for scheduled job execution.

    This function can be called from a cron job or other scheduler.

    Args:
        c: Configuration object

    Returns:
        Dictionary with processing statistics
    """
    logger.info("Starting bulk alert processing job via main() function")

    try:
        # Create processor and run
        processor = AlertBulkProcessor(c)
        results: Dict[str, int] = processor.process_all_bulk_alerts()

        # Log final results
        logger.info(f"Bulk alert processing completed: {results}")

        return results

    except Exception as e:
        logger.error(f"Critical error in bulk alert processing main(): {e}")
        raise


if __name__ == "__main__":
    # Allow running directly as a script
    c = Config()
    main(c)
