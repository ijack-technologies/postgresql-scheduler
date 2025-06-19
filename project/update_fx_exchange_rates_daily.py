#!/usr/bin/env python3
"""
Bank of Canada Historical Currency Rate Updater

Downloads FREE historical USD/CAD exchange rates from Bank of Canada's Valet API
and populates the currencies_rates table.

Usage:
    # Get last 30 days
    python bank_of_canada_historical_updater.py --days-back 30

    # Get specific date range
    python bank_of_canada_historical_updater.py --start-date 2020-01-01 --end-date 2024-12-31

    # Get all available history (goes back to 1950s)
    python bank_of_canada_historical_updater.py --all-history

API: Bank of Canada Valet API (100% FREE, no API key required)
Endpoint: https://www.bankofcanada.ca/valet/observations/FXUSDCAD/csv
Data: Daily USD/CAD rates back to ~1950
"""

import argparse
import csv
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import requests
from sqlalchemy import Column, Date, ForeignKey, Integer, Numeric, create_engine
from sqlalchemy.dialects.postgresql import TEXT, VARCHAR
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from project.utils import (
    Config,
    error_wrapper,
    exit_if_already_running,
)

# Create a minimal base for our models
Base = declarative_base()


# Define minimal models needed for currency operations
class Currency(Base):
    """Minimal Currency model for currency rate operations"""

    __tablename__ = "currencies"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True)
    name = Column(VARCHAR, nullable=False, unique=True)
    description = Column(TEXT, nullable=True)


class CurrencyRate(Base):
    """Minimal CurrencyRate model for storing historical rates"""

    __tablename__ = "currencies_rates"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True)
    currency_id = Column(Integer, ForeignKey("public.currencies.id"), nullable=False)
    rate_date = Column(Date, nullable=False)
    fx_rate_cad_per = Column(Numeric(10, 6), nullable=False)
    source = Column(VARCHAR(50), nullable=False, default="bankofcanada.ca")


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BankOfCanadaAPI:
    """Handles API communication with Bank of Canada Valet API"""

    BASE_URL = "https://www.bankofcanada.ca/valet/observations"

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "IJack-Currency-Updater/1.0 (contact@myijack.com)"}
        )

    def get_usd_cad_rates(
        self, start_date: date, end_date: date
    ) -> List[Tuple[date, float]]:
        """
        Fetch USD/CAD historical exchange rates from Bank of Canada.

        Args:
            start_date: Start date for data
            end_date: End date for data

        Returns:
            List of (date, rate) tuples
        """
        url = f"{self.BASE_URL}/FXUSDCAD/csv"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        try:
            logger.info(f"Fetching USD/CAD rates from {start_date} to {end_date}")
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            # Parse CSV response
            rates = []
            csv_data = response.text

            # Skip headers and find the OBSERVATIONS section
            lines = csv_data.split("\n")
            observations_started = False

            for line in lines:
                if line.strip() == '"OBSERVATIONS"':
                    observations_started = True
                    continue
                elif line.strip().startswith('"date","FXUSDCAD"'):
                    continue  # Skip column headers
                elif observations_started and line.strip():
                    # Parse data line: "2024-01-02","1.3316"
                    reader = csv.reader([line])
                    row = next(reader)
                    if len(row) == 2:
                        try:
                            rate_date = datetime.strptime(row[0], "%Y-%m-%d").date()
                            rate_value = float(row[1])
                            rates.append((rate_date, rate_value))
                        except (ValueError, IndexError) as e:
                            logger.warning(
                                f"Skipping invalid row: {line.strip()} - {e}"
                            )

            logger.info(f"Retrieved {len(rates)} exchange rates")
            return rates

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch rates: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return []


class CurrencyRateManager:
    """Handles database operations for currency rates"""

    def __init__(self, session: Session):
        self.session = session

    def get_currency_id(self, currency_code: str) -> Optional[int]:
        """Get currency ID by code (handles emoji currency names)"""
        # First try exact match
        currency = self.session.query(Currency).filter_by(name=currency_code).first()
        if currency:
            return currency.id

        # If not found, try partial match (handles "CAD 🍁" and "USD 🦅")
        currency = (
            self.session.query(Currency)
            .filter(Currency.name.like(f"{currency_code}%"))
            .first()
        )
        return currency.id if currency else None

    def store_rates(
        self,
        currency_id: int,
        rates: List[Tuple[date, float]],
        source: str = "bankofcanada.ca",
    ) -> Tuple[int, int]:
        """
        Store multiple currency rates in the database.

        Args:
            currency_id: ID of the currency
            rates: List of (date, rate) tuples
            source: Data source name

        Returns:
            Tuple of (inserted_count, updated_count)
        """
        inserted = 0
        updated = 0

        for rate_date, rate_value in rates:
            try:
                # Check if rate already exists
                existing = (
                    self.session.query(CurrencyRate)
                    .filter_by(currency_id=currency_id, rate_date=rate_date)
                    .first()
                )

                if existing:
                    # Update existing rate
                    existing.fx_rate_cad_per = rate_value
                    existing.source = source
                    updated += 1
                    logger.debug(f"Updated rate for {rate_date}: {rate_value}")
                else:
                    # Create new rate
                    new_rate = CurrencyRate(
                        currency_id=currency_id,
                        rate_date=rate_date,
                        fx_rate_cad_per=rate_value,
                        source=source,
                    )
                    self.session.add(new_rate)
                    inserted += 1
                    logger.debug(f"Added rate for {rate_date}: {rate_value}")

                # Commit in batches for better performance
                if (inserted + updated) % 100 == 0:
                    self.session.commit()
                    logger.info(f"Processed {inserted + updated} rates...")

            except IntegrityError as e:
                logger.error(f"Database integrity error for {rate_date}: {e}")
                self.session.rollback()
            except Exception as e:
                logger.error(f"Unexpected error storing rate for {rate_date}: {e}")
                self.session.rollback()

        # Final commit
        try:
            self.session.commit()
            logger.info(
                f"Successfully stored rates: {inserted} inserted, {updated} updated"
            )
        except Exception as e:
            logger.error(f"Final commit failed: {e}")
            self.session.rollback()

        return inserted, updated

    def get_existing_date_range(
        self, currency_id: int
    ) -> Tuple[Optional[date], Optional[date]]:
        """Get the min and max dates for existing rates"""
        from sqlalchemy import func

        result = (
            self.session.query(
                func.min(CurrencyRate.rate_date), func.max(CurrencyRate.rate_date)
            )
            .filter_by(currency_id=currency_id)
            .first()
        )

        min_date = result[0] if result and result[0] else None
        max_date = result[1] if result and result[1] else None

        return min_date, max_date


def update_historical_rates(
    database_url: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    days_back: Optional[int] = None,
    all_history: bool = False,
) -> None:
    """
    Main function to update historical USD/CAD rates.

    Args:
        database_url: Database connection string
        start_date: Start date for updates
        end_date: End date for updates
        days_back: Number of days back from today
        all_history: Download all available history
    """
    # Calculate date range
    if all_history:
        start_date = date(1950, 1, 1)  # Bank of Canada has data from ~1950
        end_date = date.today()
    elif days_back:
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
    elif start_date is None:
        # Default to last 30 days
        end_date = date.today()
        start_date = end_date - timedelta(days=30)
    elif end_date is None:
        end_date = date.today()

    logger.info(f"Updating USD/CAD rates from {start_date} to {end_date}")

    # Initialize components
    engine = create_engine(database_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    api = BankOfCanadaAPI()
    manager = CurrencyRateManager(session)

    try:
        # Get USD currency ID
        usd_id = manager.get_currency_id("USD")
        if not usd_id:
            logger.error("USD currency not found in database. Please add it first.")
            return

        cad_id = manager.get_currency_id("CAD")
        if not cad_id:
            logger.error("CAD currency not found in database. Please add it first.")
            return

        logger.info(f"Found USD currency ID: {usd_id}, CAD currency ID: {cad_id}")

        # Show existing data range
        min_date, max_date = manager.get_existing_date_range(usd_id)
        if min_date and max_date:
            logger.info(f"Existing USD/CAD data: {min_date} to {max_date}")
        else:
            logger.info("No existing USD/CAD data found")

        # Fetch rates from Bank of Canada
        rates = api.get_usd_cad_rates(start_date, end_date)
        if not rates:
            logger.error("No rates retrieved from Bank of Canada")
            return

        # Store USD rates (these are CAD per USD)
        inserted, updated = manager.store_rates(usd_id, rates, "bankofcanada.ca")

        # Store CAD rates (always 1.0)
        cad_rates = [(rate_date, 1.0) for rate_date, _ in rates]
        cad_inserted, cad_updated = manager.store_rates(
            cad_id, cad_rates, "bankofcanada.ca"
        )

        logger.info(f"USD rates - Inserted: {inserted}, Updated: {updated}")
        logger.info(f"CAD rates - Inserted: {cad_inserted}, Updated: {cad_updated}")
        logger.info("Historical rate update completed successfully!")

    except Exception as e:
        logger.error(f"Fatal error during update: {e}")
        raise
    finally:
        session.close()


def get_sqla_conn_string(
    user: str, passw: str, db: str, host: str, port: int = 5432
) -> str:
    """Returns a SQLAlchemy connection string"""
    return f"postgresql+psycopg2://{user}:{passw}@{host}:{port}/{db}"


@error_wrapper(filename=Path(__file__).name)
def main(c: Config) -> None:
    """Command line interface"""

    exit_if_already_running(c, Path(__file__).name)

    parser = argparse.ArgumentParser(
        description="Update historical USD/CAD exchange rates from Bank of Canada"
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        help="Start date (YYYY-MM-DD)",
        default=date.today() - timedelta(days=14),
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        help="End date (YYYY-MM-DD)",
        default=date.today(),
    )
    parser.add_argument(
        "--days-back", type=int, help="Number of days back from today", default=None
    )
    parser.add_argument(
        "--all-history",
        action="store_true",
        help="Download all available history (1950+)",
        default=False,
    )
    parser.add_argument(
        "--database-url", help="Database URL (default: from environment)", default=None
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Verbose logging", default=False
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Get database URL
    database_url = args.database_url
    if not database_url:
        # Try DATABASE_URL environment variable first
        database_url = os.getenv("DATABASE_URL")

        # If not found, try to construct from Flask app config environment variables
        if not database_url:
            try:
                user_ij = os.getenv("USER_IJ")
                pass_ij = os.getenv("PASS_IJ")
                db_ij = os.getenv("DB_IJ")
                host_ij = os.getenv("HOST_IJ", "pgbouncer-rds")
                port_ij = int(os.getenv("PORT_IJ", 5432))

                if user_ij and pass_ij and db_ij:
                    database_url = get_sqla_conn_string(
                        user=user_ij,
                        passw=pass_ij,
                        db=db_ij,
                        host=host_ij,
                        port=port_ij,
                    )
                    logger.info(
                        "Using database URL from Flask app config environment variables"
                    )
                else:
                    logger.debug("Flask app config environment variables not found")
            except Exception as e:
                logger.debug(f"Could not construct database URL from Flask config: {e}")
                database_url = None

        if not database_url:
            logger.error(
                "Database URL not provided. Options:\n"
                "1. Use --database-url argument\n"
                "2. Set DATABASE_URL environment variable\n"
                "3. Set Flask app config variables: USER_IJ, PASS_IJ, DB_IJ, HOST_IJ, PORT_IJ"
            )
            sys.exit(1)

    try:
        update_historical_rates(
            database_url=database_url,
            start_date=args.start_date,
            end_date=args.end_date,
            days_back=args.days_back,
            all_history=args.all_history,
        )
    except Exception as e:
        logger.error(f"Update failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
