# # Load the secret environment variables using python-dotenv
# from dotenv import load_dotenv
# load_dotenv()

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg2

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

from project.utils import (
    Config,
    get_conn,
    get_resilient_conn,
    is_connection_alive,
    send_error_messages,
)

LOGFILE_NAME = "test_time_series_update_views"

c = Config()
c.DEV_TEST_PRD = "development"


class TestAll(unittest.TestCase):
    # executed prior to each test below, not just when the class is initialized
    def setUp(self):
        global c
        c.DEV_TEST_PRD = "development"
        c.TEST_FUNC = True

    @patch("project.utils.send_twilio_sms")
    @patch("project.utils.send_mailgun_email")
    def test_send_error_messages(
        self,
        mock_send_mailgun_email,
        mock_send_twilio_sms,
    ):
        """Test the send_error_messages() function"""

        global c
        err = Exception("This is an error message")
        filename = Path(__file__).name

        rv = send_error_messages(
            c=c, err=err, filename=filename, want_email=True, want_sms=True
        )

        self.assertIsNone(rv)
        mock_send_mailgun_email.assert_called_once()
        mock_send_twilio_sms.assert_called_once()


class TestIsConnectionAlive(unittest.TestCase):
    """Tests for the is_connection_alive() function."""

    def test_returns_false_when_connection_closed(self):
        """Test that is_connection_alive returns False when conn.closed is True."""
        mock_conn = MagicMock()
        mock_conn.closed = True

        result = is_connection_alive(mock_conn)

        self.assertFalse(result)

    def test_returns_true_when_connection_responsive(self):
        """Test that is_connection_alive returns True when SELECT 1 succeeds."""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        result = is_connection_alive(mock_conn)

        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    def test_returns_false_when_query_fails(self):
        """Test that is_connection_alive returns False when SELECT 1 raises exception."""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_conn.cursor.return_value.__enter__.side_effect = Exception(
            "Connection lost"
        )

        result = is_connection_alive(mock_conn)

        self.assertFalse(result)


class TestGetConn(unittest.TestCase):
    """Tests for the get_conn() context manager."""

    @patch("project.utils.psycopg2.connect")
    @patch.dict(
        "os.environ",
        {
            "HOST_IJ": "localhost",
            "PORT_IJ": "5432",
            "DB_IJ": "test_db",
            "USER_IJ": "test_user",
            "PASS_IJ": "test_pass",
        },
    )
    def test_handles_closed_connection_on_rollback(self, mock_connect):
        """Test that get_conn handles connection-already-closed gracefully."""
        mock_conn = MagicMock()
        mock_conn.closed = True  # Simulate already-closed connection
        mock_connect.return_value = mock_conn

        with self.assertRaises(ValueError):
            with get_conn():
                raise ValueError("Test error")

        # rollback should NOT be called if connection is closed
        mock_conn.rollback.assert_not_called()

    @patch("project.utils.psycopg2.connect")
    @patch.dict(
        "os.environ",
        {
            "HOST_IJ": "localhost",
            "PORT_IJ": "5432",
            "DB_IJ": "test_db",
            "USER_IJ": "test_user",
            "PASS_IJ": "test_pass",
        },
    )
    def test_calls_rollback_when_connection_open(self, mock_connect):
        """Test that get_conn calls rollback when connection is still open."""
        mock_conn = MagicMock()
        mock_conn.closed = False  # Connection is still open
        mock_connect.return_value = mock_conn

        with self.assertRaises(ValueError):
            with get_conn():
                raise ValueError("Test error")

        # rollback SHOULD be called if connection is open
        mock_conn.rollback.assert_called_once()

    @patch("project.utils.psycopg2.connect")
    @patch.dict(
        "os.environ",
        {
            "HOST_IJ": "localhost",
            "PORT_IJ": "5432",
            "DB_IJ": "test_db",
            "USER_IJ": "test_user",
            "PASS_IJ": "test_pass",
        },
    )
    def test_sets_sslmode_require_for_aws_rds(self, mock_connect):
        """Test that sslmode=require is set for AWS RDS connections."""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_connect.return_value = mock_conn

        with get_conn(db="aws_rds"):
            pass

        # Check sslmode was passed
        call_kwargs = mock_connect.call_args.kwargs
        self.assertEqual(call_kwargs.get("sslmode"), "require")

    @patch("project.utils.psycopg2.connect")
    @patch.dict(
        "os.environ",
        {
            "HOST_TS": "localhost",
            "PORT_TS": "5432",
            "DB_TS": "test_db",
            "USER_TS": "test_user",
            "PASS_TS": "test_pass",
        },
    )
    def test_sets_sslmode_prefer_for_timescale(self, mock_connect):
        """Test that sslmode=prefer is set for TimescaleDB connections."""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_connect.return_value = mock_conn

        with get_conn(db="timescale"):
            pass

        # Check sslmode was passed
        call_kwargs = mock_connect.call_args.kwargs
        self.assertEqual(call_kwargs.get("sslmode"), "prefer")


class TestGetResilientConn(unittest.TestCase):
    """Tests for the get_resilient_conn() context manager."""

    @patch("project.utils.time.sleep")
    @patch("project.utils.get_conn")
    def test_retries_on_ssl_connection_error(self, mock_get_conn, mock_sleep):
        """Test that resilient connection retries on SSL errors."""
        # First call fails with SSL error, second succeeds
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Simulate: first call raises SSL error, second call succeeds
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise psycopg2.OperationalError(
                    "SSL connection has been closed unexpectedly"
                )
            # Return a context manager that yields mock_conn
            from contextlib import contextmanager

            @contextmanager
            def mock_cm():
                yield mock_conn

            return mock_cm()

        mock_get_conn.side_effect = side_effect

        with get_resilient_conn(
            db="aws_rds", max_retries=2, retry_delay_base=0.01
        ) as conn:
            self.assertIsNotNone(conn)

        # Should have tried twice (once failed, once succeeded)
        self.assertEqual(call_count, 2)
        # Should have slept once (after first failure)
        mock_sleep.assert_called_once()

    @patch("project.utils.get_conn")
    def test_raises_after_max_retries_exceeded(self, mock_get_conn):
        """Test that exception is raised after max retries exceeded."""
        mock_get_conn.side_effect = psycopg2.OperationalError("connection refused")

        with self.assertRaises(psycopg2.OperationalError):
            with get_resilient_conn(
                db="aws_rds", max_retries=2, retry_delay_base=0.001
            ):
                pass

        # Should have tried max_retries + 1 times (0, 1, 2 = 3 attempts)
        self.assertEqual(mock_get_conn.call_count, 3)

    @patch("project.utils.get_conn")
    def test_does_not_retry_on_non_connection_error(self, mock_get_conn):
        """Test that non-connection errors are not retried."""
        mock_get_conn.side_effect = psycopg2.OperationalError("syntax error")

        with self.assertRaises(psycopg2.OperationalError):
            with get_resilient_conn(
                db="aws_rds", max_retries=2, retry_delay_base=0.001
            ):
                pass

        # Should have only tried once (no retry for syntax errors)
        self.assertEqual(mock_get_conn.call_count, 1)


if __name__ == "__main__":
    unittest.main()
