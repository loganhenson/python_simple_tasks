import unittest
from unittest.mock import patch, MagicMock
from python_simple_tasks.cli import main


class TestCLI(unittest.TestCase):
    @patch("python_simple_tasks.cli.create_tasks_table")
    def test_setup_tables(self, mock_create_tasks_table):
        """Test the --setup-tables CLI option."""
        with patch("sys.argv", ["pst", "--setup-tables"]):
            main()
            mock_create_tasks_table.assert_called_once()

    @patch("python_simple_tasks.cli.process_and_watch_tasks")
    def test_process_tasks_once(self, mock_process_and_watch_tasks):
        """Test processing tasks once."""
        with patch("sys.argv", ["pst"]):
            main()
            mock_process_and_watch_tasks.assert_called_once_with(watch=False, interval=10)

    @patch("python_simple_tasks.cli.process_and_watch_tasks")
    def test_watch_tasks(self, mock_process_and_watch_tasks):
        """Test the --watch CLI option."""
        with patch("sys.argv", ["pst", "--watch"]):
            main()
            mock_process_and_watch_tasks.assert_called_once_with(watch=True, interval=10)

    @patch("python_simple_tasks.cli.process_and_watch_tasks")
    def test_watch_tasks_with_custom_interval(self, mock_process_and_watch_tasks):
        """Test the --watch CLI option with a custom interval."""
        with patch("sys.argv", ["pst", "--watch", "--interval", "5"]):
            main()
            mock_process_and_watch_tasks.assert_called_once_with(watch=True, interval=5)


if __name__ == "__main__":
    unittest.main()
