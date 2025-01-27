import unittest
import subprocess
from datetime import datetime
from python_simple_tasks.scheduler import queue_task, process_tasks, connect_to_db


class TestLambdaTasks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the tasks table using the CLI."""
        subprocess.run(["pst", "--setup-tables"], check=True)

    @classmethod
    def tearDownClass(cls):
        """Drop the tasks table after tests."""
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS tasks;")
        conn.commit()
        cursor.close()
        conn.close()

    def test_valid_lambda_with_defaults(self):
        """Test a valid lambda with default arguments."""
        task_lambda = lambda x=5: x * 2

        queue_task(
            name="valid_lambda_with_defaults",
            scheduled_time=datetime.now(),
            function=task_lambda
        )

        process_tasks()

        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT status, output FROM tasks WHERE name = %s;", ("valid_lambda_with_defaults",))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        self.assertIsNotNone(result, "Task result is None. The task was not queued or processed correctly.")
        self.assertEqual(result[0], "success")
        self.assertEqual(result[1], "10")  # x=5 * 2

    def test_lambda_with_no_arguments(self):
        """Test a lambda with no arguments or closures."""
        task_lambda = lambda: "No arguments"

        queue_task(
            name="lambda_with_no_arguments",
            scheduled_time=datetime.now(),
            function=task_lambda
        )

        process_tasks()

        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT status, output FROM tasks WHERE name = %s;", ("lambda_with_no_arguments",))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        self.assertIsNotNone(result, "Task result is None. The task was not queued or processed correctly.")
        self.assertEqual(result[0], "success")
        self.assertEqual(result[1], "No arguments")

    def test_lambda_with_captured_variables(self):
        """Test a lambda with captured variables."""
        base = 10
        task_lambda = lambda x=5: x + base

        queue_task(
            name="lambda_with_captured_variables",
            scheduled_time=datetime.now(),
            function=task_lambda
        )

        process_tasks()

        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT status, output FROM tasks WHERE name = %s;", ("lambda_with_captured_variables",))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        self.assertIsNotNone(result, "Task result is None. The task was not queued or processed correctly.")
        self.assertEqual(result[0], "success")
        self.assertEqual(result[1], "15")  # x=5 + base=10

    def test_invalid_lambda(self):
        """Test an invalid lambda (non-callable)."""
        with self.assertRaises(ValueError):
            queue_task(
                name="invalid_lambda",
                scheduled_time=datetime.now(),
                function="not_a_function"
            )

    def test_large_closure(self):
        """Test a lambda with a large captured variable."""
        large_data = "x" * 10**6  # 1 MB of data
        task_lambda = lambda: f"Length of data: {len(large_data)}"

        queue_task(
            name="large_closure",
            scheduled_time=datetime.now(),
            function=task_lambda
        )

        process_tasks()

        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT status, output FROM tasks WHERE name = %s;", ("large_closure",))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        self.assertIsNotNone(result, "Task result is None. The task was not queued or processed correctly.")
        self.assertEqual(result[0], "success")
        self.assertEqual(result[1], "Length of data: 1000000")

    def test_conflicting_arguments(self):
        """Test a lambda with default arguments and provided arguments."""
        task_lambda = lambda x=5, y=10: x + y

        queue_task(
            name="conflicting_arguments",
            scheduled_time=datetime.now(),
            function=task_lambda
        )

        process_tasks()

        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT status, output FROM tasks WHERE name = %s;", ("conflicting_arguments",))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        self.assertIsNotNone(result, "Task result is None. The task was not queued or processed correctly.")
        self.assertEqual(result[0], "success")
        self.assertEqual(result[1], "15")  # x=5 + y=10


if __name__ == "__main__":
    unittest.main()
