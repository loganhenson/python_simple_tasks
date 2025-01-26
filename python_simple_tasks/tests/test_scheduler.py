import unittest
from datetime import datetime, timezone, timedelta
from python_simple_tasks.scheduler import create_tasks_table, process_tasks, connect_to_db


class TestSchedulerWithDB(unittest.TestCase):
    def setUp(self):
        """Set up the test database and create the tasks table."""
        create_tasks_table()
        self.conn = connect_to_db()
        self.cursor = self.conn.cursor()

    def tearDown(self):
        """Clean up the test database."""
        self.cursor.execute("DROP TABLE IF EXISTS tasks;")
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def test_create_tasks_table(self):
        """Verify that the tasks table is created successfully."""
        self.cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'tasks';
            """
        )
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "tasks")

    def test_process_tasks_no_due_tasks(self):
        """Test processing when no tasks are due."""
        # Ensure the table is empty
        self.cursor.execute("SELECT COUNT(*) FROM tasks;")
        count = self.cursor.fetchone()[0]
        self.assertEqual(count, 0)

        # Run the process tasks function
        process_tasks()

        # Verify no changes to the database
        self.cursor.execute("SELECT COUNT(*) FROM tasks;")
        count_after = self.cursor.fetchone()[0]
        self.assertEqual(count_after, 0)

    def test_process_due_task(self):
        """Test processing a single due task."""
        # Insert a task into the database
        now = datetime.now(timezone.utc)
        self.cursor.execute(
            """
            INSERT INTO tasks (name, scheduled_time, completed, in_progress, args)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (
                "test_task",
                now - timedelta(minutes=1),  # Due now
                False,
                False,
                '{"function": "lambda: 42"}',
            ),
        )
        self.conn.commit()

        # Run the process tasks function
        process_tasks()

        # Verify the task is marked as completed
        self.cursor.execute("SELECT completed, status, output FROM tasks WHERE name = %s;", ("test_task",))
        result = self.cursor.fetchone()
        self.assertTrue(result[0])  # completed = True
        self.assertEqual(result[1], "success")  # status = success
        self.assertEqual(result[2], "42")  # output = 42

    def test_process_failed_task(self):
        """Test processing a task that raises an exception."""
        from datetime import datetime, timedelta
        import json

        # Insert a task that will fail
        now = datetime.now(timezone.utc)
        self.cursor.execute(
            """
            INSERT INTO tasks (name, scheduled_time, completed, in_progress, args)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (
                "failing_task",
                now - timedelta(minutes=1),  # Due now
                False,
                False,
                json.dumps({"function": "lambda: 1/0"}),  # Serialize as JSON
            ),
        )
        self.conn.commit()

        # Run the process tasks function
        process_tasks()

        # Verify the task is marked as failed
        self.cursor.execute("SELECT completed, status, output FROM tasks WHERE name = %s;", ("failing_task",))
        result = self.cursor.fetchone()
        self.assertFalse(result[0])  # completed = False
        self.assertEqual(result[1], "failure")  # status = failure
        self.assertIn("division by zero", result[2])  # output contains error


if __name__ == "__main__":
    unittest.main()
