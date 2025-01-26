import psycopg2
from python_simple_tasks.utils import load_settings
import json
from datetime import datetime, timezone



def connect_to_db():
    """
    Connect to the database using the DATABASES configuration from settings.py.
    Gracefully fail if the configuration is missing or incorrect.
    """
    try:
        settings = load_settings()
    except (FileNotFoundError, ValueError) as e:
        raise RuntimeError(f"Failed to load settings.py: {e}")

    db = settings.DATABASES["default"]

    return psycopg2.connect(
        dbname=db["NAME"],
        user=db["USER"],
        password=db["PASSWORD"],
        host=db.get("HOST", "localhost"),
        port=db.get("PORT", 5432),
    )


def create_tasks_table():
    """Create the tasks table if it doesn't already exist."""
    conn = connect_to_db()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        scheduled_time TIMESTAMP NOT NULL,
        completed BOOLEAN DEFAULT FALSE,
        in_progress BOOLEAN DEFAULT FALSE,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        output TEXT,
        status TEXT,
        args JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()


def process_tasks():
    """Process all due tasks."""
    conn = connect_to_db()
    cursor = conn.cursor()

    # Fetch all due tasks
    cursor.execute(
        "SELECT * FROM tasks WHERE completed = FALSE AND in_progress = FALSE AND scheduled_time <= %s",
        [datetime.now(timezone.utc)],
    )
    tasks = cursor.fetchall()

    for task in tasks:
        task_id, name, scheduled_time, completed, in_progress, start_time, end_time, output, status, args, created_at = task

        # Mark as in progress and set start_time
        cursor.execute(
            "UPDATE tasks SET in_progress = TRUE, start_time = %s WHERE id = %s",
            [datetime.now(timezone.utc), task_id],
        )
        conn.commit()

        try:
            # Deserialize arguments (ensure args is a valid JSON string)
            if isinstance(args, str):
                task_args = json.loads(args)  # Deserialize JSON string
            elif isinstance(args, dict):
                task_args = args  # Already a dictionary
            else:
                raise ValueError("Invalid args type: expected str or dict.")

            # Extract and execute the function
            function = eval(task_args.pop("function"))
            function_output = function(**task_args)

            # Mark task as completed
            cursor.execute(
                """
                UPDATE tasks
                SET completed = TRUE, in_progress = FALSE, end_time = %s, output = %s, status = %s
                WHERE id = %s
                """,
                [datetime.now(timezone.utc), str(function_output), "success", task_id],
            )
        except Exception as e:
            # Record failure with error message
            cursor.execute(
                """
                UPDATE tasks
                SET in_progress = FALSE, end_time = %s, output = %s, status = %s
                WHERE id = %s
                """,
                [datetime.now(timezone.utc), f"Error: {e}", "failure", task_id],
            )

        conn.commit()

    cursor.close()
    conn.close()