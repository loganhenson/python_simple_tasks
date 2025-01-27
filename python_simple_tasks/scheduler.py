import base64
import inspect
import json
import marshal
import types

import psycopg2
from psycopg2 import sql

from python_simple_tasks.utils import load_settings
from datetime import datetime


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
        scheduled_time TIMESTAMP WITH TIME ZONE NOT NULL,
        function_path TEXT NOT NULL,
        args JSONB,
        status TEXT DEFAULT 'pending',    -- Tracks task states: 'pending', 'in_progress', 'success', 'failure'
        output TEXT,                      -- Stores task output or error message
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()


def extract_callable_args(func):
    """
    Extract arguments captured by a callable (function or lambda).

    :param func: The callable function or lambda.
    :return: A dictionary of argument names and their values.
    """
    if not callable(func):
        raise TypeError("Provided function is not callable.")

    # Extract default arguments
    signature = inspect.signature(func)
    default_args = {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }

    # Extract free variables from the closure (if any)
    free_vars = (
        {var: cell.cell_contents for var, cell in zip(func.__code__.co_freevars, func.__closure__)}
        if func.__closure__
        else {}
    )

    # Merge default arguments and free variables
    return {**default_args, **free_vars}


def serialize_lambda(func):
    """
    Serialize a lambda function into a dictionary containing its code, closure, and default arguments.

    Args:
        func (callable): The lambda function to serialize.
    Returns:
        dict: Serialized representation of the lambda.
    """
    if not callable(func) or func.__name__ != "<lambda>":
        raise ValueError("Only lambda functions are supported.")

    # Capture default arguments
    signature = inspect.signature(func)
    default_args = {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }

    return {
        "code": base64.b64encode(marshal.dumps(func.__code__)).decode("utf-8"),
        "closure": [cell.cell_contents for cell in func.__closure__] if func.__closure__ else [],
        "default_args": default_args,
    }


def queue_task(name, scheduled_time, function):
    """
    Queue a lambda-based task in the database.

    Args:
        name (str): Name of the task.
        scheduled_time (datetime): Time the task should run.
        function (callable): A lambda function capturing its arguments.
    """
    # Serialize the lambda
    serialized_function = json.dumps(serialize_lambda(function))

    # Insert the task into the database
    conn = connect_to_db()
    cursor = conn.cursor()
    try:
        insert_query = sql.SQL("""
            INSERT INTO tasks (name, scheduled_time, function_path, args, status)
            VALUES (%s, %s, %s, %s, 'pending');
        """)
        cursor.execute(insert_query, (name, scheduled_time, serialized_function, json.dumps({})))
        conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def deserialize_lambda(serialized_function):
    """
    Deserialize a serialized lambda function.

    Args:
        serialized_function (str): Serialized representation of the lambda.
    Returns:
        callable: The deserialized lambda function.
    """
    function_data = json.loads(serialized_function)
    code = marshal.loads(base64.b64decode(function_data["code"]))
    closure = tuple(types.CellType(c) for c in function_data["closure"])
    default_args = function_data.get("default_args", {})

    # Rebuild the lambda with the correct defaults
    def wrapped_lambda(**kwargs):
        # Apply default arguments if not provided
        kwargs = {**default_args, **kwargs}
        return types.FunctionType(code, globals(), "<lambda>", None, closure)(**kwargs)

    return wrapped_lambda


def process_tasks():
    """
    Process all due tasks in the database.
    """
    conn = connect_to_db()
    cursor = conn.cursor()

    # Fetch all due tasks
    select_query = """
    SELECT id, name, function_path, args
    FROM tasks
    WHERE scheduled_time <= NOW() AND status = 'pending'
    ORDER BY scheduled_time ASC
    """
    cursor.execute(select_query)
    tasks = cursor.fetchall()

    for task in tasks:
        task_id, task_name, serialized_function, args_json = task

        try:
            # Deserialize the lambda
            function = deserialize_lambda(serialized_function)

            # Execute the lambda
            output = function()

            # Mark the task as completed successfully
            cursor.execute("""
                UPDATE tasks
                SET status = 'success', output = %s
                WHERE id = %s
            """, (output, task_id))
            conn.commit()

        except Exception as e:
            # Mark the task as failed
            cursor.execute("""
                UPDATE tasks
                SET status = 'failure', output = %s
                WHERE id = %s
            """, (str(e), task_id))
            conn.commit()
            print(f"Task '{task_name}' failed with error: {e}")

    cursor.close()
    conn.close()


__all__ = ["queue_task"]
