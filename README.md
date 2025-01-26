# **Python Simple Tasks**

A lightweight task scheduling and processing system for Python, designed to be simple, efficient, and robust. `Python Simple Tasks` supports both one-time and recurring tasks, leveraging PostgreSQL for task management and offering Elastic Beanstalk integration for production environments.

---

## **Features**

### 🎯 **Core Features**
- **Dynamic Tasks**: Queue tasks dynamically at runtime with full argument support.
- **Recurring Tasks**: Schedule recurring tasks directly in `settings.py` with support for intervals (e.g., daily, weekly).
- **Task Status Tracking**: Monitor tasks with statuses like `pending`, `success`, or `failure`.
- **PostgreSQL Integration**: Reliable, scalable backend for task management.
- **Elastic Beanstalk Support**: Automatic cron job configuration for production environments.
- **CLI Integration**: Manage tasks with a straightforward command-line interface:
  - Process tasks once.
  - Run tasks in watch mode for continuous processing.
  - Set up database tables and Elastic Beanstalk configurations.

### ⚙️ **Production-Ready**
- **Database-Driven Management**: Tasks are stored in a PostgreSQL database with columns for scheduling, status, output, and more.
- **Elastic Beanstalk Integration**: A cron job is set up to process tasks in production environments.

### 🧑‍💻 **Developer-Friendly**
- **Local Debugging**: Watch mode simulates task processing locally for easy debugging.
- **Idempotent Operations**: Safe CLI commands for database and configuration setup to avoid redundant operations.

---

## **Setup**

### 1. **Install the Package**
Install `python-simple-tasks` using pip:
```bash
pip install python-simple-tasks
```

### 2. **Set Up Your PostgreSQL Database**
Ensure PostgreSQL is running and create a database for the tasks system (e.g., `python_simple_tasks`):
```bash
createdb python_simple_tasks
```

### 3. **Configure Your Application**
Add a `settings.py` file in your project root with the following database configuration:
```python
DATABASES = {
    "default": {
        "NAME": "python_simple_tasks",
        "USER": "postgres",
        "PASSWORD": "postgres",
        "HOST": "localhost",
        "PORT": 5432,
    }
}

# Define recurring tasks here
TASK_SCHEDULER_TASKS = [
    {
        "name": "send_weekly_report",
        "interval": {"days": 7},  # Run every 7 days
        "run_time": "09:00",  # UTC time
        "function": lambda: send_report(email="example@example.com", subject="Weekly Update"),
    },
    {
        "name": "cleanup_temp_files",
        "interval": {"days": 1},  # Run daily
        "run_time": "02:00",  # UTC time
        "function": lambda: cleanup(folder="/tmp", dry_run=False),
    },
]
```

### 4. **Set Up the Database Tables**
Run the CLI command to create the necessary database tables:
```bash
pst --setup-tables
```

---

## **Usage**

### **Run Tasks Once**
Process all due tasks one time:
```bash
pst
```

### **Watch Mode**
Continuously process tasks in watch mode:
```bash
pst --watch
```

Customize the interval between task processing cycles (default is 10 seconds):
```bash
pst --watch --interval 5
```

### **Elastic Beanstalk Configuration**
Generate Elastic Beanstalk settings for production environments:
```bash
pst --setup-eb
```

Optionally overwrite existing settings:
```bash
pst --setup-eb --overwrite
```

---

## **Examples**

### **Dynamically Queue a One-Time Task**
You can dynamically queue a one-time task at runtime:
```python
from datetime import datetime, timedelta
from python_simple_tasks.scheduler import queue_task

queue_task(
    name="send_custom_email",
    scheduled_time=datetime.now(tz=timezone.utc) + timedelta(minutes=10),
    function=lambda: send_report(email="user@example.com", subject="Custom Report"),
)
```

### **Define Recurring Tasks**
Recurring tasks can be defined in `settings.py`:
```python
TASK_SCHEDULER_TASKS = [
    {
        "name": "daily_cleanup",
        "interval": {"days": 1},
        "run_time": "00:00",  # Midnight UTC
        "function": lambda: cleanup(folder="/tmp", dry_run=False),
    }
]
```

### **Inspect the Task Table**
The `tasks` table tracks task status, timestamps, and results:

| **id** | **name**             | **scheduled_time**       | **completed** | **status** | **output**                       | **created_at**          |
|--------|-----------------------|--------------------------|---------------|------------|-----------------------------------|-------------------------|
| 1      | `send_weekly_report` | `2025-01-30 09:00:00`    | `TRUE`        | `success`  | "Weekly report sent successfully!" | `2025-01-23 08:00:00` |
| 2      | `cleanup_temp_files` | `2025-01-31 02:00:00`    | `FALSE`       | `failure`  | "Error: Folder not found"         | `2025-01-30 08:00:00` |


## **License**

This project is licensed under the MIT License.

