import argparse
from python_simple_tasks.scheduler import create_tasks_table, process_tasks
from python_simple_tasks.utils import setup_eb_settings


def process_and_watch_tasks(watch=False, interval=10):
    """Process tasks once or continuously in watch mode."""
    import time

    if watch:
        print("Starting task processing in watch mode...")
        try:
            while True:
                print("Processing tasks...")
                process_tasks()
                print(f"Waiting {interval} seconds before the next cycle...")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("Stopping watch mode.")
    else:
        print("Processing tasks once...")
        process_tasks()
        print("Tasks processed successfully.")


def main():
    parser = argparse.ArgumentParser(description="Python Simple Tasks CLI")
    parser.add_argument("--setup-tables", action="store_true", help="Create the required tasks table (idempotent)")
    parser.add_argument("--setup-eb", action="store_true", help="Generate EB settings for task processing (idempotent)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing EB settings")
    parser.add_argument("--watch", action="store_true", help="Run task processing in watch mode")
    parser.add_argument("--interval", type=int, default=10, help="Interval (in seconds) for watch mode")

    args = parser.parse_args()

    if args.setup_tables:
        create_tasks_table()
    elif args.setup_eb:
        setup_eb_settings(overwrite=args.overwrite)
    else:
        process_and_watch_tasks(watch=args.watch, interval=args.interval)
