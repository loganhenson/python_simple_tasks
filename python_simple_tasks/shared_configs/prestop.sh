#!/bin/bash
echo "Running prestop hook: Waiting for tasks to complete..."

DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/python_simple_tasks_test}"
IN_PROGRESS_COUNT=$(psql $DATABASE_URL -t -c "SELECT COUNT(*) FROM tasks WHERE in_progress = TRUE")

# Wait until all in-progress tasks are completed
while [ "$IN_PROGRESS_COUNT" -gt 0 ]; do
  echo "Tasks still in progress: $IN_PROGRESS_COUNT. Waiting..."
  sleep 5
  IN_PROGRESS_COUNT=$(psql $DATABASE_URL -t -c "SELECT COUNT(*) FROM tasks WHERE in_progress = TRUE")
done

echo "All tasks completed. Proceeding with instance shutdown."
