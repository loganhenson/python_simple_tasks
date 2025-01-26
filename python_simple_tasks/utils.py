import os
import importlib.util


def load_settings():
    """
    Dynamically load a settings.py file from the current directory or a child directory
    that matches the name of the current directory. Gracefully fail if the file is not found
    or if the DATABASES configuration is missing.
    """
    # Get the current working directory and its name
    current_dir = os.getcwd()
    current_dir_name = os.path.basename(current_dir)

    # Possible locations for settings.py
    possible_paths = [
        os.path.join(current_dir, "settings.py"),  # settings.py in the current directory
        os.path.join(current_dir, current_dir_name, "settings.py"),  # settings.py in a child directory
    ]

    # Find the first existing settings.py
    settings_path = next((path for path in possible_paths if os.path.exists(path)), None)

    if not settings_path:
        raise FileNotFoundError(
            f"settings.py not found in the current directory ({current_dir}) or in {current_dir_name}/."
        )

    # Dynamically load settings.py
    spec = importlib.util.spec_from_file_location("settings", settings_path)
    settings = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(settings)

    # Validate the existence of the DATABASES configuration
    if not hasattr(settings, "DATABASES"):
        raise ValueError(
            "The settings.py file is missing the DATABASES configuration. Please include a DATABASES dictionary."
        )

    # Validate that the default database is configured
    if "default" not in settings.DATABASES:
        raise ValueError(
            "The DATABASES configuration in settings.py must include a 'default' key with database connection details."
        )

    return settings


def setup_eb_settings(overwrite=False):
    """Generate Elastic Beanstalk (EB) settings for task processing."""
    eb_dir = ".ebextensions"
    cron_file = os.path.join(eb_dir, "cron.config")

    if not os.path.exists(eb_dir):
        os.makedirs(eb_dir)
        print(f"Created directory: {eb_dir}")
    elif os.path.exists(cron_file) and not overwrite:
        print(f"File {cron_file} already exists. Use --overwrite to regenerate.")
        return

    with open(cron_file, "w") as f:
        f.write(
            """files:
    "/etc/cron.d/python-simple-tasks":
        mode: "000644"
        owner: root
        group: root
        content: |
            * * * * * root /usr/bin/python3 /path/to/your/manage.py process_tasks
commands:
    remove_old_cron:
        command: "rm -f /etc/cron.d/python-simple-tasks.bak"
    restart_cron:
        command: "service crond restart"
"""
        )
    print(f"EB cron settings generated at: {cron_file}")


import inspect


def extract_lambda_args(func):
    """
    Extract arguments captured by a lambda function.
    Handles both free variables (from closures) and default arguments.

    :param func: The lambda function.
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

    # Extract free variables from the closure
    free_vars = (
        {var: cell.cell_contents for var, cell in zip(func.__code__.co_freevars, func.__closure__)}
        if func.__closure__
        else {}
    )

    # Merge default arguments and free variables
    return {**default_args, **free_vars}

