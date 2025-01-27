import os
import shutil


def setup_eb_settings():
    """
    Generate Elastic Beanstalk configuration by copying shared settings into `.ebextensions`.
    Always overwrites existing files for idempotence.
    """
    shared_configs_path = os.path.join(os.path.dirname(__file__), "shared_configs")
    ebextensions_path = ".ebextensions"

    # Ensure .ebextensions directory exists
    os.makedirs(ebextensions_path, exist_ok=True)

    # Copy cron settings
    cron_src = os.path.join(shared_configs_path, "cron_settings.txt")
    cron_dest = os.path.join(ebextensions_path, "cron.config")
    shutil.copyfile(cron_src, cron_dest)
    print(f"Cron configuration copied to {cron_dest}")

    # Copy prestop hook
    prestop_src = os.path.join(shared_configs_path, "prestop.sh")
    prestop_dest = os.path.join(ebextensions_path, "prestop.sh")
    shutil.copyfile(prestop_src, prestop_dest)
    os.chmod(prestop_dest, 0o755)
    print(f"Prestop hook copied to {prestop_dest}")

    print("Elastic Beanstalk configuration successfully generated!")
