"""
    1. Load environment variables from .env file and set them in the current session.
    2. Persist environment variables to Windows user environment registry for future sessions.
"""

import os
import subprocess
import sys
import winreg
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent
DBT_DIR = ROOT_DIR / "dbt_project" / "health_pipeline"
DBT_EXECUTABLE = r"C:\Users\Darvin\miniconda3\envs\health-pipeline\Scripts\dbt.EXE"


def load_env() -> dict:
    """Load environment variables from .env file."""
    env_path = ROOT_DIR / ".env"
    env_vars = os.environ.copy()

    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                os.environ[key] = value
                env_vars[key] = value

    print("✓ Environment variables loaded")
    return env_vars

# only needed if your system doesnt recognise env variables
def persist_env_vars(env_vars: dict):
    """Persist environment variables to Windows user environment registry.
    Only needs to run once — vars will be available in all future terminal sessions.
    """
    env_keys = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_ROLE', 'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE_DEV',
        'SNOWFLAKE_DATABASE_PROD', 'GOOGLE_SHEET_NAME', 'GCP_SERVICE_ACCOUNT_PATH'
    ]

    key = winreg.OpenKey(
        winreg.HKEY_CURRENT_USER,
        'Environment',
        0,
        winreg.KEY_SET_VALUE
    )

    for env_key in env_keys:
        if env_key in env_vars:
            winreg.SetValueEx(key, env_key, 0, winreg.REG_SZ, env_vars[env_key])

    winreg.CloseKey(key)
    print("✓ Environment variables persisted to user environment")
    print("  Open a new terminal after setup for changes to take effect")


def run_command(command, cwd=None, description="", env=None):
    """Run a shell command and log output."""
    print(f"\n>>> {description}")
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        env=env or os.environ.copy()
    )
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print(f"ERROR: {result.stderr}")
        sys.exit(1)
    print(f"✓ {description} completed")


def main():
    print("=" * 50)
    print("Health Data Pipeline — Setup")
    print("=" * 50)

    # Step 1 — Load env vars
    env_vars = load_env()

    # Step 2 — Persist env vars to Windows user environment
    persist_env_vars(env_vars)

    # Step 3 — Install dependencies
    run_command(
        [sys.executable, "-m", "pip", "install", "-r", str(ROOT_DIR / "requirements.txt")],
        cwd=ROOT_DIR,
        description="Installing dependencies",
        env=env_vars
    )

    # Step 4 — Initialise Snowflake databases and schemas
    run_command(
        [sys.executable, str(ROOT_DIR / "scripts" / "init_db.py")],
        cwd=ROOT_DIR,
        description="Initialising Snowflake databases and schemas",
        env=env_vars
    )

    # Step 5 — Install dbt packages
    run_command(
        [DBT_EXECUTABLE, "deps"],
        cwd=DBT_DIR,
        description="Installing dbt packages",
        env=env_vars
    )

    print("\n" + "=" * 50)
    print("Setup complete! Open a new terminal then run:")
    print("  python ingestion/load_data.py dev")
    print("  cd dbt_project/health_pipeline")
    print("  dbt run --target dev")
    print("  dbt test --target dev")
    print("=" * 50)


if __name__ == "__main__":
    main()