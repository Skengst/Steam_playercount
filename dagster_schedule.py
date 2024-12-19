from dagster import job, op, schedule, Definitions
import subprocess
import os

# Ensure environment variables are loaded
from dotenv import load_dotenv
load_dotenv()  # This assumes the .env file is in the same directory as this script

# Define an operation to run the main.py script
@op
def run_main_py():
    # Change directory to where main.py resides
    script_dir = os.path.dirname(os.path.abspath(__file__))
    subprocess.run(["python", os.path.join(script_dir, "main.py")], check=True)

# Define a job that uses the operation
@job
def main_job():
    run_main_py()

# Define a schedule to run the job once per hour
@schedule(cron_schedule="0 * * * *", job=main_job, execution_timezone="UTC")
def hourly_schedule(_context):
    return {}

# Combine everything into Dagster Definitions
defs = Definitions(
    jobs=[main_job],
    schedules=[hourly_schedule],
)