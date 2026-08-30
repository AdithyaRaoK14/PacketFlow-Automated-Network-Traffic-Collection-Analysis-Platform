"""
STUDY DAG 6 — Operators & Scheduling
=======================================
Mixes three operator types in one DAG, and this run actually IS scheduled
automatically (unlike study_01 through study_05), to show how
schedule_interval/start_date/catchup work together.

Core concepts introduced here:

- An "operator" is a template for one KIND of task. This project alone
  uses:
    - PythonOperator   -> runs a Python function      (most tasks here)
    - BashOperator     -> runs a shell command
    - EmptyOperator    -> does nothing, just a marker/join point in the
                          graph (used in study_03 and study_04)
    - BranchPythonOperator -> picks a path at runtime  (study_03)
  There are many more (SQL operators, sensors that wait for a condition,
  operators for specific services like S3/Slack/Docker, etc.) - an
  "operator" is really just "a reusable task type someone already wrote".

- `schedule_interval` controls automatic runs. It accepts:
    - A cron string, e.g. "*/5 * * * *" (every 5 minutes - what the main
      `packet_capture_pipeline` DAG uses)
    - A preset like "@daily", "@hourly", "@once"
    - A Python `timedelta(hours=1)`
    - `None` for manual-only (what every other study DAG in this folder
      uses, so triggering them doesn't accidentally spam runs)

- `start_date` is the earliest point in time this DAG is allowed to have
  a scheduled run for. It's a fixed point in the past, not "now" - Airflow
  schedules the NEXT interval after start_date, not immediately at
  start_date.

- `catchup` controls whether Airflow tries to "back-fill" every missed
  scheduled run between start_date and now the first time the DAG is
  turned on. `catchup=False` (used everywhere in this project) means:
  only run going forward from now - which is what you want for a demo/
  study project, since `catchup=True` on an old start_date could trigger
  hundreds of backlogged runs at once.

Try this:
  1. Turn this DAG on and leave it - it'll run automatically every
     2 minutes (`schedule_interval="*/2 * * * *"`), unlike the other
     study DAGs which only run when you click the trigger button.
  2. Compare its three tasks' logs - notice the BashOperator's log
     literally shows the shell command and its stdout, while the
     PythonOperator's log shows your print() output.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def python_task_example():
    print("This ran inside a PythonOperator - any Python function works here.")


with DAG(
    dag_id="study_06_operators_and_scheduling",
    description="STUDY: PythonOperator vs BashOperator vs EmptyOperator, and an actual schedule",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/2 * * * *",  # runs automatically every 2 minutes
    catchup=False,
    tags=["study"],
) as dag:

    start = EmptyOperator(task_id="start")  # just a marker, does no work

    python_step = PythonOperator(
        task_id="python_step",
        python_callable=python_task_example,
    )

    bash_step = BashOperator(
        task_id="bash_step",
        bash_command="echo 'This ran as a shell command via BashOperator' && date",
    )

    start >> [python_step, bash_step]
