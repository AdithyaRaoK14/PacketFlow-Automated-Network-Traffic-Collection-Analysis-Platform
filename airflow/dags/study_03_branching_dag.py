"""
STUDY DAG 3 — Branching
=========================
Picks one of two paths at runtime, based on a condition, then joins back
into a single final task.

Core concepts introduced here:

- `BranchPythonOperator` runs a function that returns the task_id (or list
  of task_ids) that should run next. Every OTHER downstream task not
  returned gets automatically marked "skipped" instead of running.

- This is how you express "if/else" logic in a DAG - a decision made once
  at runtime, not something you'd know when you're just writing the
  Python file.

- `trigger_rule="none_failed_min_one_success"` on the join task matters:
  by default, a task waits for ALL of its upstream tasks to succeed. But
  here only ONE of the two branches will actually run (the other is
  "skipped", not "failed") - so the join task needs a rule that accepts
  "at least one upstream succeeded, and nothing outright failed" instead
  of demanding every single upstream task succeed.

Try this:
  1. Trigger this DAG a few times and check the Graph view for each run -
     you should see it alternate between the "even" and "odd" branch
     being skipped (greyed out) vs run (green), based on the current
     minute.
  2. This is exactly the kind of pattern you'd use for "if this capture
     found more than N packets, do extra analysis" in the main pipeline.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator


def choose_branch(**context):
    """Returns the task_id to run next - this is the "if" statement."""
    minute = datetime.now().minute
    if minute % 2 == 0:
        print(f"Current minute ({minute}) is even -> taking the 'even' branch")
        return "even_branch"
    print(f"Current minute ({minute}) is odd -> taking the 'odd' branch")
    return "odd_branch"


def handle_even():
    print("Handling the 'even minute' case.")


def handle_odd():
    print("Handling the 'odd minute' case.")


with DAG(
    dag_id="study_03_branching",
    description="STUDY: BranchPythonOperator picks one path, the other is skipped",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["study"],
) as dag:

    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="choose_branch",
        python_callable=choose_branch,
    )

    even_branch = PythonOperator(task_id="even_branch", python_callable=handle_even)
    odd_branch = PythonOperator(task_id="odd_branch", python_callable=handle_odd)

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    start >> branch >> [even_branch, odd_branch] >> join
