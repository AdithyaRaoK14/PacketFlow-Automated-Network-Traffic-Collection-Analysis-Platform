"""
STUDY DAG 2 — Task Dependencies
=================================
A linear 3-step pipeline: extract -> transform -> load.

Core concepts introduced here:

- Dependencies between tasks are declared with the `>>` operator.
  `extract_task >> transform_task >> load_task` means: don't start
  `transform_task` until `extract_task` finishes successfully, and don't
  start `load_task` until `transform_task` finishes successfully.

- This is the actual "graph" part of "Directed Acyclic Graph": tasks are
  nodes, `>>` draws the edges, and the graph can't loop back on itself
  (that's what "acyclic" means - no task can depend on itself, directly
  or indirectly).

- Data can be passed between tasks with XCom (see study_05) - here we
  keep it simple and just show the ORDER of execution, not data passing.

- You can also write dependencies the other direction with `<<`, or wire
  up several tasks at once - see study_04 for fan-out/fan-in with lists.

Try this:
  1. Trigger this DAG and open the "Graph" view for the run.
  2. Watch the tasks turn from queued -> running -> success in order,
     left to right - that's the dependency chain in action.
  3. Try editing this file to make `load_task` depend on nothing (remove
     it from the chain) and see the graph view change on the next run.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract():
    print("Extracting data from a source (pretend this hits an API or file)...")


def transform():
    print("Transforming the extracted data (pretend this cleans/reshapes it)...")


def load():
    print("Loading the transformed data into a destination (pretend this is a DB)...")


with DAG(
    dag_id="study_02_task_dependencies",
    description="STUDY: linear extract -> transform -> load chain using >>",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["study"],
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    # This single line defines the entire shape of the DAG's graph.
    extract_task >> transform_task >> load_task
