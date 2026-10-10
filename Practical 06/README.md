# Practical 6: Setting up Apache Airflow and Creating a DAG for ETL Orchestration

**Author:** Wali Momin

---

## 1. Aim
To install and initialize Apache Airflow, define a Directed Acyclic Graph (DAG) for a multi-stage university ETL pipeline, configure task dependencies, and test task execution using the Airflow CLI.

---

## 2. Brief Theory
Apache Airflow is an open-source platform used to author, schedule, and monitor workflows programmatically:
- **Core Architecture:**
  - **Scheduler:** Handles triggering and scheduling scheduled jobs.
  - **Metadata Database:** Stores state of DAGs, tasks, and variables (e.g. SQLite, PostgreSQL).
  - **Webserver:** Web UI for monitoring pipeline health.
  - **Workers / Executors:** Execute scheduled tasks.
- **DAG (Directed Acyclic Graph):** Collection of tasks organized with directional dependencies, ensuring execution without circular loops.
- **Operators:** Building blocks of tasks:
  - `BashOperator`: Executes bash shell commands and scripts.
  - `PythonOperator`: Calls Python functions.
- **Task Dependencies (`>>` / `<<`):** Defines sequential execution flow (e.g. `task_start >> extract_data >> pipeline_complete`).

---

## 3. Code
```python
!pip install -q apache-airflow

import os
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
!airflow db migrate

!mkdir -p /root/airflow/dags

%%writefile /root/airflow/dags/data_extraction.py
# Practical 4: Extracting Data from APIs and Flat Files Using Python
# Objective: fetch user data from a public REST API, clean it, and merge it
#            with a flat CSV file containing location details.
import pandas as pd
import requests

# >>> CHANGE THIS TO YOUR NAME <<<
STUDENT_NAME = "Wali Momin"


def extract_api_data(url):
    """Fetches data from a REST API and returns a DataFrame."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        # Normalizing JSON data into a flat table
        return pd.json_normalize(data)
    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
        return pd.DataFrame()


def extract_csv_data(file_path):
    """Reads a flat CSV file."""
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError as e:
        print(f"File Error: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    # 1. API Extraction
    api_url = "https://jsonplaceholder.typicode.com/users"
    api_df = extract_api_data(api_url)
    if not api_df.empty:
        api_df = api_df[["id", "name", "email", "company.name"]]
        api_df.rename(columns={"company.name": "company"}, inplace=True)

    # 2. Flat File Extraction (Mock CSV Data)
    # 'locations.csv' has columns: id, city
    csv_file = "locations.csv"

    # Creating a dummy file for the lab test run
    pd.DataFrame(
        {
            "id": list(range(1, 11)),
            "city": [
                "New York", "London", "Paris", "Tokyo", "Berlin",
                "Delhi", "Sydney", "Moscow", "Cairo", "Beijing",
            ],
        }
    ).to_csv(csv_file, index=False)

    csv_df = extract_csv_data(csv_file)

    # 3. Data Transformation & Merging
    if not api_df.empty and not csv_df.empty:
        merged_df = pd.merge(api_df, csv_df, on="id", how="inner")
        print("\n--- Merged ETL Pipeline Data View ---")
        print(merged_df.head())

        # Save to target Data Lake / Warehouse stage
        merged_df.to_csv("cleaned_warehouse_profiles.csv", index=False)
        print("\nData successfully saved to 'cleaned_warehouse_profiles.csv'")

        # Check for missing values after the merge
        print("\nMissing values per column:\n", merged_df.isnull().sum())

    print("\n" + "=" * 40)
    print("Submitted by:", STUDENT_NAME)
    print("=" * 40)

%%writefile /root/airflow/dags/data_pipeline_dag.py
# Practical 5: Setting up Apache Airflow and Creating a DAG
# Objective: configure a DAG that runs daily with sequential tasks:
#            a start notifier, an extraction script, and a pipeline success logger.
# Place this file in your Airflow DAGs directory: ~/airflow/dags/data_pipeline_dag.py
import os
from datetime import datetime, timedelta

from airflow import DAG

# Works on both Airflow 3.x (new import paths) and Airflow 2.x (old paths)
try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator

# >>> CHANGE THIS TO YOUR NAME <<<
STUDENT_NAME = "Wali Momin"

# Practical 4 script: copy it to ~/airflow/dags/ and name it data_extraction.py
EXTRACTION_SCRIPT = os.path.expanduser("~/airflow/dags/data_extraction.py")

# Default arguments for the workflow pipeline execution
default_args = {
    "owner": "data_engineering_lab",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG context
with DAG(
    "university_etl_orchestration",
    default_args=default_args,
    description="Lab assignment for API and Flat File ETL orchestration",
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    # Task 1: Pipeline Init Anchor
    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # Task 2: Run Python Extraction Script
    execute_extraction = BashOperator(
        task_id="run_extraction_script",
        bash_command=f"python3 {EXTRACTION_SCRIPT}",
    )

    # Task 3: Finalize and Log Metrics
    pipeline_complete = BashOperator(
        task_id="log_pipeline_success",
        bash_command='echo "ETL Execution completed successfully at $(date)"',
    )

    # Task 4: Print the student name in the task log
    show_student_name = BashOperator(
        task_id="show_student_name",
        bash_command=f'echo "Submitted by: {STUDENT_NAME}"',
    )

    # Task dependencies (execution order)
    start_pipeline >> execute_extraction >> pipeline_complete >> show_student_name

# ---------------------------------------------------------------------------
# Commands to run (Terminal):
#   airflow dags list
#   airflow tasks test university_etl_orchestration start_pipeline 2026-01-01
#   airflow tasks test university_etl_orchestration run_extraction_script 2026-01-01
#   airflow tasks test university_etl_orchestration log_pipeline_success 2026-01-01
#   airflow tasks test university_etl_orchestration show_student_name 2026-01-01
# ---------------------------------------------------------------------------

!airflow tasks test university_etl_orchestration start_pipeline 2026-01-01

!airflow tasks test university_etl_orchestration run_extraction_script 2026-01-01

!airflow tasks test university_etl_orchestration log_pipeline_success 2026-01-01

!airflow tasks test university_etl_orchestration show_student_name 2026-01-01

!airflow dags list
```

---

## 4. Output
```text
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 0.0/6.9 MB ? eta -:--:--   ━━━━━━━━━━━╸━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 2.1/6.9 MB 62.2 MB/s eta 0:00:01   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╸ 6.9/6.9 MB 102.9 MB/s eta 0:00:01   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 6.9/6.9 MB 65.0 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 607.5/607.5 kB 32.2 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 268.7/268.7 kB 17.7 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 49.0/49.0 kB 3.9 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 98.7/98.7 kB 7.2 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 163.5/163.5 kB 8.5 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 43.3/43.3 kB 1.9 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 61.2/61.2 kB 5.5 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 76.0/76.0 kB 6.5 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 46.7/46.7 kB 2.8 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 117.5/117.5 kB 7.9 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 71.5/71.5 kB 5.4 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 2.3/2.3 MB 70.6 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 230.5/230.5 kB 14.2 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 72.5/72.5 kB 6.4 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 57.3/57.3 kB 3.8 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 348.9/348.9 kB 25.8 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 155.6/155.6 kB 9.6 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 73.8/73.8 kB 4.2 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 83.5/83.5 kB 6.6 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 623.8/623.8 kB 39.5 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 79.5/79.5 kB 6.3 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 69.4/69.4 kB 5.8 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 778.7/778.7 kB 44.4 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 354.8/354.8 kB 26.7 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 140.1/140.1 kB 10.4 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 60.3/60.3 kB 5.1 MB/s eta 0:00:00
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 206.3/206.3 kB 16.8 MB/s eta 0:00:00
ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
google-adk 2.7.1 requires opentelemetry-api<=1.42.1,>=1.39, but you have opentelemetry-api 1.45.1 which is incompatible.
google-adk 2.7.1 requires opentelemetry-sdk<=1.42.1,>=1.39, but you have opentelemetry-sdk 1.45.1 which is incompatible.


2026-10-09T15:29:27.001471Z [info     ] Performing upgrade to the metadata database [airflow.cli.commands.db_command] loc=db_command.py:134 url=sqlite:////root/airflow/airflow.db
2026-10-09T15:29:27.746717Z [info     ] Context impl SQLiteImpl.       [alembic.runtime.migration] loc=migration.py:205
2026-10-09T15:29:27.746961Z [info     ] Will assume non-transactional DDL. [alembic.runtime.migration] loc=migration.py:208
2026-10-09T15:29:28.966002Z [info     ] Context impl SQLiteImpl.       [alembic.runtime.migration] loc=migration.py:205
2026-10-09T15:29:28.966225Z [info     ] Will assume non-transactional DDL. [alembic.runtime.migration] loc=migration.py:208
2026-10-09T15:29:28.966774Z [info     ] Creating Airflow database tables from the ORM [airflow.utils.db] loc=db.py:749
2026-10-09T15:29:28.966949Z [info     ] Creating global lock context   [airflow.utils.db] loc=db.py:730
2026-10-09T15:29:28.967113Z [info     ] Pool status: Pool size: 5  Connections in pool: 0 Current Overflow: -4 Current Checked out connections: 1 [airflow.utils.db] loc=db.py:733
2026-10-09T15:29:28.967228Z [info     ] Creating metadata              [airflow.utils.db] loc=db.py:735
2026-10-09T15:29:29.504369Z [info     ] Getting alembic config         [airflow.utils.db] loc=db.py:738
2026-10-09T15:29:29.505807Z [info     ] Stamping migration head        [airflow.utils.db] loc=db.py:741
2026-10-09T15:29:29.507209Z [info     ] Context impl SQLiteImpl.       [alembic.runtime.migration] loc=migration.py:205
2026-10-09T15:29:29.507384Z [info     ] Will assume non-transactional DDL. [alembic.runtime.migration] loc=migration.py:208
2026-10-09T15:29:29.555017Z [info     ] Running stamp_revision  -> d2f4e1b3c5a7 [alembic.runtime.migration] loc=migration.py:611
2026-10-09T15:29:29.560678Z [info     ] Airflow database tables created [airflow.utils.db] loc=db.py:744
2026-10-09T15:29:29.755471Z [info     ] Database migration done!       [airflow.cli.commands.db_command] loc=db_command.py:152

Writing /root/airflow/dags/data_extraction.py

Writing /root/airflow/dags/data_pipeline_dag.py

2026-10-09T15:29:41.664606Z [info     ] DAG bundles loaded: dags-folder [airflow.dag_processing.bundles.manager.DagBundlesManager] loc=manager.py:304
2026-10-09T15:29:42.430149Z [info     ] Added new DAG bundle dags-folder to the database [airflow.dag_processing.bundles.manager.DagBundlesManager] loc=manager.py:378
2026-10-09T15:29:42.442788Z [info     ] Filling up the DagBag from /root/airflow/dags [airflow.dag_processing.dagbag.BundleDagBag] loc=dagbag.py:460
2026-10-09T15:29:42.478260Z [info     ] Bulk-writing dags to db        [airflow.serialization.definitions.dag] count=1 loc=dag.py:218
2026-10-09T15:29:42.499488Z [info     ] Creating ORM DAG for university_etl_orchestration [airflow.dag_processing.collection] loc=collection.py:102
2026-10-09T15:29:42.515278Z [info     ] get next_dagrun_info_v2        [airflow.serialization.definitions.dag] last_automated_run_info=None loc=dag.py:453 next_info=DagRunInfo(run_after=DateTime(2026, 10, 9, 15, 29, 42, 515139, tzinfo=Timezone('UTC')), data_interval=DataInterval(start=DateTime(2026, 10, 9, 15, 29, 42, 515139, tzinfo=Timezone('UTC')), end=DateTime(2026, 10, 9, 15, 29, 42, 515139, tzinfo=Timezone('UTC'))), partition_date=None, partition_key=None)

... [Log truncated for brevity] ...

2026-10-09T15:30:05.669454Z [info     ] Task instance state updated    [airflow.api_fastapi.execution_api.routes.task_instances] correlation_id=01a12149-2aca-70e5-8cf5-1f0edbc3d202 loc=task_instances.py:485 new_state=success rows_affected=1 ti_id=01a12149-0991-7efd-a95c-46783ea6ae79
2026-10-09T15:30:05.677927Z [info     ] ::endgroup::                   [task] loc=task_runner.py:2379
2026-10-09T15:30:05.691615Z [info     ] [DAG TEST] end task task_id=run_extraction_script map_index=-1 [airflow.sdk.definitions.dag] loc=dag.py:1563

2026-10-09T15:30:11.713326Z [info     ] DAG bundles loaded: dags-folder [airflow.dag_processing.bundles.manager.DagBundlesManager] loc=manager.py:304
2026-10-09T15:30:11.939941Z [info     ] Filling up the DagBag from /root/airflow/dags [airflow.dag_processing.dagbag.BundleDagBag] loc=dagbag.py:460
2026-10-09T15:30:11.963641Z [info     ] Bulk-writing dags to db        [airflow.serialization.definitions.dag] count=1 loc=dag.py:218
2026-10-09T15:30:11.986015Z [info     ] get next_dagrun_info_v2        [airflow.serialization.definitions.dag] last_automated_run_info=None loc=dag.py:453 next_info=DagRunInfo(run_after=DateTime(2026, 10, 9, 15, 30, 11, 985912, tzinfo=Timezone('UTC')), data_interval=DataInterval(start=DateTime(2026, 10, 9, 15, 30, 11, 985912, tzinfo=Timezone('UTC')), end=DateTime(2026, 10, 9, 15, 30, 11, 985912, tzinfo=Timezone('UTC'))), partition_date=None, partition_key=None)
2026-10-09T15:30:11.986358Z [info     ] setting next dagrun info       [airflow.models.dag] loc=dag.py:848 next_dagrun='2026-10-09 15:30:11.985912+00:00' next_dagrun_create_after='2026-10-09 15:30:11.985912+00:00' next_dagrun_data_interval_end='2026-10-09 15:30:11.985912+00:00' next_dagrun_data_interval_start='2026-10-09 15:30:11.985912+00:00' next_dagrun_partition_date=None next_dagrun_partition_key=None
2026-10-09T15:30:12.025365Z [info     ] creating dag run               [airflow.serialization.definitions.dag] loc=dag.py:615 logical_date=DateTime(2026, 1, 1, 0, 0, 0, tzinfo=Timezone('UTC')) partition_key=None run_after=DateTime(2026, 1, 1, 0, 0, 0, tzinfo=Timezone('UTC')) run_id=__airflow_temporary_run_2026-10-09T15:30:12.023102+00:00__
2026-10-09T15:30:12.116413Z [info     ] Created dag run.               [airflow.models.dagrun] dagrun=<DagRun university_etl_orchestration @ 2026-01-01 00:00:00+00:00: __airflow_temporary_run_2026-10-09T15:30:12.023102+00:00__, state:running, queued_at: None. run_type: manual> loc=dagrun.py:2464
2026-10-09T15:30:12.130591Z [info     ] [DAG TEST] starting task_id=log_pipeline_success map_index=-1 [airflow.sdk.definitions.dag] loc=dag.py:1501
2026-10-09T15:30:12.131108Z [info     ] [DAG TEST] running task <TaskInstance: university_etl_orchestration.log_pipeline_success __airflow_temporary_run_2026-10-09T15:30:12.023102+00:00__ [None] ti_id=01a12149-43d2-7560-8675-148315689be6> [airflow.sdk.definitions.dag] loc=dag.py:1504
2026-10-09T15:30:19.486227Z [info     ] Task started                   [airflow.api_fastapi.execution_api.routes.task_instances] correlation_id=01a12149-4a61-799b-85b8-7917729a3677 hostname=998e8c7e2756 loc=task_instances.py:226 previous_state=queued ti_id=01a12149-43d2-7560-8675-148315689be6
2026-10-09T15:30:19.488986Z [info     ] Task instance state updated    [airflow.api_fastapi.execution_api.routes.task_instances] correlation_id=01a12149-4a61-799b-85b8-7917729a3677 loc=task_instances.py:254 rows_affected=1 ti_id=01a12149-43d2-7560-8675-148315689be6
2026-10-09T15:30:19.527360Z [info     ] Updating RenderedTaskInstanceFields [airflow.api_fastapi.execution_api.routes.task_instances] correlation_id=01a12149-60fe-7569-b97b-8bf7b1afd347 field_count=3 loc=task_instances.py:1017 ti_id=01a12149-43d2-7560-8675-148315689be6
2026-10-09T15:30:19.547102Z [info     ] ::endgroup::                   [task] loc=task_runner.py:2232
2026-10-09T15:30:19.548175Z [info     ] Tmp dir root location: /tmp    [airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook] loc=subprocess.py:78
2026-10-09T15:30:19.548667Z [info     ] Running command: ['/usr/bin/bash', '-c', 'echo "ETL Execution completed successfully at $(date)"'] [airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook] loc=subprocess.py:80
2026-10-09T15:30:19.549346Z [info     ] Output:                        [airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook] loc=subprocess.py:92
2026-10-09T15:30:19.552250Z [info     ] ETL Execution completed successfully at Fri Oct  9 03:30:19 PM UTC 2026 [airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook] loc=subprocess.py:99
2026-10-09T15:30:19.552508Z [info     ] Command exited with return code 0 [airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook] loc=subprocess.py:103
2026-10-09T15:30:19.552899Z [info     ] ::group::Post Execute          [task] loc=task_runner.py:1580
2026-10-09T15:30:19.553171Z [info     ] Pushing xcom                   [task] loc=task_runner.py:2280 ti=RuntimeTaskInstance(id=UUID('01a12149-43d2-7560-8675-148315689be6'), task_id='log_pipeline_success', dag_id='university_etl_orchestration', run_id='__airflow_temporary_run_2026-10-09T15:30:12.023102+00:00__', try_number=0, dag_version_id=UUID('01a12148-d09c-7945-8d96-b72caf92ae3e'), map_index=-1, hostname='998e8c7e2756', context_carrier=None, queue='default', task=<Task(BashOperator): log_pipeline_success>, max_tries=1, start_date=datetime.datetime(2026, 10, 9, 15, 30, 13, 727099, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='')
2026-10-09T15:30:19.599897Z [info     ] Task instance state updated    [airflow.api_fastapi.execution_api.routes.task_instances] correlation_id=01a12149-6139-7f32-b2eb-d82338d63d5a loc=task_instances.py:485 new_state=success rows_affected=1 ti_id=01a12149-43d2-7560-8675-148315689be6
2026-10-09T15:30:19.607530Z [info     ] ::endgroup::                   [task] loc=task_runner.py:2379
2026-10-09T15:30:19.620341Z [info     ] [DAG TEST] end task task_id=log_pipeline_success map_index=-1 [airflow.sdk.definitions.dag] loc=dag.py:1563

2026-10-09T15:30:25.776133Z [info     ] DAG bundles loaded: dags-folder [airflow.dag_processing.bundles.manager.DagBundlesManager] loc=manager.py:304
2026-10-09T15:30:25.999888Z [info     ] Filling up the DagBag from /root/airflow/dags [airflow.dag_processing.dagbag.BundleDagBag] loc=dagbag.py:460
2026-10-09T15:30:26.025403Z [info     ] Bulk-writing dags to db        [airflow.serialization.definitions.dag] count=1 loc=dag.py:218
2026-10-09T15:30:26.048859Z [info     ] get next_dagrun_info_v2        [airflow.serialization.definitions.dag] last_automated_run_info=None loc=dag.py:453 next_info=DagRunInfo(run_after=DateTime(2026, 10, 9, 15, 30, 26, 48751, tzinfo=Timezone('UTC')), data_interval=DataInterval(start=DateTime(2026, 10, 9, 15, 30, 26, 48751, tzinfo=Timezone('UTC')), end=DateTime(2026, 10, 9, 15, 30, 26, 48751, tzinfo=Timezone('UTC'))), partition_date=None, partition_key=None)
2026-10-09T15:30:26.049214Z [info     ] setting next dagrun info       [airflow.models.dag] loc=dag.py:848 next_dagrun='2026-10-09 15:30:26.048751+00:00' next_dagrun_create_after='2026-10-09 15:30:26.048751+00:00' next_dagrun_data_interval_end='2026-10-09 15:30:26.048751+00:00' next_dagrun_data_interval_start='2026-10-09 15:30:26.048751+00:00' next_dagrun_partition_date=None next_dagrun_partition_key=None
2026-10-09T15:30:26.091087Z [info     ] creating dag run               [airflow.serialization.definitions.dag] loc=dag.py:615 logical_date=DateTime(2026, 1, 1, 0, 0, 0, tzinfo=Timezone('UTC')) partition_key=None run_after=DateTime(2026, 1, 1, 0, 0, 0, tzinfo=Timezone('UTC')) run_id=__airflow_temporary_run_2026-10-09T15:30:26.088762+00:00__
2026-10-09T15:30:26.185497Z [info     ] Created dag run.               [airflow.models.dagrun] dagrun=<DagRun university_etl_orchestration @ 2026-01-01 00:00:00+00:00: __airflow_temporary_run_2026-10-09T15:30:26.088762+00:00__, state:running, queued_at: None. run_type: manual> loc=dagrun.py:2464
2026-10-09T15:30:26.200017Z [info     ] [DAG TEST] starting task_id=show_student_name map_index=-1 [airflow.sdk.definitions.dag] loc=dag.py:1501
2026-10-09T15:30:26.200502Z [info     ] [DAG TEST] running task <TaskInstance: university_etl_orchestration.show_student_name __airflow_temporary_run_2026-10-09T15:30:26.088762+00:00__ [None] ti_id=01a12149-7ac5-7570-8aad-a6843af2149a> [airflow.sdk.definitions.dag] loc=dag.py:1504
2026-10-09T15:30:33.652282Z [info     ] Task started                   [airflow.api_fastapi.execution_api.routes.task_instances] correlation_id=01a12149-8194-79ee-8b80-07dff9d850d6 hostname=998e8c7e2756 loc=task_instances.py:226 previous_state=queued ti_id=01a12149-7ac5-7570-8aad-a6843af2149a
2026-10-09T15:30:33.655901Z [info     ] Task instance state updated    [airflow.api_fastapi.execution_api.routes.task_instances] correlation_id=01a12149-8194-79ee-8b80-07dff9d850d6 loc=task_instances.py:254 rows_affected=1 ti_id=01a12149-7ac5-7570-8aad-a6843af2149a
2026-10-09T15:30:33.693634Z [info     ] Updating RenderedTaskInstanceFields [airflow.api_fastapi.execution_api.routes.task_instances] correlation_id=01a12149-9854-7c28-893b-b8d712ff0784 field_count=3 loc=task_instances.py:1017 ti_id=01a12149-7ac5-7570-8aad-a6843af2149a
2026-10-09T15:30:33.712527Z [info     ] ::endgroup::                   [task] loc=task_runner.py:2232
2026-10-09T15:30:33.713421Z [info     ] Tmp dir root location: /tmp    [airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook] loc=subprocess.py:78
2026-10-09T15:30:33.713881Z [info     ] Running command: ['/usr/bin/bash', '-c', 'echo "Submitted by: Wali Momin"'] [airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook] loc=subprocess.py:80
2026-10-09T15:30:33.714588Z [info     ] Output:                        [airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook] loc=subprocess.py:92
2026-10-09T15:30:33.715786Z [info     ] Submitted by: Wali Momin [airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook] loc=subprocess.py:99
2026-10-09T15:30:33.716005Z [info     ] Command exited with return code 0 [airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook] loc=subprocess.py:103
2026-10-09T15:30:33.716355Z [info     ] ::group::Post Execute          [task] loc=task_runner.py:1580
2026-10-09T15:30:33.716667Z [info     ] Pushing xcom                   [task] loc=task_runner.py:2280 ti=RuntimeTaskInstance(id=UUID('01a12149-7ac5-7570-8aad-a6843af2149a'), task_id='show_student_name', dag_id='university_etl_orchestration', run_id='__airflow_temporary_run_2026-10-09T15:30:26.088762+00:00__', try_number=0, dag_version_id=UUID('01a12148-d09c-7945-8d96-b72caf92ae3e'), map_index=-1, hostname='998e8c7e2756', context_carrier=None, queue='default', task=<Task(BashOperator): show_student_name>, max_tries=1, start_date=datetime.datetime(2026, 10, 9, 15, 30, 27, 857938, tzinfo=datetime.timezone.utc), end_date=None, state=<TaskInstanceState.RUNNING: 'running'>, is_mapped=False, rendered_map_index=None, sentry_integration='')
2026-10-09T15:30:33.770318Z [info     ] Task instance state updated    [airflow.api_fastapi.execution_api.routes.task_instances] correlation_id=01a12149-988c-7418-ac55-ddae93d6e7e6 loc=task_instances.py:485 new_state=success rows_affected=1 ti_id=01a12149-7ac5-7570-8aad-a6843af2149a
2026-10-09T15:30:33.779471Z [info     ] ::endgroup::                   [task] loc=task_runner.py:2379
2026-10-09T15:30:33.792980Z [info     ] [DAG TEST] end task task_id=show_student_name map_index=-1 [airflow.sdk.definitions.dag] loc=dag.py:1563

             |             |             |           |             | bundle_vers
dag_id       | fileloc     | owners      | is_paused | bundle_name | ion        
=============+=============+=============+===========+=============+============
university_e | /root/airfl | data_engine | True      | dags-folder | None       
tl_orchestra | ow/dags/dat | ering_lab   |           |             |            
tion         | a_pipeline_ |             |           |             |            
             | dag.py      |             |           |             |            
                                                                                
```

---

## 5. Conclusion
Successfully installed and initialized Apache Airflow. Designed and tested a complete ETL DAG with sequential dependencies, verifying task execution, log propagation, and pipeline orchestration.
