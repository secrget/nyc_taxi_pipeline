from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import config as con

with DAG(
    dag_id="dbt_run",
    schedule_interval=None,
) as dag:
    run_dbt=BashOperator(
        task_id="run_dbt_model",
        bash_command=f"""cd{con.DBT_PROJECT_PATH} && \
        dbt build
        """
    )
