import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
        dag_id='github_test_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def start_load_github_profile(ds, **kwargs):
        from libs.github.test import test_github_headers
        test_github_headers()
        return 'End start_load_github_profile'
    op_start_load_github_profile = PythonOperator(
        task_id='op_start_load_github_profile',
        python_callable=start_load_github_profile,
    )

    def end_load_github_profile(ds, **kwargs):
        return 'End::end_load_github_profile'
    op_end_load_github_profile = PythonOperator(
        task_id='op_end_load_github_profile',
        python_callable=end_load_github_profile,
    )
    op_start_load_github_profile >> op_end_load_github_profile