from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# git_init_sync_v0.0.3
# from ..libs.base_dict.variable_key import NEED_INIT_GITS

with DAG(
        dag_id='os2ck',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['os2ck'],
) as dag:
    def do_sync_os2ck(params, **kwargs):
        return 'do_sync_os2ck'


    op_do_sync_os2ck = PythonOperator(
        task_id=f'do_sync_os2ck',
        python_callable=do_sync_os2ck,
        op_kwargs={'params': {}},
    )
    op_do_sync_os2ck
