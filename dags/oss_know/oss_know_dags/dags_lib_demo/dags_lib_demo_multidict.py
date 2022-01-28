from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

with DAG(
        dag_id='demo_lib_multidict',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['demo_lib'],
) as dag:
    def scheduler_demo_lib_multidict(ds, **kwargs):
        return 'scheduler_demo_lib_multidict'


    op_scheduler_demo_lib_multidict = PythonOperator(
        task_id='op_scheduler_demo_lib_multidict',
        python_callable=scheduler_demo_lib_multidict
    )


    def do_demo_lib_multidict(params):
        from oss_know.libs.lib_demo import lib_demo_multidict

        do_init_sync_info = lib_demo_multidict.lib_demo_multidict()

        return do_init_sync_info


    op_do_demo_lib_multidict = PythonOperator(
        task_id='op_do_demo_lib_multidict',
        python_callable=do_demo_lib_multidict,
        provide_context=True,
    )

    op_scheduler_demo_lib_multidict >> op_do_demo_lib_multidict
