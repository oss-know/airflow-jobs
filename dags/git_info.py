import time
from datetime import datetime
from pprint import pprint
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='git_sync_v1',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['github'],
) as dag:
    # [START howto_operator_python]
    def init_sync_git_info(ds, **kwargs):
        from airflow.models import Variable
        from libs.github import gits

        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)

        init_sync_git_info = gits.init_sync_git_datas("https://github.com/gitpython-developers/GitPython.git",
                                             "gitpython-developers",
                                             "GitPython",
                                             opensearch_conn_datas,)
        print(init_sync_git_info)
        return 'init_sync_git_info:::end'

    op_init_sync_git_info = PythonOperator(
        task_id='init_sync_git_info',
        python_callable=init_sync_git_info,
    )
    # [END howto_operator_python]

    # [START howto_operator_python_kwargs]
    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(random_base)

    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):
        task = PythonOperator(
            task_id='sleep_for_' + str(i),
            python_callable=my_sleeping_function,
            op_kwargs={'random_base': float(i) / 10},
        )

        op_init_sync_git_info >> task
    # [END howto_operator_python_kwargs]


