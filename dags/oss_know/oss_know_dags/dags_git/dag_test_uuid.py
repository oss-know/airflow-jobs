from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# git_init_sync_v0.0.3
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITS

with DAG(
        dag_id='test_uuid',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['uuid'],
) as dag:
    def init_sync_git_info(ds, **kwargs):
        return 'Start test_uuid'


    op_init_sync_git_info = PythonOperator(
        task_id='test_uuid',
        python_callable=init_sync_git_info,
    )


    def do_sync_git_info(params, **kwargs):
        from oss_know.libs.github._test_uuid import get_uuid

        kwargs['ti'].xcom_push(key=f'{params}_ids', value=get_uuid())
        return 'do_sync_git_info:::end'


    def get_len(**kwargs):
        aal = []
        for i in range(300):
            # set.union(ss,set(kwargs['ti'].xcom_pull(key=f'{i}_ids')))
            # print(i)
            print(len(kwargs['ti'].xcom_pull(key=f'{i}_ids')))
            aal = aal + kwargs['ti'].xcom_pull(key=f'{i}_ids')
        print(len(set(aal)))
        # print(len(ss),"----------------------------")

    op_get_length = PythonOperator(
        task_id=f'do_test_uuid',
        python_callable=get_len,
        provide_context=True
    )

    for i in range(300):
        op_do_init_sync_git_info = PythonOperator(
            task_id=f'do_test_uuid_{i}',
            python_callable=do_sync_git_info,
            op_kwargs={'params': i},
            provide_context=True
        )
        op_init_sync_git_info >> op_do_init_sync_git_info>>op_get_length
