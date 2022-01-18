from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, REDIS_CLIENT_DATA, \
    DURATION_OF_SYNC_GITHUB_PROFILES

# v0.0.1
# SYNC_PROFILES_TASK_NUM: 开启查询更新的线程个数
SYNC_PROFILES_TASK_NUM = 10

with DAG(
        dag_id='github_sync_profiles_v101',
        schedule_interval=None,
        # TODO：生产环境设定定时,更改schedule_interval的值
        # schedule_interval='0 0 * * *',
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_sync_github_profiles(ds, **kwargs):
        return 'End:scheduler_sync_github_profiles'


    op_scheduler_sync_github_profiles = PythonOperator(
        task_id='op_scheduler_sync_github_profiles',
        python_callable=scheduler_sync_github_profiles
    )


    def do_sync_github_profiles():
        from airflow.models import Variable
        from oss_know.libs.github import sync_profiles01

        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        redis_client_info = Variable.get(REDIS_CLIENT_DATA, deserialize_json=True)
        duration_of_sync_github_profiles = Variable.get(DURATION_OF_SYNC_GITHUB_PROFILES, deserialize_json=True)
        sync_profiles01.sync_github_profiles(github_tokens=github_tokens, opensearch_conn_info=opensearch_conn_info,
                                             redis_client_info=redis_client_info,
                                             duration_of_sync_github_profiles=duration_of_sync_github_profiles)


    for time in range(SYNC_PROFILES_TASK_NUM):
        op_do_sync_github_profiles = PythonOperator(
            task_id=f'op_do_sync_github_profiles_{time + 1}',
            python_callable=do_sync_github_profiles,
            provide_context=True,
        )


    def init_storage_updated_profiles():
        from airflow.models import Variable
        from oss_know.libs.github import sync_profiles01

        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        redis_client_info= Variable.get(REDIS_CLIENT_DATA, deserialize_json=True)
        sync_profiles01.init_storage_pipeline(opensearch_conn_info=opensearch_conn_info,
                                              redis_client_info=redis_client_info)


    op_init_storage_updated_profiles = PythonOperator(
        task_id='op_init_storage_updated_profiles',
        python_callable=init_storage_updated_profiles,
        provide_context=True,
    )

    op_scheduler_sync_github_profiles >> op_init_storage_updated_profiles >> op_do_sync_github_profiles
