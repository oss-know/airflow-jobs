from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS
from loguru import logger

# v0.0.1

with DAG(
        dag_id='github_sync_profiles_v101',
        # schedule_interval='0 0 1 * *',
        # TODO：生产环境设定定时,更改schedule_interval的值
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_sync_github_profiles(ds, **kwargs):
        return 'End:scheduler_sync_github_profiles'


    op_scheduler_sync_github_profiles = PythonOperator(
        task_id='op_scheduler_sync_github_profiles01',
        python_callable=scheduler_sync_github_profiles
    )


    def do_sync_github_profiles(params):
        from airflow.models import Variable
        from oss_know.libs.github import sync_profiles02os

        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
        opensearch_conn_infos = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        sync_profiles02os.sync_github_profiles(github_tokens, opensearch_conn_infos, params)
        logger.info(f"检查更新GitHub login以{params}开头的profile")


    for i in [chr(x) for x in range(ord('a'), ord('z') + 1)]+[str(y) for y in range(9)]+\
             [chr(x) for x in range(ord('A'), ord('Z') + 1)]:
        op_do_sync_github_profiles = PythonOperator(
            task_id=f'op_do_sync_github_profiles_start_with_{i}',
            python_callable=do_sync_github_profiles,
            op_kwargs={'params': i},
            provide_context=True,
        )

    op_scheduler_sync_github_profiles >> op_do_sync_github_profiles
