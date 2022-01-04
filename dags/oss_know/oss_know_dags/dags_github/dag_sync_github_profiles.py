from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS

# v0.0.1

with DAG(
        dag_id='github_sync_profiles_v1',
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
        from oss_know.libs.github import sync_profiles01os

        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
        opensearch_conn_infos = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        sync_profiles01os.sync_github_profiles(github_tokens, opensearch_conn_infos)


    op_do_sync_github_profiles = PythonOperator(
        task_id='op_do_sync_github_profiles',
        python_callable=do_sync_github_profiles,
        provide_context=True,
    )

    op_scheduler_sync_github_profiles >> op_do_sync_github_profiles
