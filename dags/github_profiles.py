import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
        dag_id='github_profile_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def start_load_github_profile(ds, **kwargs):
        return 'End start_load_github_profile'
    op_start_load_github_profile = PythonOperator(
        task_id='load_github_profile_info',
        python_callable=start_load_github_profile,
    )

    def load_github_repo_profile(params):
        from airflow.models import Variable
        from libs.github import profiles

        github_tokens = Variable.get("github_infos", deserialize_json=True)
        opensearch_conn_infos = Variable.get("opensearch_conn_data", deserialize_json=True)

        owner = params["owner"]
        repo = params["repo"]

        do_init_sync_profile = profiles.load_github_profile(github_tokens, opensearch_conn_infos, owner, repo)

        print(do_init_sync_profile)
        return 'End load_github_repo_profile'


    need_sync_github_profile_repos = Variable.get("need_sync_github_profile_repo_list", deserialize_json=True)

    for now_need_sync_github_profile_repos in need_sync_github_profile_repos:
        op_load_github_repo_profile = PythonOperator(
            task_id='op_load_github_repo_profile_{owner}_{repo}'.format(
                owner=now_need_sync_github_profile_repos["owner"],
                repo=now_need_sync_github_profile_repos["repo"]),
            python_callable=load_github_repo_profile,
            op_kwargs={'params': now_need_sync_github_profile_repos},
        )

        op_start_load_github_profile >> op_load_github_repo_profile
