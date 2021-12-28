from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import airflow.providers.postgres.hooks.postgres as postgres_hooks

# irflow.providers.postgres.hooks.postgres
# v0.0.1 初始化实现
# v0.0.2 增加set_github_init_commits_check_data 用于设置初始化后更新的point data

with DAG(
        dag_id='github_sync_commits_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_sync_github_commit(ds, **kwargs):
        return 'End::scheduler_init_sync_github_commit'


    op_scheduler_sync_github_commit = PythonOperator(
        task_id='op_scheduler_sync_github_commit',
        python_callable=scheduler_sync_github_commit
    )


    def do_sync_github_commit(params):
        from airflow.models import Variable
        from libs.github import sync_github_commits

        github_tokens = Variable.get("github_tokens", deserialize_json=True)
        opensearch_conn_info = Variable.get("opensearch_conn_data", deserialize_json=True)

        owner = params["owner"]
        repo = params["repo"]

        do_init_sync_info = sync_github_commits.sync_github_commits(github_tokens,
                                                                    opensearch_conn_info,
                                                                    owner, repo)
        return "END::do_sync_github_commit"


    need_do_inti_sync_ops = []

    from airflow.models import Variable

    need_sync_github_commits_list = Variable.get("need_sync_github_commits", deserialize_json=True)

    for now_need_sync_github_commits in need_sync_github_commits_list:
        op_do_sync_github_commit = PythonOperator(
            task_id='op_do_sync_github_commit_{owner}_{repo}'.format(
                owner=now_need_sync_github_commits["owner"],
                repo=now_need_sync_github_commits["repo"]),
            python_callable=do_sync_github_commit,
            op_kwargs={'params': now_need_sync_github_commits},
        )
        op_scheduler_sync_github_commit >> op_do_sync_github_commit
