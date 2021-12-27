from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# v0.0.1

with DAG(
        dag_id='github_init_pull_requests_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_init_sync_github_pull_requests(ds, **kwargs):
        return 'End:scheduler_init_sync_github_pull_requests'


    op_scheduler_init_sync_github_pull_requests = PythonOperator(
        task_id='op_scheduler_init_sync_github_pull_requests',
        python_callable=scheduler_init_sync_github_pull_requests
    )


    def do_init_sync_github_pull_requests(params):
        from airflow.models import Variable
        from libs.github import init_pull_requests

        github_tokens = Variable.get("github_tokens", deserialize_json=True)
        opensearch_conn_info = Variable.get("opensearch_conn_data", deserialize_json=True)

        owner = params["owner"]
        repo = params["repo"]
        # since = params["since"]
        since = None

        do_init_sync_info = init_pull_requests.init_sync_github_pull_requests(
            github_tokens, opensearch_conn_info, owner, repo, since)

        return "End:do_init_sync_github_pull_requests"


    need_do_init_sync_ops = []

    from airflow.models import Variable

    need_init_sync_github_pull_requests_repos = Variable.get("need_init_sync_github_pull_requests_list", deserialize_json=True)

    for init_sync_github_pull_requests_repo in need_init_sync_github_pull_requests_repos:
        op_do_init_sync_github_pull_requests = PythonOperator(
            task_id='op_do_init_sync_github_pull_requests_{owner}_{repo}'.format(
                owner=init_sync_github_pull_requests_repo["owner"],
                repo=init_sync_github_pull_requests_repo["repo"]),
            python_callable=do_init_sync_github_pull_requests,
            op_kwargs={'params': init_sync_github_pull_requests_repo},
        )
        op_scheduler_init_sync_github_pull_requests >> op_do_init_sync_github_pull_requests
