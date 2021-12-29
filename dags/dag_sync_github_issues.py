from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# v0.0.1

with DAG(
        dag_id='github_sync_issues_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_sync_github_issues(ds, **kwargs):
        return 'End:scheduler_sync_github_issues'


    op_scheduler_sync_github_issues = PythonOperator(
        task_id='op_scheduler_sync_github_issues',
        python_callable=scheduler_sync_github_issues
    )


    def do_sync_github_issues(params):
        from airflow.models import Variable
        from libs.github import sync_issues

        github_tokens = Variable.get("github_tokens", deserialize_json=True)
        opensearch_conn_infos = Variable.get("opensearch_conn_data", deserialize_json=True)

        owner = params["owner"]
        repo = params["repo"]

        issues_numbers = sync_github_issues.sync_github_issues(
            github_tokens, opensearch_conn_infos, owner, repo)

        return issues_numbers


    def do_sync_github_issues_comments(params, **kwargs):
        owner = params["owner"]
        repo = params["repo"]

        ti = kwargs['ti']
        task_ids = f'op_do_sync_github_issues_{owner}_{repo}'
        issues_numbers = ti.xcom_pull(task_ids=task_ids)

        from airflow.models import Variable
        from libs.github import sync_issues_comments

        github_tokens = Variable.get("github_tokens", deserialize_json=True)
        opensearch_conn_info = Variable.get("opensearch_conn_data", deserialize_json=True)


        do_sync_since = sync_github_issues_comments.sync_github_issues_comments(
            github_tokens, opensearch_conn_info, owner, repo, issues_numbers)

    from airflow.models import Variable
    need_do_sync_ops = []
    need_sync_github_issues_repos = Variable.get("need_sync_github_issues", deserialize_json=True)
    for sync_github_issues_repo in need_sync_github_issues_repos:
        op_do_sync_github_issues = PythonOperator(
            task_id='op_do_sync_github_issues_{owner}_{repo}'.format(
                owner=sync_github_issues_repo["owner"],
                repo=sync_github_issues_repo["repo"]),
            python_callable=do_sync_github_issues,
            op_kwargs={'params': sync_github_issues_repo},
            provide_context=True,
        )

        op_do_sync_github_issues_comments = PythonOperator(
            task_id='op_do_sync_github_issues_comments_{owner}_{repo}'.format(
                owner=sync_github_issues_repo["owner"],
                repo=sync_github_issues_repo["repo"]),
            python_callable=do_sync_github_issues_comments,
            op_kwargs={'params': sync_github_issues_repo},
            # provide_context=True,
        )

        op_scheduler_sync_github_issues >> op_do_sync_github_issues
        op_do_sync_github_issues >> op_do_sync_github_issues_comments
