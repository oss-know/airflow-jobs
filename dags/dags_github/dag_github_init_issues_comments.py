from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# v0.0.1
from ..libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS

NEED_INIT_SYNC_GITHUB_ISSUES_COMMENTS_REPOS = "need_init_github_issues_comments_repos"

with DAG(
        dag_id='github_init_issues_comments_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_init_github_issues_comments(ds, **kwargs):
        return 'End:scheduler_init_github_issues_comments'


    op_scheduler_init_github_issues_comments = PythonOperator(
        task_id='op_scheduler_init_github_issues_comments',
        python_callable=scheduler_init_github_issues_comments
    )


    def do_init_github_issues_comments(params):
        from airflow.models import Variable
        from ..libs.github import init_issues_comments

        # OPENSEARCH_CONN_DATA = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        # opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

        owner = params["owner"]
        repo = params["repo"]

        do_init_sync_info = init_issues_comments.init_github_issues_comments(
            github_tokens, opensearch_conn_info, owner, repo)

        return params


    need_do_init_ops = []

    from airflow.models import Variable

    need_init_github_issues_comments_repos = Variable.get(NEED_INIT_SYNC_GITHUB_ISSUES_COMMENTS_REPOS,
                                                          deserialize_json=True)

    for need_init_github_issues_comments_repo in need_init_github_issues_comments_repos:
        op_do_init_github_issues_comments = PythonOperator(
            task_id='op_do_init_github_issues_comments_{owner}_{repo}'.format(
                owner=need_init_github_issues_comments_repo["owner"],
                repo=need_init_github_issues_comments_repo["repo"]),
            python_callable=do_init_github_issues_comments,
            op_kwargs={'params': need_init_github_issues_comments_repo},
        )
        op_scheduler_init_github_issues_comments >> op_do_init_github_issues_comments
