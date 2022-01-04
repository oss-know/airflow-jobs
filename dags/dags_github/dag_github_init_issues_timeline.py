from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# v0.0.1
from ..libs.base_dict.variable_key import NEED_INIT_GITHUB_ISSUES_TIMELINE_REPOS, GITHUB_TOKENS, OPENSEARCH_CONN_DATA

with DAG(
        dag_id='github_init_issues_timeline_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_init_github_issues_timeline(ds, **kwargs):
        return 'End:scheduler_init_github_issues_timeline'


    op_scheduler_init_github_issues_timeline = PythonOperator(
        task_id='op_scheduler_init_github_issues_timeline',
        python_callable=scheduler_init_github_issues_timeline
    )


    def do_init_github_issues_timeline(params):
        from airflow.models import Variable
        from ..libs.github import init_issues_timeline

        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

        owner = params["owner"]
        repo = params["repo"]
        # since = params["since"]
        since = None

        do_init_sync_info = init_issues_timeline.init_sync_github_issues_timeline(
            github_tokens, opensearch_conn_info, owner, repo, since)

        return params


    need_do_init_ops = []

    from airflow.models import Variable

    need_init_github_issues_timeline_repos = Variable.get(NEED_INIT_GITHUB_ISSUES_TIMELINE_REPOS,
                                                          deserialize_json=True)

    for need_init_github_issues_timeline_repo in need_init_github_issues_timeline_repos:
        op_do_init_github_issues_timeline = PythonOperator(
            task_id='op_do_init_github_issues_timeline_{owner}_{repo}'.format(
                owner=need_init_github_issues_timeline_repo["owner"],
                repo=need_init_github_issues_timeline_repo["repo"]),
            python_callable=do_init_github_issues_timeline,
            op_kwargs={'params': need_init_github_issues_timeline_repo},
        )
        op_scheduler_init_github_issues_timeline >> op_do_init_github_issues_timeline
