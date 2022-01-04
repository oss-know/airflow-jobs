from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from ..libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, \
    NEED_SYNC_GITHUB_PULL_REPUESTS_REPOS

# v0.0.1

with DAG(
        dag_id='github_sync_pull_requests_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_sync_github_pull_requests(ds, **kwargs):
        return 'scheduler_sync_github_pull_requests'


    op_scheduler_sync_github_pull_requests = PythonOperator(
        task_id='op_scheduler_sync_github_pull_requests',
        python_callable=scheduler_sync_github_pull_requests
    )


    def do_sync_github_pull_requests(params):
        from airflow.models import Variable
        from ..libs.github import sync_pull_requests

        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
        opensearch_conn_infos = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

        owner = params["owner"]
        repo = params["repo"]

        pull_requests_numbers = sync_pull_requests.sync_github_pull_requests(
            github_tokens, opensearch_conn_infos, owner, repo)

        return pull_requests_numbers


    # def do_sync_github_pull_requests_comments(params, **kwargs):
    #     owner = params["owner"]
    #     repo = params["repo"]
    #
    #     ti = kwargs['ti']
    #     task_ids = f'op_do_sync_github_pull_requests_{owner}_{repo}'
    #     issues_numbers = ti.xcom_pull(task_ids=task_ids)
    #
    #     from airflow.models import Variable
    #     from libs.github import sync_issues_comments
    #
    #     github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    #     opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    #
    #     do_sync_since = sync_issues_comments.sync_github_issues_comments(
    #         github_tokens, opensearch_conn_info, owner, repo, issues_numbers)
    #
    #
    # def do_sync_github_issues_timelines(params, **kwargs):
    #     owner = params["owner"]
    #     repo = params["repo"]
    #
    #     ti = kwargs['ti']
    #     task_ids = f'op_do_sync_github_issues_{owner}_{repo}'
    #     issues_numbers = ti.xcom_pull(task_ids=task_ids)
    #
    #     from airflow.models import Variable
    #     from libs.github import sync_issues_timelines
    #
    #     github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    #     opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    #
    #     do_sync_since = sync_issues_timelines.sync_github_issues_timelines(
    #         github_tokens=github_tokens, opensearch_conn_info=opensearch_conn_info,
    #         owner=owner, repo=repo, issues_numbers=issues_numbers)

    from airflow.models import Variable

    need_do_sync_ops = []
    need_sync_github_pull_requests_repos = Variable.get(NEED_SYNC_GITHUB_PULL_REPUESTS_REPOS, deserialize_json=True)
    for sync_github_pull_requests_repo in need_sync_github_pull_requests_repos:
        op_do_sync_github_pull_requests = PythonOperator(
            task_id='op_do_sync_github_pull_requests_{owner}_{repo}'.format(
                owner=sync_github_pull_requests_repo["owner"],
                repo=sync_github_pull_requests_repo["repo"]),
            python_callable=do_sync_github_pull_requests,
            op_kwargs={'params': sync_github_pull_requests_repo},
            provide_context=True,
        )

        # op_do_sync_github_pull_requests_comments = PythonOperator(
        #     task_id='op_do_sync_github_issues_comments_{owner}_{repo}'.format(
        #         owner=sync_github_pull_requests_repo["owner"],
        #         repo=sync_github_pull_requests_repo["repo"]),
        #     python_callable=do_sync_github_pull_requests_comments,
        #     op_kwargs={'params': sync_github_pull_requests_repo},
        #     # provide_context=True,
        # )
        #
        # op_do_sync_github_issues_timelines = PythonOperator(
        #     task_id='op_do_sync_github_issues_timelines_{owner}_{repo}'.format(
        #         owner=sync_github_pull_requests_repo["owner"],
        #         repo=sync_github_pull_requests_repo["repo"]),
        #     python_callable=do_sync_github_issues_timelines,
        #     op_kwargs={'params': sync_github_pull_requests_repo},
        #     # provide_context=True,
        # )

        op_scheduler_sync_github_pull_requests >> op_do_sync_github_pull_requests
        # op_do_sync_github_pull_requests >> op_do_sync_github_pull_requests_comments
        # op_do_sync_github_pull_requests >> op_do_sync_github_issues_timelines
