from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import PROXY_CONFS, GITHUB_TOKENS, \
    OPENSEARCH_CONN_DATA, NEED_SYNC_GITHUB_ISSUES_REPOS
from oss_know.libs.github import sync_issues
from oss_know.libs.github import sync_issues_comments
from oss_know.libs.github import sync_issues_timelines

# v0.0.1
from oss_know.libs.util.proxy import GithubTokenProxyAccommodator, make_accommodator, \
    ProxyServiceProvider

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

    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    proxy_accommodator = make_accommodator(github_tokens, proxy_confs, ProxyServiceProvider.Kuai,
                                           GithubTokenProxyAccommodator.POLICY_FIXED_MAP)


    def do_sync_github_issues(params):
        opensearch_conn_infos = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

        owner = params["owner"]
        repo = params["repo"]

        issues_numbers, pr_numbers = sync_issues.sync_github_issues(opensearch_conn_info=opensearch_conn_infos,
                                                                    owner=owner,
                                                                    repo=repo,
                                                                    token_proxy_accommodator=proxy_accommodator)

        return issues_numbers


    def do_sync_github_issues_comments(params, **kwargs):
        owner = params["owner"]
        repo = params["repo"]

        ti = kwargs['ti']
        task_ids = f'op_do_sync_github_issues_{owner}_{repo}'
        issues_numbers = ti.xcom_pull(task_ids=task_ids)

        sync_issues_comments.sync_github_issues_comments(
            opensearch_conn_info=opensearch_conn_info,
            owner=owner,
            repo=repo,
            token_proxy_accommodator=proxy_accommodator,
            issues_numbers=issues_numbers
        )


    def do_sync_github_issues_timelines(params, **kwargs):
        owner = params["owner"]
        repo = params["repo"]

        ti = kwargs['ti']
        task_ids = f'op_do_sync_github_issues_{owner}_{repo}'
        issues_numbers = ti.xcom_pull(task_ids=task_ids)

        sync_issues_timelines.sync_github_issues_timelines(
            opensearch_conn_info=opensearch_conn_info,
            owner=owner,
            repo=repo,
            token_proxy_accommodator=proxy_accommodator,
            issues_numbers=issues_numbers)


    need_do_sync_ops = []
    need_sync_github_issues_repos = Variable.get(NEED_SYNC_GITHUB_ISSUES_REPOS, deserialize_json=True)
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

        op_do_sync_github_issues_timelines = PythonOperator(
            task_id='op_do_sync_github_issues_timelines_{owner}_{repo}'.format(
                owner=sync_github_issues_repo["owner"],
                repo=sync_github_issues_repo["repo"]),
            python_callable=do_sync_github_issues_timelines,
            op_kwargs={'params': sync_github_issues_repo},
            # provide_context=True,
        )

        op_scheduler_sync_github_issues >> op_do_sync_github_issues
        op_do_sync_github_issues >> op_do_sync_github_issues_comments
        op_do_sync_github_issues >> op_do_sync_github_issues_timelines
