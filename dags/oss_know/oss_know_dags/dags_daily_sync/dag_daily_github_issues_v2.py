from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, PROXY_CONFS, \
    DAILY_SYNC_GITHUB_ISSUES_EXCLUDES
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES
from oss_know.libs.github import sync_issues, sync_issues_comments, sync_issues_timelines
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

with DAG(
        dag_id='daily_github_issues_sync_v2',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github', 'daily sync'],
) as dag:
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)


    def scheduler_sync_github_issues(ds, **kwargs):
        return 'End:scheduler_sync_github_issues'


    op_scheduler_sync_github_issues = PythonOperator(
        task_id='op_scheduler_sync_github_issues',
        python_callable=scheduler_sync_github_issues
    )


    def get_proxy_accommodator():

        proxy_api_url = proxy_confs["api_url"]
        proxy_order_id = proxy_confs["orderid"]
        proxy_reserved_proxies = proxy_confs["reserved_proxies"]
        proxies = []
        for proxy in proxy_reserved_proxies:
            proxies.append(f"http://{proxy}")
        proxy_service = KuaiProxyService(api_url=proxy_api_url,
                                         orderid=proxy_order_id)
        token_manager = TokenManager(tokens=github_tokens)
        proxy_manager = ProxyManager(proxies=proxies,
                                     proxy_service=proxy_service)
        proxy_accommodator = GithubTokenProxyAccommodator(token_manager=token_manager,
                                                          proxy_manager=proxy_manager,
                                                          shuffle=True,
                                                          policy=GithubTokenProxyAccommodator.POLICY_FIXED_MAP)
        return proxy_accommodator


    def do_sync_github_issues(params):

        proxy_accommodator = get_proxy_accommodator()
        owner = params["owner"]
        repo = params["repo"]

        issues_numbers, pr_numbers = sync_issues.sync_github_issues(opensearch_conn_info=opensearch_conn_info,
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

        proxy_accommodator = get_proxy_accommodator()
        do_sync_since = sync_issues_comments.sync_github_issues_comments(
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

        proxy_accommodator = get_proxy_accommodator()
        do_sync_since = sync_issues_timelines.sync_github_issues_timelines(
            opensearch_conn_info=opensearch_conn_info,
            owner=owner,
            repo=repo,
            token_proxy_accommodator=proxy_accommodator,
            issues_numbers=issues_numbers)


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    excludes = Variable.get(DAILY_SYNC_GITHUB_ISSUES_EXCLUDES, deserialize_json=True, default_var=None)
    uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, OPENSEARCH_INDEX_GITHUB_ISSUES, excludes)
    for uniq_owner_repo in uniq_owner_repos:
        owner = uniq_owner_repo['owner']
        repo = uniq_owner_repo['repo']

        op_do_sync_github_issues = PythonOperator(
            task_id=f'op_do_sync_github_issues_{owner}_{repo}',
            python_callable=do_sync_github_issues,
            op_kwargs={'params': uniq_owner_repo},
            provide_context=True,
        )

        op_do_sync_github_issues_comments = PythonOperator(
            task_id=f'op_do_sync_github_issues_comments_{owner}_{repo}',
            python_callable=do_sync_github_issues_comments,
            op_kwargs={'params': uniq_owner_repo},
            # provide_context=True,
        )

        op_do_sync_github_issues_timelines = PythonOperator(
            task_id=f'op_do_sync_github_issues_timelines_{owner}_{repo}',
            python_callable=do_sync_github_issues_timelines,
            op_kwargs={'params': uniq_owner_repo},
            # provide_context=True,
        )

        op_scheduler_sync_github_issues >> op_do_sync_github_issues
        op_do_sync_github_issues >> op_do_sync_github_issues_comments
        op_do_sync_github_issues >> op_do_sync_github_issues_timelines
