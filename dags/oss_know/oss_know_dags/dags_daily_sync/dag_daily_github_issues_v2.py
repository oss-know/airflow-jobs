from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES, \
    OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS, OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, PROXY_CONFS, \
    DAILY_SYNC_GITHUB_ISSUES_EXCLUDES, CLICKHOUSE_DRIVER_INFO, CK_TABLE_DEFAULT_VAL_TPLT, \
    DAILY_SYNC_GITHUB_ISSUES_INCLUDES
from oss_know.libs.github import sync_issues, sync_issues_comments, sync_issues_timelines
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.data_transfer import sync_clickhouse_from_opensearch
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

OP_SYNC_ISSUE_OS_PREFIX = 'op_sync_github_issues_opensearch'

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

    clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
    table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
    github_issue_table_template = table_templates.get(OPENSEARCH_INDEX_GITHUB_ISSUES)
    issue_comment_table_template = table_templates.get(OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS)
    issue_timeline_table_template = table_templates.get(OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE)


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


    def do_sync_github_issues_opensearch(owner, repo):
        proxy_accommodator = get_proxy_accommodator()
        issues_numbers, pr_numbers = sync_issues.sync_github_issues(opensearch_conn_info=opensearch_conn_info,
                                                                    owner=owner,
                                                                    repo=repo,
                                                                    token_proxy_accommodator=proxy_accommodator)

        return issues_numbers


    def do_sync_github_issues_clickhouse(owner, repo):
        sync_clickhouse_from_opensearch(owner, repo, OPENSEARCH_INDEX_GITHUB_ISSUES, opensearch_conn_info,
                                        OPENSEARCH_INDEX_GITHUB_ISSUES, clickhouse_conn_info,
                                        github_issue_table_template)


    def do_sync_github_issues_comments_opensearch(owner, repo, **kwargs):
        ti = kwargs['ti']
        task_ids = f'{OP_SYNC_ISSUE_OS_PREFIX}_{owner}_{repo}'
        issues_numbers = ti.xcom_pull(task_ids=task_ids)

        proxy_accommodator = get_proxy_accommodator()
        do_sync_since = sync_issues_comments.sync_github_issues_comments(
            opensearch_conn_info=opensearch_conn_info,
            owner=owner,
            repo=repo,
            token_proxy_accommodator=proxy_accommodator,
            issues_numbers=issues_numbers
        )


    def do_sync_github_issues_comments_clickhouse(owner, repo):
        sync_clickhouse_from_opensearch(owner, repo,
                                        OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS, opensearch_conn_info,
                                        OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS, clickhouse_conn_info,
                                        issue_comment_table_template)


    def do_sync_github_issues_timelines_opensearch(owner, repo, **kwargs):
        ti = kwargs['ti']
        task_ids = f'{OP_SYNC_ISSUE_OS_PREFIX}_{owner}_{repo}'
        issues_numbers = ti.xcom_pull(task_ids=task_ids)

        proxy_accommodator = get_proxy_accommodator()
        sync_issues_timelines.sync_github_issues_timelines(
            opensearch_conn_info=opensearch_conn_info,
            owner=owner,
            repo=repo,
            token_proxy_accommodator=proxy_accommodator,
            issues_numbers=issues_numbers)


    def do_sync_github_issues_timeline_clickhouse(owner, repo):
        sync_clickhouse_from_opensearch(owner, repo,
                                        OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE, opensearch_conn_info,
                                        OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE, clickhouse_conn_info,
                                        issue_timeline_table_template)


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    uniq_owner_repos = []
    includes = Variable.get(DAILY_SYNC_GITHUB_ISSUES_INCLUDES, deserialize_json=True, default_var=None)
    if not includes:
        excludes = Variable.get(DAILY_SYNC_GITHUB_ISSUES_EXCLUDES, deserialize_json=True, default_var=None)
        uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, OPENSEARCH_INDEX_GITHUB_ISSUES,
                                                               excludes)
    else:
        for owner_repo_str, origin in includes.items():
            owner, repo = owner_repo_str.split('::')
            uniq_owner_repos.append({
                'owner': owner,
                'repo': repo,
                'origin': origin
            })

    for uniq_owner_repo in uniq_owner_repos:
        owner = uniq_owner_repo['owner']
        repo = uniq_owner_repo['repo']

        op_sync_github_issues_opensearch = PythonOperator(
            task_id=f'{OP_SYNC_ISSUE_OS_PREFIX}_{owner}_{repo}',
            python_callable=do_sync_github_issues_opensearch,
            op_kwargs=uniq_owner_repo,
            provide_context=True,
        )
        op_sync_github_issues_clickhouse = PythonOperator(
            task_id=f'op_sync_github_issues_clickhouse_{owner}_{repo}',
            python_callable=do_sync_github_issues_clickhouse,
            op_kwargs=uniq_owner_repo,
            provide_context=True,
        )

        op_sync_github_issues_comments_opensearch = PythonOperator(
            task_id=f'op_sync_github_issues_comments_opensearch_{owner}_{repo}',
            python_callable=do_sync_github_issues_comments_opensearch,
            op_kwargs=uniq_owner_repo,
            # provide_context=True,
        )
        op_sync_github_issues_comments_clickhouse = PythonOperator(
            task_id=f'op_sync_github_issues_comments_clickhouse_{owner}_{repo}',
            python_callable=do_sync_github_issues_comments_clickhouse,
            op_kwargs=uniq_owner_repo,
            # provide_context=True,
        )

        op_sync_github_issues_timelines_opensearch = PythonOperator(
            task_id=f'op_sync_github_issues_timelines_opensearch_{owner}_{repo}',
            python_callable=do_sync_github_issues_timelines_opensearch,
            op_kwargs=uniq_owner_repo,
            # provide_context=True,
        )
        op_sync_github_issues_timelines_clickhouse = PythonOperator(
            task_id=f'op_sync_github_issues_timelines_clickhouse_{owner}_{repo}',
            python_callable=do_sync_github_issues_timeline_clickhouse,
            op_kwargs=uniq_owner_repo,
            # provide_context=True,
        )

        op_sync_github_issues_opensearch >> op_sync_github_issues_clickhouse

        op_sync_github_issues_opensearch >> op_sync_github_issues_comments_opensearch
        op_sync_github_issues_opensearch >> op_sync_github_issues_timelines_opensearch

        op_sync_github_issues_comments_opensearch >> op_sync_github_issues_comments_clickhouse
        op_sync_github_issues_timelines_opensearch >> op_sync_github_issues_timelines_clickhouse
