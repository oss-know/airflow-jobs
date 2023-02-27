from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_COMMITS
from oss_know.libs.base_dict.variable_key import GITHUB_TOKENS, OPENSEARCH_CONN_DATA, PROXY_CONFS, \
    DAILY_SYNC_GITHUB_COMMITS_EXCLUDES, CLICKHOUSE_DRIVER_INFO, CK_TABLE_DEFAULT_VAL_TPLT, \
    DAILY_SYNC_GITHUB_COMMITS_INCLUDES
from oss_know.libs.github.sync_commits import sync_github_commits_opensearch
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.data_transfer import sync_clickhouse_from_opensearch
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
github_commits_table_template = table_templates.get(OPENSEARCH_INDEX_GITHUB_COMMITS)

with DAG(dag_id='daily_github_commits_sync',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync']) as dag:
    def op_init_daily_github_commits_sync():
        return 'Start init_daily_github_commits_sync'


    op_init_daily_github_commits_sync = PythonOperator(task_id='op_init_daily_github_commits_sync',
                                                       python_callable=op_init_daily_github_commits_sync)

    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    proxy_api_url = proxy_confs["api_url"]
    proxy_order_id = proxy_confs["orderid"]
    proxy_reserved_proxies = proxy_confs["reserved_proxies"]
    proxies = []
    for proxy in proxy_reserved_proxies:
        proxies.append(f"http://{proxy}")
    proxy_service = KuaiProxyService(api_url=proxy_api_url, orderid=proxy_order_id)
    token_manager = TokenManager(tokens=github_tokens)
    proxy_manager = ProxyManager(proxies=proxies, proxy_service=proxy_service)
    proxy_accommodator = GithubTokenProxyAccommodator(token_manager=token_manager,
                                                      proxy_manager=proxy_manager, shuffle=True,
                                                      policy=GithubTokenProxyAccommodator.POLICY_FIXED_MAP)


    def do_sync_github_commits_opensearch(owner, repo):
        sync_github_commits_opensearch(opensearch_conn_info, owner, repo, proxy_accommodator)
        return 'do_sync_github_commits:::end'


    def do_sync_github_commits_clickhouse(owner, repo):
        sync_clickhouse_from_opensearch(owner, repo,
                                        OPENSEARCH_INDEX_GITHUB_COMMITS, opensearch_conn_info,
                                        OPENSEARCH_INDEX_GITHUB_COMMITS, clickhouse_conn_info,
                                        github_commits_table_template)


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    uniq_owner_repos = []
    includes = Variable.get(DAILY_SYNC_GITHUB_COMMITS_INCLUDES, deserialize_json=True, default_var=None)
    if not includes:
        excludes = Variable.get(DAILY_SYNC_GITHUB_COMMITS_EXCLUDES, deserialize_json=True, default_var=None)
        uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, OPENSEARCH_INDEX_GITHUB_COMMITS,
                                                               excludes)
    else:
        for owner_repo_str, origin in includes.items():
            owner, repo = owner_repo_str.split('::')
            uniq_owner_repos.append({
                'owner': owner,
                'repo': repo,
                'origin': origin
            })

    for uniq_item in uniq_owner_repos:
        owner = uniq_item['owner']
        repo = uniq_item['repo']

        op_sync_github_commits_opensearch = PythonOperator(
            task_id=f'do_sync_github_commits_opensearch_{owner}_{repo}',
            python_callable=do_sync_github_commits_opensearch,
            op_kwargs={
                "owner": owner,
                "repo": repo
            })
        op_sync_github_commits_clickhouse = PythonOperator(
            task_id=f'do_sync_github_commits_clickhouse_{owner}_{repo}',
            python_callable=do_sync_github_commits_clickhouse,
            op_kwargs={
                "owner": owner,
                "repo": repo
            })

        op_init_daily_github_commits_sync >> op_sync_github_commits_opensearch >> op_sync_github_commits_clickhouse
