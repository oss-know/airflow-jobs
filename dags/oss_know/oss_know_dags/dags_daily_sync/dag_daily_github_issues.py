from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES
from oss_know.libs.base_dict.variable_key import GITHUB_TOKENS, OPENSEARCH_CONN_DATA, PROXY_CONFS, \
    DAILY_SYNC_GITHUB_ISSUES_EXCLUDES, CLICKHOUSE_DRIVER_INFO, CK_TABLE_DEFAULT_VAL_TPLT, \
    DAILY_SYNC_GITHUB_ISSUES_INCLUDES
from oss_know.libs.github.sync_issues import sync_github_issues
from oss_know.libs.util.base import get_opensearch_client, arrange_owner_repo_into_letter_groups
from oss_know.libs.util.data_transfer import sync_clickhouse_repos_from_opensearch
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

with DAG(dag_id='daily_github_issues_sync',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync']) as dag:
    def op_init_daily_github_issues_sync():
        return 'Start init_daily_github_issues_sync'


    op_init_daily_github_issues_sync = PythonOperator(task_id='op_init_daily_github_issues_sync',
                                                      python_callable=op_init_daily_github_issues_sync)

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

    clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
    table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
    github_issue_table_template = table_templates.get(OPENSEARCH_INDEX_GITHUB_ISSUES)


    def do_sync_github_issues_opensearch_group(owner_repo_group):
        for item in owner_repo_group:
            owner = item['owner']
            repo = item['repo']
            sync_github_issues(opensearch_conn_info, owner, repo, proxy_accommodator)
        return 'do_sync_github_issues:::end'


    def do_sync_github_issues_clickhouse_group(owner_repo_group):
        sync_clickhouse_repos_from_opensearch(owner_repo_group,
                                              OPENSEARCH_INDEX_GITHUB_ISSUES, opensearch_conn_info,
                                              OPENSEARCH_INDEX_GITHUB_ISSUES, clickhouse_conn_info,
                                              github_issue_table_template)


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

    task_groups_by_capital_letter = arrange_owner_repo_into_letter_groups(uniq_owner_repos)
    # prev_op = op_init_daily_github_issues_sync
    for letter, owner_repos in task_groups_by_capital_letter.items():
        op_sync_github_issues_opensearch = PythonOperator(
            task_id=f'op_sync_github_issues_opensearch_group_{letter}',
            python_callable=do_sync_github_issues_opensearch_group,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
        op_sync_github_issues_clickhouse = PythonOperator(
            task_id=f'op_sync_github_issues_clickhouse_group_{letter}',
            python_callable=do_sync_github_issues_clickhouse_group,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
        op_init_daily_github_issues_sync >> op_sync_github_issues_opensearch >> op_sync_github_issues_clickhouse
        # prev_op = op_sync_github_issues_clickhouse_group
