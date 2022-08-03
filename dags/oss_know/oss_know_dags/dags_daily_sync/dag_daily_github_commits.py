from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import GITHUB_TOKENS, OPENSEARCH_CONN_DATA, PROXY_CONFS
from oss_know.libs.github.sync_commits import sync_github_commits
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

with DAG(dag_id='daily_github_commits_sync',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync']) as dag:
    def op_init_daily_gits_sync():
        return 'Start init_daily_github_commits_sync'


    op_init_daily_github_commits_sync = PythonOperator(task_id='op_init_daily_gits_sync',
                                                       python_callable=op_init_daily_gits_sync)

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


    def do_sync_github_commits(params):
        owner = params["owner"]
        repo = params["repo"]

        sync_github_commits(opensearch_conn_info, owner, repo, proxy_accommodator)
        return 'do_sync_github_commits:::end'


    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, 'github_commits')
    for uniq_item in uniq_owner_repos:
        owner = uniq_item['owner']
        repo = uniq_item['repo']

        op_do_sync_github_commits = PythonOperator(task_id=f'do_sync_github_commits_{owner}_{repo}',
                                                   python_callable=do_sync_github_commits,
                                                   op_kwargs={
                                                       'params': {
                                                           "owner": owner, "repo": repo
                                                       }
                                                   })
        op_init_daily_github_commits_sync >> op_do_sync_github_commits
