from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch

from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, NEED_INIT_GITHUB_COMMITS_REPOS, \
    PROXY_CONFS
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

# irflow.providers.postgres.hooks.postgres
# v0.0.1 初始化实现
# v0.0.2 增加set_github_init_commits_check_data 用于设置初始化后更新的point data

with DAG(
        dag_id='github_init_commits_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_init_sync_github_commit(ds, **kwargs):

        return 'End::scheduler_init_sync_github_commit'


    op_scheduler_init_sync_github_commit = PythonOperator(
        task_id='scheduler_init_sync_github_commit',
        python_callable=scheduler_init_sync_github_commit
    )


    def do_init_github_commit(params):
        from oss_know.libs.github import init_commits

        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

        proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
        proxies = []
        for line in proxy_confs['reserved_proxies']:
            proxies.append(f'http://{line}')

        proxy_service = KuaiProxyService(proxy_confs['api_url'], proxy_confs['orderid'])
        proxy_manager = ProxyManager(proxies, proxy_service)
        token_manager = TokenManager(github_tokens)

        proxy_accommodator = GithubTokenProxyAccommodator(token_manager, proxy_manager, shuffle=True,
                                                          policy=GithubTokenProxyAccommodator.POLICY_FIXED_MAP)

        owner = params["owner"]
        repo = params["repo"]
        since = params["since"]
        until = params["until"]

        init_commits.init_github_commits(opensearch_conn_info, owner, repo, proxy_accommodator, since, until)
        return params


    need_do_inti_sync_ops = []

    need_init_sync_github_commits_list = Variable.get(NEED_INIT_GITHUB_COMMITS_REPOS, deserialize_json=True)

    for now_need_init_sync_github_commits in need_init_sync_github_commits_list:
        op_do_init_sync_github_commit = PythonOperator(
            task_id='do_init_github_commit_{owner}_{repo}'.format(
                owner=now_need_init_sync_github_commits["owner"],
                repo=now_need_init_sync_github_commits["repo"]),
            python_callable=do_init_github_commit,
            op_kwargs={'params': now_need_init_sync_github_commits},
        )
        op_scheduler_init_sync_github_commit >> op_do_init_sync_github_commit
