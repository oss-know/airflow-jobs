from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch

from oss_know.libs.base_dict.variable_key import NEED_INIT_GITHUB_ISSUES_REPOS, OPENSEARCH_CONN_DATA, GITHUB_TOKENS, \
    PROXY_CONFS
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

# v0.0.1
with DAG(
        dag_id='github_init_issues_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_init_sync_github_issues(ds, **kwargs):
        return 'End:scheduler_init_sync_github_issues'


    op_scheduler_init_github_issues = PythonOperator(
        task_id='op_scheduler_init_github_issues',
        python_callable=scheduler_init_sync_github_issues
    )


    def do_init_github_issues(params):
        from airflow.models import Variable
        from oss_know.libs.github import init_issues

        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
        opensearch_conn_infos = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

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
        since = None

        init_issues.init_github_issues(opensearch_conn_infos, owner, repo, proxy_accommodator, since)


    need_do_init_sync_ops = []



    from airflow.models import Variable

    need_init_github_issues_repos = Variable.get(NEED_INIT_GITHUB_ISSUES_REPOS, deserialize_json=True)

    for init_github_issues_repo in need_init_github_issues_repos:
        op_do_init_github_issues = PythonOperator(
            task_id='do_init_github_issues_{owner}_{repo}'.format(
                owner=init_github_issues_repo["owner"],
                repo=init_github_issues_repo["repo"]),
            python_callable=do_init_github_issues,
            op_kwargs={'params': init_github_issues_repo},
        )
        op_scheduler_init_github_issues >> op_do_init_github_issues
