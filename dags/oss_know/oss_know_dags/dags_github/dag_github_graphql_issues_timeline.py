# -*-coding:utf-8-*-
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from oss_know.libs.base_dict.variable_key import PROXY_CONFS, \
    OPENSEARCH_CONN_DATA, GITHUB_TOKENS
from oss_know.libs.github.init_sync_github_graphql_timeline import init_graphql_pr_timeline
from oss_know.libs.github.init_sync_repo_languages import init_sync_repo_languages
from oss_know.libs.util.proxy import GithubTokenProxyAccommodator, make_accommodator, \
    ProxyServiceProvider

with DAG(
        dag_id='github_init_github_graphql_issues_timeline',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:

    from airflow.models import Variable

    need_init_github_graphql_timeline_list = Variable.get("need_init_github_graphql_timeline_list",
                                                                 deserialize_json=True)

    for now_need_sync_owner_repo in need_init_github_graphql_timeline_list:
        for issue_type in ['issues','pullRequests']:
            @task(task_id=f'{now_need_sync_owner_repo["owner"]}-{now_need_sync_owner_repo["repo"]}_{issue_type}')
            def do_init_github_graphql_timeline():
                from airflow.models import Variable

                opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

                github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
                proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
                proxy_accommodator = make_accommodator(github_tokens, proxy_confs, ProxyServiceProvider.Kuai,
                                                       GithubTokenProxyAccommodator.POLICY_FIXED_MAP)
                owner = now_need_sync_owner_repo["owner"]
                repo = now_need_sync_owner_repo["repo"]
                init_graphql_pr_timeline(opensearch_conn_info, owner, repo, proxy_accommodator, issue_type=issue_type)


            op_do_sync_issues_timeline = do_init_github_graphql_timeline()


