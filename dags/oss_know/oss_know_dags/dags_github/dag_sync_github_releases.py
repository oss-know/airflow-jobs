from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_COMMITS
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, PROXY_CONFS, \
    SYNC_GITHUB_RELEASE_REPOS, CLICKHOUSE_DRIVER_INFO
from oss_know.libs.github.releases import sync_releases
from oss_know.libs.util.base import arrange_owner_repo_into_letter_groups
from oss_know.libs.util.clickhouse import get_uniq_owner_repos
from oss_know.libs.util.proxy import GithubTokenProxyAccommodator, make_accommodator, \
    ProxyServiceProvider

with DAG(
        dag_id='github_sync_releases',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    proxy_accommodator = make_accommodator(github_tokens, proxy_confs, ProxyServiceProvider.Kuai,
                                           GithubTokenProxyAccommodator.POLICY_FIXED_MAP)


    def do_sync_github_releases_opensearch_group(owner_repo_group):
        for item in owner_repo_group:
            owner = item['owner']
            repo = item['repo']
            sync_releases(
                opensearch_conn_info=opensearch_conn_info,
                owner=owner,
                repo=repo,
                token_proxy_accommodator=proxy_accommodator)


    all_owner_repos = Variable.get(SYNC_GITHUB_RELEASE_REPOS, deserialize_json=True, default_var=None)
    if not all_owner_repos:
        # 如果没有在variable中指定需要计算metrics的项目则从库中获取全量的去重后的owner_repo 计算
        clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        all_owner_repos = get_uniq_owner_repos(clickhouse_conn_info, OPENSEARCH_INDEX_GITHUB_COMMITS)

    task_groups_by_capital_letter = arrange_owner_repo_into_letter_groups(all_owner_repos)
    for letter, owner_repos in task_groups_by_capital_letter.items():
        op_sync_github_pr_opensearch = PythonOperator(
            task_id=f'op_sync_github_releases_opensearch_group_{letter}',
            python_callable=do_sync_github_releases_opensearch_group,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
