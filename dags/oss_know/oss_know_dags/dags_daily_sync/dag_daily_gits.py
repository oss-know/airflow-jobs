from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.base_dict.variable_key import DAILY_SYNC_GITS_EXCLUDES, DAILY_SYNC_GITS_INCLUDES, \
    CK_TABLE_DEFAULT_VAL_TPLT, OPENSEARCH_CONN_DATA, CLICKHOUSE_DRIVER_INFO
from oss_know.libs.github.sync_gits import sync_gits_opensearch
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.data_transfer import sync_clickhouse_from_opensearch
from oss_know.libs.util.opensearch_api import OpensearchAPI

opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
gits_table_template = table_templates.get(OPENSEARCH_GIT_RAW)

with DAG(dag_id='daily_gits_sync',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync'], ) as dag:
    def op_init_daily_gits_sync():
        return 'Start init_daily_gits_sync'


    op_init_daily_gits_sync = PythonOperator(task_id='op_init_daily_gits_sync',
                                             python_callable=op_init_daily_gits_sync, )


    def do_sync_git_opensearch(owner, repo, url, proxy_config):
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        git_save_local_path = Variable.get("git_save_local_path", deserialize_json=True)
        sync_gits_opensearch(url, owner=owner, repo=repo, proxy_config=proxy_config,
                             opensearch_conn_datas=opensearch_conn_datas, git_save_local_path=git_save_local_path)
        return 'do_sync_gits:::end'


    def do_sync_git_clickhouse(owner, repo):
        sync_clickhouse_from_opensearch(owner, repo, OPENSEARCH_GIT_RAW, opensearch_conn_info,
                                        OPENSEARCH_GIT_RAW, clickhouse_conn_info, gits_table_template)


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    # Priority:
    # 1. specified included owner_repos
    # 2. all uniq owner repos with specified excludes
    # 3. all uniq owner repos
    uniq_owner_repos = []
    includes = Variable.get(DAILY_SYNC_GITS_INCLUDES, deserialize_json=True, default_var=None)
    if not includes:
        excludes = Variable.get(DAILY_SYNC_GITS_EXCLUDES, deserialize_json=True, default_var=None)
        uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, OPENSEARCH_GIT_RAW, excludes)
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
        origin = uniq_item['origin']

        op_sync_gits_opensearch = PythonOperator(task_id=f'do_sync_gits_opensearch_{owner}_{repo}',
                                                 python_callable=do_sync_git_opensearch,
                                                 op_kwargs={
                                                     "owner": owner,
                                                     "repo": repo,
                                                     "url": origin,
                                                     "proxy_config": None,
                                                 })
        op_sync_gits_clickhouse = PythonOperator(task_id=f'do_sync_gits_clickhouse_{owner}_{repo}',
                                                 python_callable=do_sync_git_clickhouse,
                                                 op_kwargs={
                                                     'owner': owner,
                                                     'repo': repo,
                                                 })

        op_init_daily_gits_sync >> op_sync_gits_opensearch >> op_sync_gits_clickhouse
