from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.util.data_transfer import sync_clickhouse_repos_from_opensearch
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.base_dict.variable_key import DAILY_SYNC_GITS_EXCLUDES, DAILY_SYNC_GITS_INCLUDES, \
    CK_TABLE_DEFAULT_VAL_TPLT, OPENSEARCH_CONN_DATA, CLICKHOUSE_DRIVER_INFO, GIT_SAVE_LOCAL_PATH, \
    DAILY_GITS_SYNC_INTERVAL, DAILY_SYNC_INTERVAL
from oss_know.libs.github.sync_gits import sync_gits_opensearch
from oss_know.libs.util.base import get_opensearch_client, arrange_owner_repo_into_letter_groups
from oss_know.libs.util.opensearch_api import OpensearchAPI

opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
gits_table_template = table_templates.get(OPENSEARCH_GIT_RAW)
git_save_local_path = Variable.get(GIT_SAVE_LOCAL_PATH, deserialize_json=True)

sync_interval = Variable.get(DAILY_GITS_SYNC_INTERVAL, default_var=None)
if not sync_interval:
    sync_interval = Variable.get(DAILY_SYNC_INTERVAL, default_var=None)

with DAG(dag_id='daily_gits_sync',  # schedule_interval='*/5 * * * *',
         schedule_interval=sync_interval, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync'], ) as dag:
    def op_init_daily_gits_sync():
        return 'Start init_daily_gits_sync'


    op_init_daily_gits_sync = PythonOperator(task_id='op_init_daily_gits_sync',
                                             python_callable=op_init_daily_gits_sync, )


    def do_sync_gits_opensearch_group(owner_repo_group, proxy_config=None):
        for item in owner_repo_group:
            owner = item['owner']
            repo = item['repo']
            url = item['origin']
            sync_gits_opensearch(url, owner=owner, repo=repo, proxy_config=proxy_config,
                                 opensearch_conn_datas=opensearch_conn_info, git_save_local_path=git_save_local_path)
        return 'do_sync_gits:::end'


    def do_sync_git_clickhouse_group(owner_repo_group):
        sync_clickhouse_repos_from_opensearch(owner_repo_group, OPENSEARCH_GIT_RAW, opensearch_conn_info,
                                              OPENSEARCH_GIT_RAW, clickhouse_conn_info, gits_table_template)


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    # Priority:
    # 1. specified included owner_repos
    # 2. all uniq owner repos with specified excludes
    # 3. all uniq owner repos(if no excludes specified)
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

    task_groups_by_capital_letter = arrange_owner_repo_into_letter_groups(uniq_owner_repos)
    # TODO Currently the DAG makes 27 parallel task groups(serial execution inside each group)
    #  Check in production env if it works as expected(won't make too much pressure on opensearch and
    #  clickhouse. Another approach is to make all groups serial, one after another, which assign
    #  init_op to prev_op at the beginning, then assign op_sync_gits_clickhouse_group to prev_op
    #  in each loop iteration(as commented below)
    #  Another tip: though the groups dict is by default sorted by alphabet, the generated DAG won't
    #  respect the order
    # prev_op = op_init_daily_gits_sync
    for letter, owner_repos in task_groups_by_capital_letter.items():
        op_sync_gits_opensearch_group = PythonOperator(
            task_id=f'op_sync_gits_opensearch_group_{letter}',
            python_callable=do_sync_gits_opensearch_group,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
        op_sync_gits_clickhouse_group = PythonOperator(
            task_id=f'op_sync_gits_clickhouse_group_{letter}',
            python_callable=do_sync_git_clickhouse_group,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
        op_init_daily_gits_sync >> op_sync_gits_opensearch_group >> op_sync_gits_clickhouse_group
        # prev_op = op_sync_gits_clickhouse_group
