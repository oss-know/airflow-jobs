from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_SYNC_INTERVAL, OPENSEARCH_SYNC_COMBINATION_TYPE, \
    SYNC_FROM_OPENSEARCH_CONN_INFO, OPENSEARCH_CONN_DATA, SYNC_OPENSEARCH_GITS_INCLUDES
from oss_know.libs.util.base import arrange_owner_repo_into_letter_groups
from oss_know.libs.util.opensearch_api import OpensearchAPI

sync_from_opensearch_conn_info = Variable.get(SYNC_FROM_OPENSEARCH_CONN_INFO, deserialize_json=True)
sync_interval = Variable.get(CLICKHOUSE_SYNC_INTERVAL, default_var=None)
sync_combination_type = Variable.get(OPENSEARCH_SYNC_COMBINATION_TYPE, default_var="diff_remote")

opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
remote_opensearch_conn_info = Variable.get(SYNC_FROM_OPENSEARCH_CONN_INFO, deserialize_json=True)
opensearch_api = OpensearchAPI()

all_owner_repos = Variable.get(SYNC_OPENSEARCH_GITS_INCLUDES, deserialize_json=True, default_var=None)
if not all_owner_repos:
    all_owner_repos = opensearch_api.combine_remote_owner_repos(opensearch_conn_info, remote_opensearch_conn_info,
                                                                OPENSEARCH_GIT_RAW)

# Daily sync gits data from other clickhouse environment by owner/repo
with DAG(dag_id='gits_sync_from_opensearch',  # schedule_interval='*/5 * * * *',
         schedule_interval=sync_interval, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'sync opensearch'], ) as dag:
    def do_sync_gits_from_opensearch_by_group(owner_repos_pairs):
        opensearch_api.sync_from_remote_by_repos(opensearch_conn_info, sync_from_opensearch_conn_info,
                                                 owner_repos_pairs, index=OPENSEARCH_GIT_RAW)


    # Init 26 sub groups by letter(to make the task DAG static)
    # Split all tasks into 26 groups by their capital letter, all tasks inside a group are executed sequentially
    # To avoid to many parallel tasks and keep the DAG static
    task_groups_by_capital_letter = arrange_owner_repo_into_letter_groups(all_owner_repos)
    prev_group = None
    for letter, owner_repos in task_groups_by_capital_letter.items():
        op_sync_gits_from_opensearch_group = PythonOperator(
            task_id=f'op_sync_gits_from_opensearch_group_{letter}',
            python_callable=do_sync_gits_from_opensearch_by_group,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repos_pairs": owner_repos
            }
        )
        if prev_group:
            prev_group >> op_sync_gits_from_opensearch_group
        prev_group = op_sync_gits_from_opensearch_group
