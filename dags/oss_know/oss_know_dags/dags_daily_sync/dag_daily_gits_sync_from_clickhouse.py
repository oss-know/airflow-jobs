from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, SYNC_FROM_CLICKHOUSE_DRIVER_INFO
from oss_know.libs.clickhouse.sync_clickhouse_data import sync_from_remote_by_repo
from oss_know.libs.util.clickhouse_driver import CKServer

clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
sync_from_clickhouse_conn_info = Variable.get(SYNC_FROM_CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

ck_client = CKServer(host=clickhouse_conn_info["HOST"],
                     port=clickhouse_conn_info["PORT"],
                     user=clickhouse_conn_info["USER"],
                     password=clickhouse_conn_info["PASSWD"],
                     database=clickhouse_conn_info["DATABASE"])
ck_cluster_name = ck_client.client.settings.get("CLUSTER_NAME")

remote_uniq_owner_repos_sql = f"""
select distinct(search_key__owner, search_key__repo)
from remote(
        '{sync_from_clickhouse_conn_info["HOST"]}:{sync_from_clickhouse_conn_info["PORT"]}',
        'default.gits',
        '{sync_from_clickhouse_conn_info["USER"]}',
        '{sync_from_clickhouse_conn_info["PASSWD"]}'
    )
"""

uniq_owner_repos_sql = f"""
select distinct(search_key__owner, search_key__repo)
from gits;
"""

import logging

airflow_debug_logger = logging.getLogger("airflow")
airflow_debug_logger.setLevel(logging.INFO)

# Daily sync git data from other clickhouse environment by owner/repo
with DAG(dag_id='daily_gits_sync_from_clickhouse',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync'], ) as dag:
    def do_init():
        local_owner_repos = [tup[0] for tup in ck_client.execute_no_params(uniq_owner_repos_sql)]
        remote_owner_repos = [tup[0] for tup in ck_client.execute_no_params(remote_uniq_owner_repos_sql)]
        all_owner_repos = set(local_owner_repos).union(set(remote_owner_repos))

        airflow_debug_logger.info('local_owner repos:', local_owner_repos)
        airflow_debug_logger.info('remote_owner repos:', remote_owner_repos)
        airflow_debug_logger.info('all_owner repos:', all_owner_repos)
        airflow_debug_logger.info('debug_sqls:', remote_uniq_owner_repos_sql, uniq_owner_repos_sql)

        return 'Start init_daily_gits_sync'


    op_init = PythonOperator(task_id='op_init_daily_gits_sync_from_clickhouse', python_callable=do_init)


    def do_sync_gits_from_clickhouse(params):
        sync_from_remote_by_repo(clickhouse_conn_info, sync_from_clickhouse_conn_info,
                                 "gits",
                                 params.get('owner'), params.get('repo'))


    local_owner_repos = [tup[0] for tup in ck_client.execute_no_params(uniq_owner_repos_sql)]
    remote_owner_repos = [tup[0] for tup in ck_client.execute_no_params(remote_uniq_owner_repos_sql)]
    all_owner_repos = set(local_owner_repos).union(set(remote_owner_repos))

    # TODO
    # Consider other solutions to control the parallelism of the tasks
    # Solution 1: One by one, then the tasks list in the UI would be super long
    # Solution 2: Split tasks into different batches, and execute each batch sequentially

    # Problem: Task list in each of the solutions(above and the current) IS dynamic, when the growing of repos,
    # task list in UI would go crazy.

    # Current solution, run all tasks in parallel
    for owner_repo_pair in all_owner_repos:
        owner, repo = owner_repo_pair
        op_sync_gits_from_clickhouse = PythonOperator(
            task_id=f'do_sync_gits_from_clickhouse_{owner}_{repo}',
            python_callable=do_sync_gits_from_clickhouse,
            op_kwargs={
                "params": {
                    "owner": owner,
                    "repo": repo,
                }
            }

        )
        op_init >> op_sync_gits_from_clickhouse
